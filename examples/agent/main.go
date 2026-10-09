// Command agent streams an AI agent's work to a browser as it happens: what it
// is thinking, which tools it calls, what they return, and the answer, token
// by token.
//
//	go run ./examples/agent
//	open http://localhost:8080
//
// Or watch the raw events:
//
//	curl -N 'localhost:8080/agent?task=compare+Go+and+Rust+for+an+edge+proxy'
//
// The "model" and the tools are scripted fakes with realistic delays, so the
// example runs anywhere. Replace plan() and the tools with real calls; nothing
// about the streaming changes.
//
// What it demonstrates:
//
//   - One named event type per kind of step (thinking, tool_call, tool_result,
//     token, done), each with a JSON body - the frontend is a switch statement.
//   - Tools running in parallel and reporting from their own goroutines. A
//     Stream is safe for concurrent use; events never interleave mid-frame.
//   - Event ids and Last-Event-ID, so a reconnecting browser resumes where it
//     stopped instead of replaying the whole run.
//   - A hard cap on agent steps, and r.Context() passed to every "model" and
//     tool call so a closed tab stops the work.
//   - Graceful shutdown: s.Done() fires when the server is draining, and the
//     handler tells the client to reconnect instead of being cut off.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	app := photon.New(photon.Logger(slog.Default()))

	app.GET("/", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.HTML(w, http.StatusOK, page)
	})
	app.GET("/agent", photon.SSE(runAgent))

	slog.Info("agent example on http://localhost:8080")
	if err := app.Run(":8080"); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

// maxSteps bounds the agent loop. Models do get stuck calling the same tool
// forever, and an unbounded loop is an unbounded bill.
const maxSteps = 4

// runAgent is the whole streaming endpoint.
func runAgent(s *photon.Stream, r *http.Request) error {
	task := strings.TrimSpace(r.URL.Query().Get("task"))
	if task == "" {
		// Nothing has been sent, so this is a plain 400.
		return photonerr.BadRequest("task is required")
	}
	if len(task) > 500 {
		return photonerr.BadRequest("task must be at most 500 characters")
	}

	// Every event gets a sequence number as its SSE id. A browser that loses
	// the connection reconnects with Last-Event-ID, and we skip what it has.
	// The header is client input: parse it, and on anything odd start over.
	resumeAfter := -1
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			resumeAfter = n
		}
	}
	ev := &events{s: s, skipThrough: resumeAfter}
	_ = s.SetRetry(2 * time.Second)

	// Before the first token can take a while; heartbeats keep proxies from
	// closing the connection while the "model" thinks.
	stop := s.StartHeartbeats()
	defer stop()

	ctx := r.Context() // cancelled when the client goes away
	notes := []string{}
	for step := 0; step < maxSteps; step++ {
		select {
		case <-s.Done():
			// The server is draining (or the client left). Tell the client to
			// come back; with ids it will resume, possibly on another instance.
			return ev.send("reconnect", map[string]string{"reason": "server restarting"})
		default:
		}

		p, err := plan(ctx, task, step)
		if err != nil {
			return err
		}
		if err := ev.send("thinking", map[string]any{"step": step + 1, "text": p.thought}); err != nil {
			return err
		}
		if len(p.calls) == 0 {
			return answer(ctx, ev, task, notes, step+1)
		}

		// Run this step's tools in parallel. Each reports its own call and
		// result straight to the stream.
		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, c := range p.calls {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = ev.send("tool_call", c)
				out := runTool(ctx, c)
				_ = ev.send("tool_result", map[string]string{"tool": c.Tool, "result": out})
				mu.Lock()
				notes = append(notes, out)
				mu.Unlock()
			}()
		}
		wg.Wait() // never return while goroutines still write to the stream
		if err := ctx.Err(); err != nil {
			return err // the client left during the tool calls
		}
	}
	return ev.send("error", map[string]string{"message": "the agent did not finish within the step limit"})
}

// answer streams the final answer a word at a time.
func answer(ctx context.Context, ev *events, task string, notes []string, steps int) error {
	text := "Here is what I found about \"" + task + "\": " + strings.Join(notes, " ") +
		" On balance, pick the one your team can operate at 3am."
	for i, word := range strings.Fields(text) {
		if i > 0 {
			word = " " + word
		}
		if err := ev.send("token", map[string]string{"text": word}); err != nil {
			return err
		}
		if err := sleep(ctx, 40*time.Millisecond); err != nil {
			return err
		}
	}
	return ev.send("done", map[string]int{"steps": steps})
}

// events numbers every event and skips those a resuming client already has.
//
// The numbering is deterministic here because the fake agent is; a real agent
// would write its events to a log and resume from the log, so a reconnect gets
// exactly the events it missed even after a server restart.
type events struct {
	s           *photon.Stream
	mu          sync.Mutex
	seq         int
	skipThrough int
}

func (e *events) send(name string, v any) error {
	e.mu.Lock() // id and event must be written together, in order
	defer e.mu.Unlock()
	id := e.seq
	e.seq++
	if id <= e.skipThrough {
		return nil
	}
	if err := e.s.SetEventID(strconv.Itoa(id)); err != nil {
		return err
	}
	return e.s.JSON(name, v)
}

// ---------------------------------------------------------------- the fake model

type toolCall struct {
	Tool  string `json:"tool"`
	Input string `json:"input"`
}

type step struct {
	thought string
	calls   []toolCall
}

// plan stands in for an LLM call that decides the next step.
func plan(ctx context.Context, task string, n int) (step, error) {
	if err := sleep(ctx, 400*time.Millisecond); err != nil {
		return step{}, err
	}
	switch n {
	case 0:
		return step{
			thought: "I need background on both options before comparing them.",
			calls: []toolCall{
				{Tool: "search", Input: task + " performance"},
				{Tool: "search", Input: task + " operational cost"},
			},
		}, nil
	case 1:
		return step{
			thought: "Benchmarks disagree; I will check the methodology of the most cited one.",
			calls:   []toolCall{{Tool: "read_url", Input: "https://example.com/benchmark"}},
		}, nil
	default:
		return step{thought: "I have enough to answer."}, nil
	}
}

// runTool stands in for a real tool. Real tools need the care described in
// docs/guides/ai-backends.md: validated arguments, least privilege, and no
// fetching of attacker-chosen URLs on your network.
func runTool(ctx context.Context, c toolCall) string {
	// Deterministic latency, so the event order - and the ids a resuming
	// client relies on - is the same on every run.
	_ = sleep(ctx, time.Duration(300+len(c.Input)%7*60)*time.Millisecond)
	switch c.Tool {
	case "search":
		return "Results for '" + c.Input + "' suggest both are fast enough; the difference is in tooling."
	case "read_url":
		return "The benchmark measured a single core with a warm cache, which flatters both."
	default:
		return "unknown tool"
	}
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

const page = `<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>photon agent example</title>
<style>
  body { font: 15px/1.5 system-ui, sans-serif; max-width: 760px; margin: 2rem auto; padding: 0 1rem; color: #1d1d1f; }
  form { display: flex; gap: .5rem; } input { flex: 1; padding: .5rem; } button { padding: .5rem 1rem; }
  .ev { margin: .4rem 0; padding: .4rem .6rem; border-left: 3px solid #ccc; background: #f6f6f7; white-space: pre-wrap; }
  .thinking { border-color: #8e8e93; } .tool_call { border-color: #ff9f0a; } .tool_result { border-color: #30d158; }
  .error { border-color: #ff453a; } #answer { margin-top: 1rem; font-size: 1.05rem; }
  small { color: #6e6e73; }
</style>
<h1>Agent event stream</h1>
<form id="f"><input id="task" value="compare Go and Rust for an edge proxy"><button>Run</button></form>
<div id="log"></div><div id="answer"></div>
<p><small>Events arrive over one SSE connection. Stop the server mid-run: the page reconnects and resumes from the last event id.</small></p>
<script>
const log = document.getElementById("log"), answer = document.getElementById("answer");
let es;
function show(type, text) {
  const d = document.createElement("div"); d.className = "ev " + type;
  d.textContent = type + ": " + text; log.appendChild(d);
}
document.getElementById("f").onsubmit = (e) => {
  e.preventDefault(); if (es) es.close(); log.textContent = ""; answer.textContent = "";
  es = new EventSource("/agent?task=" + encodeURIComponent(document.getElementById("task").value));
  es.addEventListener("thinking", (e) => show("thinking", JSON.parse(e.data).text));
  es.addEventListener("tool_call", (e) => { const c = JSON.parse(e.data); show("tool_call", c.tool + "(" + c.input + ")"); });
  es.addEventListener("tool_result", (e) => show("tool_result", JSON.parse(e.data).result));
  es.addEventListener("token", (e) => { answer.textContent += JSON.parse(e.data).text; });
  es.addEventListener("done", () => { show("done", "finished"); es.close(); });
  es.addEventListener("error", (e) => { if (e.data) { show("error", JSON.parse(e.data).message || e.data); es.close(); } });
};
</script>
</html>`
