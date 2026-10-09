package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/agenticmarket/photon"
	"github.com/agenticmarket/photon/photonerr"
)

// runServe starts the demo service: a dashboard, a token stream, and an
// OpenAI-compatible chat endpoint backed by a scripted "model". It exists so a
// new user can see streaming, cancellation, and back-pressure working before
// writing any code, and so `photon bench` has something to point at.
func runServe(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	port := flags.String("port", "", "port to listen on (default $PORT, then 8484)")
	maxStreams := flags.Int("max-streams", 1000, "maximum concurrent streams")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *port == "" {
		*port = firstNonEmpty(os.Getenv("PORT"), os.Getenv("PHOTON_PORT"), "8484")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	app := photon.New(photon.Logger(logger), photon.MaxStreams(*maxStreams))
	d := &demo{started: time.Now()}

	app.GET("/", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.HTML(w, http.StatusOK, dashboardHTML)
	})
	app.GET("/agent/stream", photon.SSE(d.agentStream))
	app.POST("/v1/chat/completions", d.chatCompletions)
	app.GET("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.JSON(w, http.StatusOK, map[string]any{
			"object": "list",
			"data":   []map[string]string{{"id": "photon-demo", "object": "model", "owned_by": "photon"}},
		})
	})
	app.GET("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_ = photon.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	app.GET("/metrics", d.metrics)

	base := "http://localhost:" + *port
	fmt.Printf(`
  Photon demo service

  Dashboard          %[1]s/
  Token stream       curl -N %[1]s/agent/stream
  OpenAI-compatible  curl -N %[1]s/v1/chat/completions -H 'Content-Type: application/json' \
                       -d '{"model":"photon-demo","stream":true,"messages":[{"role":"user","content":"hi"}]}'
  Benchmark it       photon bench -c 20 %[1]s/agent/stream?burst=true

  Ctrl-C drains active streams and exits; press it twice to exit at once.

`, base)
	return app.Run(":" + *port)
}

// demo holds the service's counters.
type demo struct {
	started time.Time
	streams atomic.Int64
	events  atomic.Int64
}

var demoWords = strings.Fields("Thinking about the question. Photon sends each token the moment it " +
	"is produced, stops the model when you close the tab, and keeps a slow client from " +
	"costing the server memory. Try closing this stream halfway and watch the log.")

// agentStream streams a scripted answer. ?events=N sets the length and
// ?burst=true removes the delay between tokens, for benchmarking.
func (d *demo) agentStream(s *photon.Stream, r *http.Request) error {
	d.streams.Add(1)
	count := len(demoWords)
	if n, err := strconv.Atoi(r.URL.Query().Get("events")); err == nil && n > 0 && n <= 100_000 {
		count = n
	}
	burst := r.URL.Query().Get("burst") == "true"
	for i := 0; i < count; i++ {
		if err := s.JSON("token", map[string]any{"index": i, "token": demoWords[i%len(demoWords)] + " "}); err != nil {
			return err // the client left: stop producing
		}
		d.events.Add(1)
		if !burst {
			select {
			case <-time.After(30 * time.Millisecond):
			case <-r.Context().Done():
				return nil
			}
		}
	}
	return s.JSON("done", map[string]any{"tokens": count})
}

// chatCompletions imitates the OpenAI Chat Completions API, streaming and not.
func (d *demo) chatCompletions(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Model    string `json:"model"`
		Stream   bool   `json:"stream"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := photon.DecodeJSON(r, &req); err != nil {
		photon.Error(w, r, err)
		return
	}
	if len(req.Messages) == 0 {
		photon.Error(w, r, photonerr.BadRequest("messages must not be empty"))
		return
	}
	model := firstNonEmpty(req.Model, "photon-demo")
	id := "chatcmpl-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	tokens := strings.SplitAfter("Hello! I am a scripted model served by Photon, one token at a time.", " ")

	if !req.Stream {
		_ = photon.JSON(w, http.StatusOK, map[string]any{
			"id": id, "object": "chat.completion", "created": time.Now().Unix(), "model": model,
			"choices": []map[string]any{{
				"index": 0, "finish_reason": "stop",
				"message": map[string]string{"role": "assistant", "content": strings.Join(tokens, "")},
			}},
		})
		return
	}
	photon.SSE(func(s *photon.Stream, r *http.Request) error {
		d.streams.Add(1)
		for _, tok := range tokens {
			// Unnamed events: OpenAI SDKs treat a named event as something
			// other than a chunk.
			if err := s.JSON("", map[string]any{
				"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
				"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": tok}}},
			}); err != nil {
				return err
			}
			d.events.Add(1)
			select {
			case <-time.After(40 * time.Millisecond):
			case <-r.Context().Done():
				return nil
			}
		}
		return s.Event("", []byte("[DONE]"))
	})(w, r)
}

func (d *demo) metrics(w http.ResponseWriter, r *http.Request) {
	_ = photon.JSON(w, http.StatusOK, map[string]any{
		"streams_served": d.streams.Load(),
		"events_emitted": d.events.Load(),
		"uptime_seconds": int(time.Since(d.started).Seconds()),
	})
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

const dashboardHTML = `<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Photon demo</title>
<style>
  :root { --bg:#0b0f17; --card:#131a26; --line:#223047; --text:#e6edf6; --muted:#8fa1b8; --accent:#4cc2ff; --ok:#3ad29f; }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 32px 20px; background: var(--bg); color: var(--text);
         font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
  main { max-width: 860px; margin: 0 auto; }
  h1 { margin: 0 0 4px; font-size: 22px; } p.sub { margin: 0 0 24px; color: var(--muted); }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 18px; }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px; }
  .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
  .value { font-size: 22px; font-weight: 600; margin-top: 4px; color: var(--accent); }
  button { background: var(--accent); color: #031522; border: 0; border-radius: 8px; padding: 9px 16px;
           font-weight: 600; cursor: pointer; margin-right: 8px; }
  button.secondary { background: var(--line); color: var(--text); }
  #out { margin-top: 14px; min-height: 140px; white-space: pre-wrap; background: #070a10; border: 1px solid var(--line);
         border-radius: 10px; padding: 14px; font-family: ui-monospace, "Cascadia Code", Menlo, monospace; }
  code { font-family: ui-monospace, Menlo, monospace; color: #c9d6e6; }
  ul { color: var(--muted); padding-left: 18px; }
</style>
<main>
  <h1>Photon demo</h1>
  <p class="sub">A token stream over server-sent events, delivered as it is produced.</p>
  <div class="grid">
    <div class="card"><div class="label">Time to first token</div><div class="value" id="ttft">-</div></div>
    <div class="card"><div class="label">Tokens</div><div class="value" id="count">0</div></div>
    <div class="card"><div class="label">Status</div><div class="value" id="status" style="color:var(--ok)">Ready</div></div>
  </div>
  <div class="card">
    <button id="start">Stream</button><button class="secondary" id="burst">Burst 2,000 tokens</button>
    <button class="secondary" id="stop">Stop</button>
    <div id="out"></div>
  </div>
  <ul>
    <li><code>GET /agent/stream</code> - this stream; <code>?burst=true&amp;events=N</code> for benchmarking</li>
    <li><code>POST /v1/chat/completions</code> - OpenAI-compatible, streaming and not</li>
    <li><code>GET /healthz</code>, <code>GET /metrics</code></li>
  </ul>
</main>
<script>
const out = document.getElementById("out"), ttft = document.getElementById("ttft"),
      count = document.getElementById("count"), status = document.getElementById("status");
let es;
function run(query) {
  if (es) es.close();
  out.textContent = ""; count.textContent = "0"; ttft.textContent = "-"; status.textContent = "Streaming";
  const t0 = performance.now(); let n = 0;
  es = new EventSource("/agent/stream" + query);
  es.addEventListener("token", (e) => {
    if (n === 0) ttft.textContent = Math.round(performance.now() - t0) + " ms";
    n++; count.textContent = n;
    if (n <= 400) out.textContent += JSON.parse(e.data).token;
  });
  es.addEventListener("done", () => { status.textContent = "Done"; es.close(); });
  es.onerror = () => { status.textContent = "Closed"; es.close(); };
}
document.getElementById("start").onclick = () => run("");
document.getElementById("burst").onclick = () => run("?burst=true&events=2000");
document.getElementById("stop").onclick = () => { if (es) es.close(); status.textContent = "Stopped"; };
</script>
</html>`
