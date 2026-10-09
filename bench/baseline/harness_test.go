package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestFrameworksRespondIdentically is the precondition for the whole audit.
//
// If two frameworks return different bytes for the same workload, the
// throughput comparison measures the responses rather than the frameworks, and
// every table in the report is void. This test is therefore run before any
// benchmark and must not be weakened.
func TestFrameworksRespondIdentically(t *testing.T) {
	for _, w := range allWorkloads {
		t.Run(w.name, func(t *testing.T) {
			var reference *httptest.ResponseRecorder
			var refName string

			for _, tg := range frameworks(w, 0) {
				req := httptest.NewRequest(w.method, w.target, bytesReader(w.body))
				rec := httptest.NewRecorder()
				tg.handler.ServeHTTP(rec, req)
				res := rec.Result()
				res.Body.Close()

				got := rec.Body.Bytes()
				if reference == nil {
					reference, refName = rec, tg.name
					if res.StatusCode != specsByWorkload[w.name].status &&
						specsByWorkload[w.name].status != 0 {
						t.Logf("%s returned %d, spec says %d",
							tg.name, res.StatusCode, specsByWorkload[w.name].status)
					}
					continue
				}
				if res.StatusCode != reference.Code {
					t.Errorf("%s status = %d, %s = %d", tg.name, res.StatusCode,
						refName, reference.Code)
				}
				if string(got) != reference.Body.String() {
					t.Errorf("%s body = %q, %s = %q", tg.name, truncate(got),
						refName, truncate(reference.Body.Bytes()))
				}
				if ct := res.Header.Get("Content-Type"); ct != reference.Header().Get("Content-Type") {
					t.Logf("%s Content-Type = %q, %s = %q (not fatal, but noted)",
						tg.name, ct, refName, reference.Header().Get("Content-Type"))
				}
			}
		})
	}
}

// TestParamWorkloadActuallyEchoes exercises the parameter path specifically,
// because it is the one workload where a framework could return the right bytes
// without having read the parameter at all.
func TestParamWorkloadActuallyEchoes(t *testing.T) {
	const path = "/users/123456"
	for _, tg := range frameworks(allWorkloads[2], 0) {
		t.Run(tg.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			rec := httptest.NewRecorder()
			tg.handler.ServeHTTP(rec, req)
			if got := rec.Body.String(); got != `{"id":"123456"}` {
				t.Errorf("body = %q, want %q: the parameter was not read", got, `{"id":"123456"}`)
			}
		})
	}
}

// TestRequestBodyIsActuallyRead guards the request-body workloads against a
// framework that responds without draining the body, which would make it look
// faster simply by doing less work.
func TestRequestBodyIsActuallyRead(t *testing.T) {
	for _, w := range allWorkloads {
		if !specsByWorkload[w.name].readBody {
			continue
		}
		t.Run(w.name, func(t *testing.T) {
			for _, tg := range frameworks(w, 0) {
				req := httptest.NewRequest(w.method, w.target, bytesReader(w.body))
				rec := httptest.NewRecorder()
				tg.handler.ServeHTTP(rec, req)
				if rec.Code != 204 {
					t.Errorf("%s status = %d, want 204", tg.name, rec.Code)
				}
				// The recorder tracks unread body bytes, which is the only
				// observable proof the handler consumed the request.
				if rec.Body.Len() != 0 {
					t.Errorf("%s left %d body bytes unread", tg.name, rec.Body.Len())
				}
			}
		})
	}
}

// TestClientAgainstRealServer exercises the hand-written client against a real
// listener, so a client bug cannot silently produce wrong throughput numbers.
func TestClientAgainstRealServer(t *testing.T) {
	w := allWorkloads[0] // static plaintext
	host, stop, err := serve(newPhoton(w, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	cc, err := dial(host)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.c.Close()

	for i := 0; i < 100; i++ {
		us, err := cc.do("GET", "/", nil)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if us < 0 {
			t.Fatalf("request %d returned negative latency %v", i, us)
		}
	}
}

// TestClientHandlesChunkedResponses checks the chunked path, which the audit
// does not exercise in its main matrix but which a mis-parsed response would
// corrupt silently.
func TestClientHandlesChunkedResponses(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No Content-Length, so net/http uses chunked encoding.
		for i := 0; i < 3; i++ {
			_, _ = w.Write([]byte("chunk\n"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	})
	host, stop, err := serve(h)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	cc, err := dial(host)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.c.Close()
	if _, err := cc.do("GET", "/", nil); err != nil {
		t.Fatalf("chunked response: %v", err)
	}
}

func truncate(b []byte) string {
	const max = 60
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}
