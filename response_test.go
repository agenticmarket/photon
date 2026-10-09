package photon

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if err := JSON(rec, http.StatusCreated, user{Name: "ada", Age: 36}); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(rec.Body.Len()) {
		t.Errorf("Content-Length = %q, want %d", got, rec.Body.Len())
	}
	var got user
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if got.Name != "ada" || got.Age != 36 {
		t.Errorf("decoded = %+v, want ada/36", got)
	}
}

func TestJSONEscapesHTML(t *testing.T) {
	// encoding/json escapes <, > and & by default, so a string in a response
	// cannot become markup if the content type is ever mis-sniffed.
	rec := httptest.NewRecorder()
	if err := JSON(rec, 200, map[string]string{"x": "<script>alert(1)</script>"}); err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if strings.Contains(body, "<script>") {
		t.Errorf("body %q contains raw markup; it must be escaped", body)
	}
	if !strings.Contains(body, `\u003c`) {
		t.Errorf("body %q does not use \\u003c escaping", body)
	}
}

func TestJSONReturnsMarshalError(t *testing.T) {
	rec := httptest.NewRecorder()
	// A channel cannot be marshalled, so the error must surface rather than be
	// swallowed into a 200 with an empty body.
	err := JSON(rec, 200, make(chan int))
	if err == nil {
		t.Fatal("JSON did not return an error for an unmarshallable value")
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty when marshalling failed", rec.Body.String())
	}
}

func TestText(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := Text(rec, http.StatusAccepted, "queued"); err != nil {
		t.Fatalf("Text: %v", err)
	}
	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", rec.Code)
	}
	if rec.Body.String() != "queued" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "queued")
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}
}

func TestNoContent(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := NoContent(rec); err != nil {
		t.Fatalf("NoContent: %v", err)
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
}

func TestHTML(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := HTML(rec, 200, "<h1>hi</h1>"); err != nil {
		t.Fatalf("HTML: %v", err)
	}
	if rec.Body.String() != "<h1>hi</h1>" {
		t.Errorf("body = %q", rec.Body.String())
	}
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff: a mislabelled HTML response is an XSS vector", got)
	}
	// A default CSP would be wrong for most applications, so none is set; the
	// decision belongs to the application.
	if got := rec.Header().Get("Content-Security-Policy"); got != "" {
		t.Errorf("Content-Security-Policy = %q, want unset: it is an application decision", got)
	}
}

func TestRedirect(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/submit", nil)
	if err := Redirect(rec, req, "/done"); err != nil {
		t.Fatalf("Redirect: %v", err)
	}
	if rec.Code != http.StatusSeeOther {
		t.Errorf("status = %d, want 303", rec.Code)
	}
	if got := rec.Header().Get("Location"); got != "/done" {
		t.Errorf("Location = %q, want /done", got)
	}
}

func TestResponseHelpersWorkThroughTheRouter(t *testing.T) {
	// The helpers must work on the writer the router passes, and the router
	// must not have written a status before the handler did.
	s := New()
	s.GET("/json", HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = JSON(w, 200, map[string]int{"n": 1})
	}))
	rt := s.Router()
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, httptest.NewRequest("GET", "/json", nil))
	if rec.Code != 200 {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"n":1`) {
		t.Errorf("body = %q", rec.Body.String())
	}
}
