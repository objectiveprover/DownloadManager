package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dlm/internal/engine"
)

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	m, err := engine.New(engine.Config{Dir: t.TempDir(), MaxActive: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	// Pause everything that gets added so tests don't hit the network.
	go func() {
		ch, unsub := m.Subscribe()
		defer unsub()
		for list := range ch {
			for _, s := range list {
				if s.Status == engine.StatusQueued || s.Status == engine.StatusDownloading {
					m.Pause(s.ID)
				}
			}
		}
	}()
	return Handler(m, Options{})
}

func do(h http.Handler, method, target, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Host = "127.0.0.1:7878"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

var jsonHdr = map[string]string{"Content-Type": "application/json"}

func TestAddWithChecksum(t *testing.T) {
	h := newTestHandler(t)
	sum := strings.Repeat("ab", 32)
	rec := do(h, "POST", "/api/downloads", `{"url":"https://example.invalid/a.iso","checksum":"`+sum+`"}`, jsonHdr)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var snap engine.Snapshot
	json.Unmarshal(rec.Body.Bytes(), &snap)
	if snap.Checksum.Algo != "sha256" || snap.Checksum.Status != engine.ChecksumPending {
		t.Errorf("checksum = %+v", snap.Checksum)
	}
}

func TestAddRejectsBadChecksum(t *testing.T) {
	h := newTestHandler(t)
	rec := do(h, "POST", "/api/downloads", `{"url":"https://example.invalid/a.iso","checksum":"sha256:abc"}`, jsonHdr)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "sha256") {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
}

func TestGuard(t *testing.T) {
	h := newTestHandler(t)
	body := `{"url":"https://example.invalid/a.iso"}`

	if rec := do(h, "POST", "/api/downloads", body, map[string]string{"Content-Type": "text/plain"}); rec.Code != http.StatusUnsupportedMediaType {
		t.Errorf("text/plain body: status %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/downloads", body, map[string]string{"Content-Type": "application/json", "Origin": "https://evil.example"}); rec.Code != http.StatusForbidden {
		t.Errorf("foreign origin: status %d", rec.Code)
	}
	if rec := do(h, "POST", "/api/downloads", body, map[string]string{"Content-Type": "application/json", "Origin": "http://127.0.0.1:7878"}); rec.Code != http.StatusCreated {
		t.Errorf("same origin: status %d: %s", rec.Code, rec.Body)
	}

	req := httptest.NewRequest("GET", "/api/downloads", nil)
	req.Host = "attacker.example:7878" // DNS rebinding
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("foreign host: status %d", rec.Code)
	}
}

func TestIndexServed(t *testing.T) {
	h := newTestHandler(t)
	rec := do(h, "GET", "/", "", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `id="checksum"`) {
		t.Fatalf("status %d", rec.Code)
	}
}
