// Package server exposes the download manager as a small JSON API and a
// single-page browser UI.
package server

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"

	"dlm/internal/engine"
)

//go:embed web/index.html
var indexHTML []byte

type Options struct {
	// AllowAnyHost disables the localhost-only Host check. Set it when
	// listening on a non-loopback address on purpose.
	AllowAnyHost bool
}

// Handler returns the routes:
//
//	GET    /                          browser UI
//	GET    /api/downloads             list
//	POST   /api/downloads             {"url": "...", "checksum": "sha256:..."} (checksum optional)
//	POST   /api/downloads/{id}/pause
//	POST   /api/downloads/{id}/resume  also retries failures; re-downloads after a checksum mismatch
//	DELETE /api/downloads/{id}        ?deleteFile=true also deletes the finished file
//	GET    /api/events                Server-Sent Events stream of the full list
func Handler(m *engine.Manager, opts Options) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'")
		w.Write(indexHTML)
	})

	mux.HandleFunc("GET /api/downloads", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, m.List())
	})

	mux.HandleFunc("POST /api/downloads", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			URL      string `json:"url"`
			Checksum string `json:"checksum"`
		}
		if !decodeJSON(w, r, &req) {
			return
		}
		snap, err := m.Add(req.URL, req.Checksum)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusCreated, snap)
	})

	mux.HandleFunc("POST /api/downloads/{id}/pause", func(w http.ResponseWriter, r *http.Request) {
		respond(w, m.Pause(r.PathValue("id")))
	})
	mux.HandleFunc("POST /api/downloads/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
		respond(w, m.Resume(r.PathValue("id")))
	})
	mux.HandleFunc("DELETE /api/downloads/{id}", func(w http.ResponseWriter, r *http.Request) {
		respond(w, m.Remove(r.PathValue("id"), r.URL.Query().Get("deleteFile") == "true"))
	})

	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		ch, unsubscribe := m.Subscribe()
		defer unsubscribe()
		for {
			select {
			case <-r.Context().Done():
				return
			case snaps, ok := <-ch:
				if !ok {
					return
				}
				data, _ := json.Marshal(snaps)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	})

	return guard(mux, opts)
}

// guard blocks other websites from driving the API through the user's
// browser: DNS-rebinding (Host check) and cross-site requests (Origin check).
// POST bodies must also be JSON, which browsers can't send cross-origin
// without a preflight this server never approves.
func guard(next http.Handler, opts Options) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !opts.AllowAnyHost && !isLoopbackHost(r.Host) {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if origin := r.Header.Get("Origin"); origin != "" {
				if u, err := url.Parse(origin); err != nil || u.Host != r.Host {
					http.Error(w, "cross-origin request refused", http.StatusForbidden)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopbackHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, errors.New("Content-Type must be application/json"))
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
		return false
	}
	return true
}

func respond(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, engine.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	default:
		writeError(w, http.StatusConflict, err)
	}
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
