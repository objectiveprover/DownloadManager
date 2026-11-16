package engine

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fileServer serves one file over TLS with knobs for misbehaving.
type fileServer struct {
	mu        sync.Mutex
	content   []byte
	etag      string
	noRanges  bool          // ignore Range headers (always 200 + full body)
	failFirst int           // abort this many data requests partway through
	chunkWait time.Duration // throttle: pause after every 8 KiB written
	ranges    []string      // Range headers of data requests (probe excluded)

	served atomic.Int64 // body bytes written for data requests
}

func (fsrv *fileServer) setContent(b []byte, etag string) {
	fsrv.mu.Lock()
	fsrv.content, fsrv.etag = b, etag
	fsrv.mu.Unlock()
}

func (fsrv *fileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	isProbe := r.Header.Get("Range") == "bytes=0-0"
	fsrv.mu.Lock()
	content, etag, noRanges, wait := fsrv.content, fsrv.etag, fsrv.noRanges, fsrv.chunkWait
	fail := false
	if !isProbe {
		fsrv.ranges = append(fsrv.ranges, r.Header.Get("Range"))
		if fsrv.failFirst > 0 {
			fsrv.failFirst--
			fail = true
		}
	}
	fsrv.mu.Unlock()

	tw := &testWriter{ResponseWriter: w, wait: wait, abortAfter: -1}
	if !isProbe {
		tw.served = &fsrv.served
	}
	if fail {
		tw.abortAfter = 100 << 10
	}

	if noRanges {
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		tw.Write(content)
		return
	}
	w.Header().Set("ETag", etag)
	http.ServeContent(tw, r, "file.bin", time.Time{}, bytes.NewReader(content))
}

type testWriter struct {
	http.ResponseWriter
	wait       time.Duration
	abortAfter int // drop the connection after this many bytes; -1 = never
	written    int
	served     *atomic.Int64
}

func (w *testWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := min(len(p), 8<<10)
		if w.abortAfter >= 0 && w.written+n > w.abortAfter {
			n = w.abortAfter - w.written
			w.ResponseWriter.Write(p[:n])
			w.ResponseWriter.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		m, err := w.ResponseWriter.Write(p[:n])
		total += m
		w.written += m
		if w.served != nil {
			w.served.Add(int64(m))
		}
		if err != nil {
			return total, err
		}
		p = p[n:]
		if w.wait > 0 {
			w.ResponseWriter.(http.Flusher).Flush()
			time.Sleep(w.wait)
		}
	}
	return total, nil
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type env struct {
	srv   *httptest.Server
	files *fileServer
	dir   string
	state string
}

func newEnv(t *testing.T, content []byte) *env {
	t.Helper()
	fsrv := &fileServer{content: content, etag: `"v1"`}
	srv := httptest.NewTLSServer(fsrv)
	t.Cleanup(srv.Close)
	return &env{srv: srv, files: fsrv, dir: t.TempDir(), state: filepath.Join(t.TempDir(), "state.json")}
}

func (e *env) url() string { return e.srv.URL + "/file.bin" }

func (e *env) manager(t *testing.T) *Manager {
	t.Helper()
	m, err := New(Config{
		Dir:         e.dir,
		StatePath:   e.state,
		Segments:    4,
		MaxActive:   3,
		MaxRetries:  3,
		RetryDelay:  10 * time.Millisecond,
		IdleTimeout: 5 * time.Second,
		Client:      e.srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func waitFor(t *testing.T, m *Manager, id string, what string, cond func(Snapshot) bool) Snapshot {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last Snapshot
	for time.Now().Before(deadline) {
		for _, s := range m.List() {
			if s.ID == id {
				last = s
				if cond(s) {
					return s
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; last state: %+v", what, last)
	return last
}

func finished(s Snapshot) bool { return s.Status == StatusDone || s.Status == StatusFailed }

func assertFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("file content differs: got %d bytes (sha256 %s), want %d bytes (sha256 %s)",
			len(got), sha256hex(got), len(want), sha256hex(want))
	}
	if _, err := os.Stat(path + ".part"); err == nil {
		t.Errorf(".part file left behind")
	}
}

func TestSegmentedDownload(t *testing.T) {
	content := randomBytes(t, 2<<20)
	e := newEnv(t, content)
	m := e.manager(t)

	snap, err := m.Add(e.url(), "")
	if err != nil {
		t.Fatal(err)
	}
	s := waitFor(t, m, snap.ID, "done", finished)
	if s.Status != StatusDone {
		t.Fatalf("status = %s (%s)", s.Status, s.Error)
	}
	if s.Segments != 4 {
		t.Errorf("segments = %d, want 4", s.Segments)
	}
	if s.Filename != "file.bin" {
		t.Errorf("filename = %q", s.Filename)
	}
	assertFile(t, filepath.Join(e.dir, "file.bin"), content)
	for _, r := range e.files.ranges {
		if !strings.HasPrefix(r, "bytes=") {
			t.Errorf("expected only range requests, got %q", r)
		}
	}
}

func TestNoRangeSupportFallsBackToSingleStream(t *testing.T) {
	content := randomBytes(t, 1<<20)
	e := newEnv(t, content)
	e.files.noRanges = true
	m := e.manager(t)

	snap, _ := m.Add(e.url(), "")
	s := waitFor(t, m, snap.ID, "done", finished)
	if s.Status != StatusDone || s.Segments != 1 {
		t.Fatalf("status = %s, segments = %d (%s)", s.Status, s.Segments, s.Error)
	}
	assertFile(t, filepath.Join(e.dir, "file.bin"), content)
}

func TestRetriesAfterDroppedConnection(t *testing.T) {
	content := randomBytes(t, 2<<20)
	e := newEnv(t, content)
	e.files.failFirst = 3
	m := e.manager(t)

	snap, _ := m.Add(e.url(), "")
	s := waitFor(t, m, snap.ID, "done", finished)
	if s.Status != StatusDone {
		t.Fatalf("status = %s (%s)", s.Status, s.Error)
	}
	assertFile(t, filepath.Join(e.dir, "file.bin"), content)
}

func TestChecksumVerified(t *testing.T) {
	content := randomBytes(t, 1<<20)
	e := newEnv(t, content)
	m := e.manager(t)

	snap, err := m.Add(e.url(), "sha256:"+strings.ToUpper(sha256hex(content)))
	if err != nil {
		t.Fatal(err)
	}
	if snap.Checksum.Status != ChecksumPending {
		t.Fatalf("checksum status on add = %s, want pending", snap.Checksum.Status)
	}
	s := waitFor(t, m, snap.ID, "done", finished)
	if s.Status != StatusDone || s.Checksum.Status != ChecksumVerified {
		t.Fatalf("status = %s, checksum = %+v (%s)", s.Status, s.Checksum, s.Error)
	}
	if s.Checksum.Actual != sha256hex(content) {
		t.Errorf("actual = %s", s.Checksum.Actual)
	}
}

func TestChecksumMismatch(t *testing.T) {
	content := randomBytes(t, 1<<20)
	e := newEnv(t, content)
	m := e.manager(t)

	wrong := sha256hex([]byte("something else"))
	snap, _ := m.Add(e.url(), wrong)
	s := waitFor(t, m, snap.ID, "finished", finished)
	if s.Status != StatusFailed || s.Checksum.Status != ChecksumMismatch {
		t.Fatalf("status = %s, checksum = %+v", s.Status, s.Checksum)
	}
	if s.Checksum.Expected != wrong || s.Checksum.Actual != sha256hex(content) {
		t.Errorf("checksum = %+v", s.Checksum)
	}
	if !strings.Contains(s.Error, "checksum mismatch") {
		t.Errorf("error = %q", s.Error)
	}
	// The file is kept so the user can inspect it.
	assertFile(t, filepath.Join(e.dir, "file.bin"), content)
}

func TestNoChecksumSkipsVerification(t *testing.T) {
	content := randomBytes(t, 512<<10)
	e := newEnv(t, content)
	m := e.manager(t)

	snap, _ := m.Add(e.url(), "  ")
	if snap.Checksum.Status != ChecksumNone {
		t.Fatalf("checksum status = %s, want none", snap.Checksum.Status)
	}
	seenVerifying := false
	ch, unsub := m.Subscribe()
	defer unsub()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case list := <-ch:
			for _, s := range list {
				if s.ID != snap.ID {
					continue
				}
				if s.Status == StatusVerifying {
					seenVerifying = true
				}
				if finished(s) {
					if s.Status != StatusDone || s.Checksum.Status != ChecksumNone || seenVerifying {
						t.Fatalf("status = %s, checksum = %s, saw verifying = %v", s.Status, s.Checksum.Status, seenVerifying)
					}
					return
				}
			}
		case <-deadline:
			t.Fatal("timed out")
		}
	}
}

func TestPauseAndResume(t *testing.T) {
	content := randomBytes(t, 2<<20)
	e := newEnv(t, content)
	e.files.chunkWait = 5 * time.Millisecond
	m := e.manager(t)

	snap, _ := m.Add(e.url(), sha256hex(content))
	waitFor(t, m, snap.ID, "some progress", func(s Snapshot) bool { return s.Downloaded > 256<<10 })
	if err := m.Pause(snap.ID); err != nil {
		t.Fatal(err)
	}
	paused := waitFor(t, m, snap.ID, "paused", func(s Snapshot) bool { return s.Status == StatusPaused })
	time.Sleep(100 * time.Millisecond)
	if after := m.List()[0].Downloaded; after > paused.Downloaded+(256<<10) {
		t.Errorf("kept downloading while paused: %d → %d", paused.Downloaded, after)
	}

	e.files.mu.Lock()
	e.files.chunkWait = 0
	e.files.mu.Unlock()
	if err := m.Resume(snap.ID); err != nil {
		t.Fatal(err)
	}
	s := waitFor(t, m, snap.ID, "done", finished)
	if s.Status != StatusDone || s.Checksum.Status != ChecksumVerified {
		t.Fatalf("status = %s, checksum = %s (%s)", s.Status, s.Checksum.Status, s.Error)
	}
	assertFile(t, filepath.Join(e.dir, "file.bin"), content)
}

// startThenStop begins a slow download and shuts the manager down midway.
func startThenStop(t *testing.T, e *env) string {
	t.Helper()
	e.files.chunkWait = 5 * time.Millisecond
	m := e.manager(t)
	snap, _ := m.Add(e.url(), "")
	waitFor(t, m, snap.ID, "some progress", func(s Snapshot) bool { return s.Downloaded > 512<<10 })
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	e.files.mu.Lock()
	e.files.chunkWait = 0
	e.files.ranges = nil
	e.files.mu.Unlock()
	e.files.served.Store(0)
	return snap.ID
}

func TestResumeAfterRestart(t *testing.T) {
	content := randomBytes(t, 2<<20)
	e := newEnv(t, content)
	id := startThenStop(t, e)

	m := e.manager(t) // fresh manager, same state file
	s := waitFor(t, m, id, "done", finished)
	if s.Status != StatusDone {
		t.Fatalf("status = %s (%s)", s.Status, s.Error)
	}
	assertFile(t, filepath.Join(e.dir, "file.bin"), content)
	if served := e.files.served.Load(); served >= int64(len(content)) {
		t.Errorf("re-downloaded %d bytes after restart; expected to resume the remaining part only", served)
	}
}

func TestRestartWhenFileChangesOnServer(t *testing.T) {
	e := newEnv(t, randomBytes(t, 2<<20))
	id := startThenStop(t, e)

	updated := randomBytes(t, 2<<20)
	e.files.setContent(updated, `"v2"`)

	m := e.manager(t)
	s := waitFor(t, m, id, "done", finished)
	if s.Status != StatusDone {
		t.Fatalf("status = %s (%s)", s.Status, s.Error)
	}
	assertFile(t, filepath.Join(e.dir, "file.bin"), updated)
}

func TestFilenameDoesNotClash(t *testing.T) {
	content := randomBytes(t, 300<<10)
	e := newEnv(t, content)
	m := e.manager(t)

	a, _ := m.Add(e.url(), "")
	waitFor(t, m, a.ID, "done", finished)
	b, _ := m.Add(e.url(), "")
	s := waitFor(t, m, b.ID, "done", finished)
	if s.Filename != "file (1).bin" {
		t.Errorf("second filename = %q", s.Filename)
	}
	assertFile(t, filepath.Join(e.dir, "file (1).bin"), content)
}

func TestUniquePathKeepsCompoundExtension(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "src.tar.gz"), nil, 0o644)
	m := &Manager{cfg: Config{Dir: dir}, downloads: map[string]*download{}}
	if got := m.uniquePathLocked(&download{filename: "src.tar.gz"}); got != filepath.Join(dir, "src (1).tar.gz") {
		t.Errorf("got %q", got)
	}
}

func TestParseChecksum(t *testing.T) {
	sha := sha256hex([]byte("x"))
	tests := []struct {
		in       string
		algo     string
		wantErr  bool
		expected string
	}{
		{in: "", algo: ""},
		{in: sha, algo: "sha256", expected: sha},
		{in: "sha256:" + sha, algo: "sha256", expected: sha},
		{in: "SHA-256=" + strings.ToUpper(sha), algo: "sha256", expected: sha},
		{in: sha + "  ubuntu.iso", algo: "sha256", expected: sha},
		{in: strings.Repeat("a", 32), algo: "md5", expected: strings.Repeat("a", 32)},
		{in: strings.Repeat("b", 40), algo: "sha1", expected: strings.Repeat("b", 40)},
		{in: strings.Repeat("c", 128), algo: "sha512", expected: strings.Repeat("c", 128)},
		{in: "md5:" + sha, wantErr: true},            // wrong length for algorithm
		{in: "crc32:deadbeef", wantErr: true},        // unknown algorithm
		{in: "not-hex-at-all", wantErr: true},        // not hex
		{in: strings.Repeat("a", 50), wantErr: true}, // unknown length
	}
	for _, tt := range tests {
		got, err := ParseChecksum(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseChecksum(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			continue
		}
		if err != nil {
			continue
		}
		if got.Algo != tt.algo || got.Expected != tt.expected {
			t.Errorf("ParseChecksum(%q) = %+v", tt.in, got)
		}
		wantStatus := ChecksumPending
		if tt.in == "" {
			wantStatus = ChecksumNone
		}
		if got.Status != wantStatus {
			t.Errorf("ParseChecksum(%q).Status = %s, want %s", tt.in, got.Status, wantStatus)
		}
	}
}

func TestSanitizeFilename(t *testing.T) {
	for in, want := range map[string]string{
		"report.pdf":          "report.pdf",
		"../../etc/passwd":    "passwd",
		`..\..\evil.exe`:      "evil.exe",
		"..":                  "download",
		"":                    "download",
		"a\x00b\nc.txt":       "abc.txt",
		"/":                   "download",
		"name with space.iso": "name with space.iso",
	} {
		if got := sanitizeFilename(in); got != want {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAddRejectsBadInput(t *testing.T) {
	e := newEnv(t, nil)
	m := e.manager(t)
	if _, err := m.Add("ftp://example.com/x", ""); err == nil {
		t.Error("accepted ftp URL")
	}
	if _, err := m.Add("example.com/x", ""); err == nil {
		t.Error("accepted URL without scheme")
	}
	if _, err := m.Add(e.url(), "sha256:abc"); err == nil {
		t.Error("accepted malformed checksum")
	}
	if len(m.List()) != 0 {
		t.Error("rejected downloads were added")
	}
}
