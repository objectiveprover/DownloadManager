// Package engine downloads files over HTTP(S) using parallel range requests,
// resumes interrupted transfers, and verifies checksums when one is given.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var ErrNotFound = errors.New("download not found")

type Config struct {
	Dir         string        // where finished files are saved
	StatePath   string        // JSON file for resuming across restarts; empty disables it
	Segments    int           // parallel connections per download
	MaxActive   int           // downloads running at the same time
	MaxRetries  int           // retries per connection before giving up
	RetryDelay  time.Duration // first backoff delay; doubles each retry
	IdleTimeout time.Duration // abort a connection that receives no data for this long
	Client      *http.Client
}

func (c *Config) setDefaults() {
	if c.Segments < 1 {
		c.Segments = 4
	}
	if c.MaxActive < 1 {
		c.MaxActive = 3
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	} else if c.MaxRetries == 0 {
		c.MaxRetries = 5
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = time.Second
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.Client == nil {
		c.Client = DefaultClient()
	}
}

// DefaultClient verifies TLS certificates (Go's default) and sets timeouts on
// each phase of a connection, but no overall timeout so large files can finish.
func DefaultClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   16,
		ForceAttemptHTTP2:     true,
	}}
}

// Manager owns all downloads and runs up to Config.MaxActive at once.
type Manager struct {
	cfg      Config
	ctx      context.Context
	stop     context.CancelCauseFunc
	wg       sync.WaitGroup
	loopDone chan struct{}
	kick     chan struct{}

	mu        sync.Mutex
	downloads map[string]*download
	order     []string
	running   int
	dirty     bool // something changed since the last broadcast
	unsaved   bool // something changed since the last save
	closed    bool
	subs      map[chan []Snapshot]struct{}
}

// New loads saved state, starts any queued downloads, and returns the manager.
// Call Close to stop downloads and save state.
func New(cfg Config) (*Manager, error) {
	cfg.setDefaults()
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	ctx, stop := context.WithCancelCause(context.Background())
	m := &Manager{
		cfg:       cfg,
		ctx:       ctx,
		stop:      stop,
		loopDone:  make(chan struct{}),
		kick:      make(chan struct{}, 1),
		downloads: map[string]*download{},
		subs:      map[chan []Snapshot]struct{}{},
	}
	if err := m.load(); err != nil {
		stop(nil)
		return nil, fmt.Errorf("loading state from %s: %w", cfg.StatePath, err)
	}
	go m.loop()
	m.mu.Lock()
	m.scheduleLocked()
	m.mu.Unlock()
	return m, nil
}

// Close stops all downloads (they resume on the next start) and saves state.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	m.stop(errShutdown)
	m.wg.Wait()
	<-m.loopDone

	m.mu.Lock()
	defer m.mu.Unlock()
	for ch := range m.subs {
		close(ch)
		delete(m.subs, ch)
	}
	return m.saveLocked()
}

// Add queues a new download. checksum may be empty; otherwise the finished
// file is verified against it automatically (see ParseChecksum for formats).
func (m *Manager) Add(rawURL, checksum string) (Snapshot, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return Snapshot{}, errors.New("enter a full http:// or https:// URL")
	}
	cs, err := ParseChecksum(checksum)
	if err != nil {
		return Snapshot{}, err
	}
	d := &download{
		id:        newID(),
		url:       u.String(),
		filename:  filenameFromURL(u),
		size:      -1,
		status:    StatusQueued,
		checksum:  cs,
		createdAt: time.Now(),
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Snapshot{}, errors.New("download manager is shutting down")
	}
	m.downloads[d.id] = d
	m.order = append(m.order, d.id)
	m.changedLocked()
	m.scheduleLocked()
	return d.snapshot(), nil
}

// Pause stops a queued or running download, keeping what was downloaded so far.
func (m *Manager) Pause(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.downloads[id]
	if !ok {
		return ErrNotFound
	}
	switch d.status {
	case StatusQueued, StatusDownloading, StatusVerifying:
		d.status = StatusPaused
		if d.cancel != nil {
			d.cancel(errPaused)
		}
	default:
		return fmt.Errorf("can't pause a download that is %s", d.status)
	}
	m.changedLocked()
	m.scheduleLocked()
	return nil
}

// Resume queues a paused or failed download again. After a checksum mismatch
// the file is discarded and downloaded from scratch.
func (m *Manager) Resume(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.downloads[id]
	if !ok {
		return ErrNotFound
	}
	switch d.status {
	case StatusPaused, StatusFailed:
	default:
		return fmt.Errorf("can't resume a download that is %s", d.status)
	}
	if d.checksum.Status == ChecksumMismatch {
		if d.path != "" {
			os.Remove(d.path)
		}
		m.resetLocked(d)
	}
	d.status = StatusQueued
	d.err = ""
	m.changedLocked()
	m.scheduleLocked()
	return nil
}

// Remove drops a download from the list and deletes its partial data.
// A finished file is kept unless deleteFile is true.
func (m *Manager) Remove(id string, deleteFile bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.downloads[id]
	if !ok {
		return ErrNotFound
	}
	delete(m.downloads, id)
	for i, oid := range m.order {
		if oid == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	d.removed = true
	d.deleteFile = deleteFile
	if d.running {
		d.cancel(errRemoved) // files are cleaned up once the worker stops writing
	} else {
		removeFiles(d)
	}
	m.changedLocked()
	m.scheduleLocked()
	return nil
}

func removeFiles(d *download) {
	if d.path == "" {
		return
	}
	os.Remove(d.path + ".part")
	if d.deleteFile && d.complete {
		os.Remove(d.path)
	}
}

// List returns all downloads in the order they were added.
func (m *Manager) List() []Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.listLocked()
}

func (m *Manager) listLocked() []Snapshot {
	out := make([]Snapshot, 0, len(m.order))
	for _, id := range m.order {
		out = append(out, m.downloads[id].snapshot())
	}
	return out
}

// Subscribe returns a channel that receives the full download list whenever
// it changes (at most every 250ms while downloads are running). Slow readers
// only see the latest list. Call the returned func to unsubscribe.
func (m *Manager) Subscribe() (<-chan []Snapshot, func()) {
	ch := make(chan []Snapshot, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		close(ch)
		return ch, func() {}
	}
	m.subs[ch] = struct{}{}
	ch <- m.listLocked()
	return ch, func() {
		m.mu.Lock()
		delete(m.subs, ch)
		m.mu.Unlock()
	}
}

func (m *Manager) changedLocked() {
	m.dirty = true
	m.unsaved = true
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

func (m *Manager) scheduleLocked() {
	if m.closed {
		return
	}
	for _, id := range m.order {
		if m.running >= m.cfg.MaxActive {
			return
		}
		d := m.downloads[id]
		if d.status == StatusQueued && !d.running {
			m.startLocked(d)
		}
	}
}

func (m *Manager) startLocked(d *download) {
	ctx, cancel := context.WithCancelCause(m.ctx)
	d.running = true
	d.cancel = cancel
	d.status = StatusDownloading
	d.err = ""
	d.speed = 0
	d.lastTick = time.Time{}
	m.running++
	m.wg.Go(func() { m.run(ctx, d) })
}

// resetLocked forgets everything learned about the remote file so the next
// run starts from zero (keeping the same destination path).
func (m *Manager) resetLocked(d *download) {
	d.probed = false
	d.complete = false
	d.size = -1
	d.acceptRanges = false
	d.etag, d.lastModified = "", ""
	d.segs = nil
	d.hashed.Store(0)
	if d.checksum.Expected != "" {
		d.checksum.Actual = ""
		d.checksum.Status = ChecksumPending
	}
}

func (m *Manager) run(ctx context.Context, d *download) {
	err := m.process(ctx, d)

	m.mu.Lock()
	defer m.mu.Unlock()
	d.running = false
	d.cancel(nil)
	d.cancel = nil
	d.speed = 0
	m.running--

	switch {
	case d.removed:
		removeFiles(d)
	case err == nil:
		// process already set the final status.
	case ctx.Err() != nil:
		if errors.Is(context.Cause(ctx), errPaused) {
			if d.status == StatusDownloading || d.status == StatusVerifying {
				d.status = StatusPaused
			}
		} else {
			d.status = StatusQueued // shutting down: carry on next time
		}
		if d.checksum.Status == ChecksumVerifying {
			d.checksum.Status = ChecksumPending
		}
	default:
		d.status = StatusFailed
		d.err = describeErr(err)
		if d.checksum.Status == ChecksumVerifying {
			d.checksum.Status = ChecksumPending
		}
	}
	m.changedLocked()
	m.scheduleLocked()
}

func (m *Manager) process(ctx context.Context, d *download) error {
	m.mu.Lock()
	complete := d.complete
	m.mu.Unlock()
	if !complete {
		if err := m.transfer(ctx, d); err != nil {
			return err
		}
	}
	return m.verify(ctx, d)
}

// verify runs the checksum step automatically when a checksum was given.
func (m *Manager) verify(ctx context.Context, d *download) error {
	m.mu.Lock()
	if d.checksum.Expected == "" {
		d.status = StatusDone
		d.checksum.Status = ChecksumNone
		m.changedLocked()
		m.mu.Unlock()
		return nil
	}
	if ctx.Err() != nil {
		m.mu.Unlock()
		return ctx.Err()
	}
	d.status = StatusVerifying
	d.checksum.Status = ChecksumVerifying
	d.checksum.Actual = ""
	d.hashed.Store(0)
	cs, path := d.checksum, d.path
	m.changedLocked()
	m.mu.Unlock()

	actual, err := hashFile(ctx, path, cs.Algo, &d.hashed)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("verifying checksum: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	d.checksum.Actual = actual
	if checksumsEqual(actual, cs.Expected) {
		d.checksum.Status = ChecksumVerified
		d.status = StatusDone
	} else {
		d.checksum.Status = ChecksumMismatch
		d.status = StatusFailed
		d.err = fmt.Sprintf("checksum mismatch: the file's %s hash doesn't match the one you entered", cs.Algo)
	}
	m.changedLocked()
	return nil
}

// loop broadcasts snapshots to subscribers, tracks speeds, and saves state.
func (m *Manager) loop() {
	defer close(m.loopDone)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	lastSave := time.Now()

	for {
		tick := false
		select {
		case <-m.ctx.Done():
			return
		case <-m.kick:
		case <-ticker.C:
			tick = true
		}
		now := time.Now()

		m.mu.Lock()
		if tick && m.updateSpeedsLocked(now) {
			m.dirty = true
			m.unsaved = true
		}
		var snaps []Snapshot
		var subs []chan []Snapshot
		if m.dirty {
			snaps = m.listLocked()
			for ch := range m.subs {
				subs = append(subs, ch)
			}
			m.dirty = false
		}
		if m.unsaved && now.Sub(lastSave) >= time.Second {
			if err := m.saveLocked(); err != nil {
				log.Printf("saving state: %v", err)
			}
			m.unsaved = false
			lastSave = now
		}
		m.mu.Unlock()

		for _, ch := range subs {
			select { // drop a stale list the reader hasn't taken yet
			case <-ch:
			default:
			}
			select {
			case ch <- snaps:
			default:
			}
		}
	}
}

// updateSpeedsLocked refreshes the smoothed speed of running downloads and
// reports whether any are running.
func (m *Manager) updateSpeedsLocked(now time.Time) bool {
	active := false
	for _, d := range m.downloads {
		if !d.running {
			continue
		}
		active = true
		if d.status != StatusDownloading {
			d.speed, d.lastTick = 0, time.Time{}
			continue
		}
		total := d.downloaded()
		if !d.lastTick.IsZero() {
			if dt := now.Sub(d.lastTick).Seconds(); dt > 0 {
				inst := max(0, float64(total-d.lastBytes)/dt)
				if d.speed == 0 {
					d.speed = inst
				} else {
					d.speed = 0.3*inst + 0.7*d.speed
				}
			}
		}
		d.lastBytes, d.lastTick = total, now
	}
	return active
}

// uniquePathLocked picks a destination that doesn't clash with an existing
// file or another download: "file.iso", "file (1).iso", ...
func (m *Manager) uniquePathLocked(d *download) string {
	ext := filepath.Ext(d.filename)
	stem := strings.TrimSuffix(d.filename, ext)
	if inner := filepath.Ext(stem); strings.EqualFold(inner, ".tar") { // keep "x.tar.gz" → "x (1).tar.gz"
		ext = inner + ext
		stem = strings.TrimSuffix(stem, inner)
	}
	for i := 0; ; i++ {
		name := d.filename
		if i > 0 {
			name = fmt.Sprintf("%s (%d)%s", stem, i, ext)
		}
		p := filepath.Join(m.cfg.Dir, name)
		if !m.pathTakenLocked(p, d) {
			return p
		}
	}
}

func (m *Manager) pathTakenLocked(p string, self *download) bool {
	for _, o := range m.downloads {
		if o != self && o.path == p {
			return true
		}
	}
	for _, candidate := range []string{p, p + ".part"} {
		if _, err := os.Lstat(candidate); err == nil {
			return true
		}
	}
	return false
}

func newID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return hex.EncodeToString(b)
}
