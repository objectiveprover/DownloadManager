package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const maxRetryDelay = 30 * time.Second

// job is the read-only information the workers of one transfer need.
type job struct {
	url       string
	path      string
	size      int64
	ranges    bool   // use Range requests (server supports them and size is known)
	validator string // If-Range value, so a changed file isn't stitched together
	segs      []*segment
}

// transfer downloads the file into path, starting over (at most twice) if
// the server reports the file has changed since the partial data was fetched.
func (m *Manager) transfer(ctx context.Context, d *download) error {
	for restarts := 0; ; restarts++ {
		err := m.transferOnce(ctx, d)
		if !errors.Is(err, errRestart) || restarts == 2 {
			return err
		}
		m.mu.Lock()
		part := d.path + ".part"
		m.resetLocked(d)
		m.changedLocked()
		m.mu.Unlock()
		os.Remove(part)
	}
}

func (m *Manager) transferOnce(ctx context.Context, d *download) error {
	if err := m.ensureProbed(ctx, d); err != nil {
		return err
	}

	m.mu.Lock()
	j := job{
		url:       d.url,
		path:      d.path,
		size:      d.size,
		ranges:    d.acceptRanges && d.size > 0,
		validator: ifRangeValidator(d.etag, d.lastModified),
		segs:      d.segs,
	}
	m.mu.Unlock()

	part := j.path + ".part"
	_, statErr := os.Stat(part)
	fresh := errors.Is(statErr, fs.ErrNotExist)
	f, err := os.OpenFile(part, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return permanent(err)
	}
	if fresh {
		// Progress recorded for a .part file that no longer exists is meaningless.
		for _, s := range j.segs {
			s.done.Store(0)
		}
		if j.size > 0 {
			if err := f.Truncate(j.size); err != nil { // reserve the space up front
				f.Close()
				return permanent(err)
			}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	for _, s := range j.segs {
		if s.complete() {
			continue
		}
		wg.Go(func() {
			if err := m.fetchSegment(ctx, j, f, s); err != nil {
				once.Do(func() { firstErr = err; cancel() })
			}
		})
	}
	wg.Wait()

	closeErr := f.Close()
	if firstErr != nil {
		return firstErr
	}
	if closeErr != nil {
		return permanent(closeErr)
	}

	var got int64
	for _, s := range j.segs {
		got += s.done.Load()
	}
	if j.size >= 0 && got != j.size {
		return fmt.Errorf("download incomplete: got %d of %d bytes", got, j.size)
	}
	if err := os.Rename(part, j.path); err != nil {
		return permanent(err)
	}

	m.mu.Lock()
	d.complete = true
	if d.size < 0 {
		d.size = got
	}
	m.changedLocked()
	m.mu.Unlock()
	return nil
}

func (m *Manager) ensureProbed(ctx context.Context, d *download) error {
	m.mu.Lock()
	probed, rawURL := d.probed, d.url
	m.mu.Unlock()
	if probed {
		return nil
	}

	var r probeResult
	err := m.withRetries(ctx, func() error {
		var err error
		r, err = probe(ctx, m.cfg.Client, rawURL)
		return err
	})
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	d.size = r.size
	d.acceptRanges = r.acceptRanges
	d.etag, d.lastModified = r.etag, r.lastModified
	if d.path == "" {
		if r.filename != "" {
			d.filename = r.filename
		}
		d.path = m.uniquePathLocked(d)
		d.filename = filepath.Base(d.path)
	}
	d.segs = planSegments(d.size, d.acceptRanges, m.cfg.Segments)
	d.probed = true
	m.changedLocked()
	return nil
}

func (m *Manager) withRetries(ctx context.Context, fn func() error) error {
	delay := m.cfg.RetryDelay
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable(err) || attempt >= m.cfg.MaxRetries {
			return err
		}
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

// fetchSegment downloads one segment, retrying with exponential backoff.
// The retry budget resets whenever a failed attempt still made progress, so a
// flaky connection that keeps dropping doesn't fail a long download.
func (m *Manager) fetchSegment(ctx context.Context, j job, f *os.File, s *segment) error {
	delay := m.cfg.RetryDelay
	failures := 0
	for {
		before := s.done.Load()
		err := m.fetchOnce(ctx, j, f, s)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable(err) {
			return err
		}
		if s.done.Load() > before {
			failures, delay = 0, m.cfg.RetryDelay
		}
		failures++
		if failures > m.cfg.MaxRetries {
			return fmt.Errorf("giving up after %d retries: %w", m.cfg.MaxRetries, err)
		}
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

func (m *Manager) fetchOnce(parent context.Context, j job, f *os.File, s *segment) error {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.url, nil)
	if err != nil {
		return permanent(err)
	}
	req.Header.Set("User-Agent", userAgent)

	offset := s.start + s.done.Load()
	if j.ranges {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, s.end))
		if j.validator != "" {
			req.Header.Set("If-Range", j.validator)
		}
	} else {
		// Without range support the only option is to start from the beginning.
		s.done.Store(0)
		offset = 0
		if err := f.Truncate(0); err != nil {
			return permanent(err)
		}
	}

	resp, err := m.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch {
	case j.ranges && resp.StatusCode == http.StatusPartialContent:
		if start, ok := contentRangeStart(resp.Header.Get("Content-Range")); !ok || start != offset {
			return errRestart
		}
	case j.ranges && resp.StatusCode == http.StatusOK:
		return errRestart // If-Range didn't match: the file changed
	case !j.ranges && resp.StatusCode == http.StatusOK:
	default:
		return newStatusError(resp)
	}

	// Abort the connection if the server stops sending data.
	idle := time.AfterFunc(m.cfg.IdleTimeout, func() { cancel(errStalled) })
	defer idle.Stop()

	buf := make([]byte, 64<<10)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			idle.Reset(m.cfg.IdleTimeout)
			chunk := buf[:n]
			if s.end >= 0 {
				if left := s.end + 1 - offset; int64(len(chunk)) > left {
					chunk = chunk[:left]
				}
			}
			if _, err := f.WriteAt(chunk, offset); err != nil {
				return permanent(err)
			}
			offset += int64(len(chunk))
			s.done.Add(int64(len(chunk)))
			if s.end >= 0 && offset > s.end {
				return nil
			}
		}
		if rerr == io.EOF {
			if s.end >= 0 {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
		if rerr != nil {
			if errors.Is(context.Cause(ctx), errStalled) {
				return errStalled
			}
			return rerr
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
