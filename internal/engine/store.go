package engine

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// The state file lets downloads resume after dlm restarts.

type stateFile struct {
	Version   int      `json:"version"`
	Downloads []record `json:"downloads"`
}

type record struct {
	ID           string      `json:"id"`
	URL          string      `json:"url"`
	Filename     string      `json:"filename"`
	Path         string      `json:"path,omitempty"`
	Size         int64       `json:"size"`
	AcceptRanges bool        `json:"acceptRanges"`
	ETag         string      `json:"etag,omitempty"`
	LastModified string      `json:"lastModified,omitempty"`
	Probed       bool        `json:"probed"`
	Complete     bool        `json:"complete"`
	Segments     []segRecord `json:"segments,omitempty"`
	Status       Status      `json:"status"`
	Error        string      `json:"error,omitempty"`
	Checksum     Checksum    `json:"checksum"`
	CreatedAt    time.Time   `json:"createdAt"`
}

type segRecord struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Done  int64 `json:"done"`
}

func (m *Manager) saveLocked() error {
	if m.cfg.StatePath == "" {
		return nil
	}
	st := stateFile{Version: 1, Downloads: make([]record, 0, len(m.order))}
	for _, id := range m.order {
		d := m.downloads[id]
		r := record{
			ID: d.id, URL: d.url, Filename: d.filename, Path: d.path, Size: d.size,
			AcceptRanges: d.acceptRanges, ETag: d.etag, LastModified: d.lastModified,
			Probed: d.probed, Complete: d.complete, Status: d.status, Error: d.err,
			Checksum: d.checksum, CreatedAt: d.createdAt,
		}
		for _, s := range d.segs {
			r.Segments = append(r.Segments, segRecord{Start: s.start, End: s.end, Done: s.done.Load()})
		}
		st.Downloads = append(st.Downloads, r)
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.cfg.StatePath), 0o755); err != nil {
		return err
	}
	tmp := m.cfg.StatePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.cfg.StatePath)
}

func (m *Manager) load() error {
	if m.cfg.StatePath == "" {
		return nil
	}
	data, err := os.ReadFile(m.cfg.StatePath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var st stateFile
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	for _, r := range st.Downloads {
		d := &download{
			id: r.ID, url: r.URL, filename: r.Filename, path: r.Path, size: r.Size,
			acceptRanges: r.AcceptRanges, etag: r.ETag, lastModified: r.LastModified,
			probed: r.Probed, complete: r.Complete, status: r.Status, err: r.Error,
			checksum: r.Checksum, createdAt: r.CreatedAt,
		}
		for _, sr := range r.Segments {
			s := &segment{start: sr.Start, end: sr.End}
			s.done.Store(sr.Done)
			d.segs = append(d.segs, s)
		}
		// Anything that was running when dlm stopped picks up where it left off.
		if d.status == StatusDownloading || d.status == StatusVerifying {
			d.status = StatusQueued
		}
		if d.checksum.Status == ChecksumVerifying {
			d.checksum.Status = ChecksumPending
		}
		m.downloads[d.id] = d
		m.order = append(m.order, d.id)
	}
	return nil
}
