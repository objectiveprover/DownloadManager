package engine

import (
	"context"
	"sync/atomic"
	"time"
)

// Status is the lifecycle stage of a download:
//
//	queued → downloading → verifying → done      (checksum given and it matched)
//	queued → downloading → done                  (no checksum given)
//	                     ↘ failed                (network error, or checksum mismatch)
//	any running stage ⇄ paused
type Status string

const (
	StatusQueued      Status = "queued"
	StatusDownloading Status = "downloading"
	StatusPaused      Status = "paused"
	StatusVerifying   Status = "verifying"
	StatusDone        Status = "done"
	StatusFailed      Status = "failed"
)

// segment is one byte range fetched by its own connection.
type segment struct {
	start int64
	end   int64 // inclusive; -1 means "until the server closes the stream" (size unknown)
	done  atomic.Int64
}

func (s *segment) complete() bool {
	return s.end >= 0 && s.start+s.done.Load() > s.end
}

// minSegmentSize keeps small files from being split into tiny ranges.
const minSegmentSize = 256 << 10

func planSegments(size int64, acceptRanges bool, n int) []*segment {
	if !acceptRanges || size <= 0 || n < 1 {
		end := int64(-1)
		if size > 0 {
			end = size - 1
		}
		return []*segment{{start: 0, end: end}}
	}
	n = int(min(int64(n), max(1, size/minSegmentSize)))
	chunk := size / int64(n)
	segs := make([]*segment, n)
	for i := range n {
		start := int64(i) * chunk
		end := start + chunk - 1
		if i == n-1 {
			end = size - 1
		}
		segs[i] = &segment{start: start, end: end}
	}
	return segs
}

type download struct {
	// Persisted state. Guarded by Manager.mu.
	id           string
	url          string
	filename     string
	path         string // final location; data is written to path + ".part" until complete
	size         int64  // -1 when unknown
	acceptRanges bool
	etag         string
	lastModified string
	probed       bool
	complete     bool // all bytes are on disk under path
	segs         []*segment
	status       Status
	err          string
	checksum     Checksum
	createdAt    time.Time

	// Runtime only. Guarded by Manager.mu except the atomics.
	running    bool
	removed    bool
	deleteFile bool
	cancel     context.CancelCauseFunc
	hashed     atomic.Int64 // bytes read during the verifying stage
	speed      float64
	lastBytes  int64
	lastTick   time.Time
}

func (d *download) downloaded() int64 {
	var n int64
	for _, s := range d.segs {
		n += s.done.Load()
	}
	return n
}

// Snapshot is a point-in-time view of a download, safe to share with UIs.
type Snapshot struct {
	ID         string    `json:"id"`
	URL        string    `json:"url"`
	Filename   string    `json:"filename"`
	Path       string    `json:"path,omitempty"`
	Size       int64     `json:"size"` // -1 when unknown
	Downloaded int64     `json:"downloaded"`
	Speed      float64   `json:"speed"` // bytes per second
	ETA        float64   `json:"eta"`   // seconds; 0 when unknown
	Segments   int       `json:"segments"`
	Status     Status    `json:"status"`
	Error      string    `json:"error,omitempty"`
	Checksum   Checksum  `json:"checksum"`
	Hashed     int64     `json:"hashed"` // bytes checked so far while verifying
	CreatedAt  time.Time `json:"createdAt"`
}

// Progress returns the download fraction in [0,1], or -1 if the size is unknown.
func (s Snapshot) Progress() float64 {
	if s.Size < 0 {
		return -1
	}
	if s.Size == 0 {
		return 1
	}
	return min(1, float64(s.Downloaded)/float64(s.Size))
}

// VerifyProgress returns the fraction of the file hashed so far in [0,1].
func (s Snapshot) VerifyProgress() float64 {
	if s.Size <= 0 {
		return 0
	}
	return min(1, float64(s.Hashed)/float64(s.Size))
}

func (d *download) snapshot() Snapshot {
	s := Snapshot{
		ID:         d.id,
		URL:        d.url,
		Filename:   d.filename,
		Path:       d.path,
		Size:       d.size,
		Downloaded: d.downloaded(),
		Speed:      d.speed,
		Segments:   len(d.segs),
		Status:     d.status,
		Error:      d.err,
		Checksum:   d.checksum,
		Hashed:     d.hashed.Load(),
		CreatedAt:  d.createdAt,
	}
	if d.complete && d.size >= 0 {
		s.Downloaded = d.size
	}
	if s.Speed > 0 && s.Size > 0 {
		s.ETA = float64(s.Size-s.Downloaded) / s.Speed
	}
	return s
}
