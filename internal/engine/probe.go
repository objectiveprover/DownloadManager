package engine

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

const userAgent = "dlm/0.1"

type probeResult struct {
	size         int64 // -1 when unknown
	acceptRanges bool
	etag         string
	lastModified string
	filename     string
}

// probe asks for the first byte of the file. A 206 reply tells us the server
// supports ranges and the total size; a 200 means it ignores ranges.
func probe(ctx context.Context, c *http.Client, rawURL string) (probeResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return probeResult{}, permanent(err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Range", "bytes=0-0")

	resp, err := c.Do(req)
	if err != nil {
		return probeResult{}, err
	}
	defer resp.Body.Close()

	r := probeResult{
		size:         -1,
		etag:         resp.Header.Get("ETag"),
		lastModified: resp.Header.Get("Last-Modified"),
		filename:     filenameFromResponse(resp),
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if total, ok := contentRangeTotal(resp.Header.Get("Content-Range")); ok {
			r.size = total
			r.acceptRanges = true
		}
	case http.StatusOK:
		r.size = resp.ContentLength
	case http.StatusRequestedRangeNotSatisfiable:
		// Usually an empty file ("bytes */0"); otherwise fall back to a plain GET.
		if total, ok := contentRangeTotal(resp.Header.Get("Content-Range")); ok && total == 0 {
			r.size = 0
		}
	default:
		return probeResult{}, newStatusError(resp)
	}
	return r, nil
}

// contentRangeTotal parses the total from "bytes 0-0/12345".
func contentRangeTotal(h string) (int64, bool) {
	i := strings.LastIndexByte(h, '/')
	if i < 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(h[i+1:], 10, 64)
	return n, err == nil && n >= 0
}

// contentRangeStart parses the first byte from "bytes 100-199/12345".
func contentRangeStart(h string) (int64, bool) {
	h, ok := strings.CutPrefix(h, "bytes ")
	if !ok {
		return 0, false
	}
	i := strings.IndexByte(h, '-')
	if i < 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(h[:i], 10, 64)
	return n, err == nil
}

func filenameFromResponse(resp *http.Response) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil && params["filename"] != "" {
			return sanitizeFilename(params["filename"])
		}
	}
	return filenameFromURL(resp.Request.URL)
}

func filenameFromURL(u *url.URL) string {
	return sanitizeFilename(path.Base(u.Path))
}

// sanitizeFilename turns a server-supplied name into a safe base name, so a
// header like `filename="../../.bashrc"` can't write outside the download dir.
func sanitizeFilename(name string) string {
	name = strings.ReplaceAll(name, "\\", "/")
	name = path.Base(name)
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || name == "/" {
		return "download"
	}
	return name
}

// ifRangeValidator picks the value for the If-Range header. Only strong ETags
// are allowed there; otherwise fall back to Last-Modified.
func ifRangeValidator(etag, lastModified string) string {
	if etag != "" && !strings.HasPrefix(etag, "W/") {
		return etag
	}
	return lastModified
}

// Errors and retry classification.

var (
	errPaused   = errors.New("paused")
	errRemoved  = errors.New("removed")
	errShutdown = errors.New("shutting down")
	errStalled  = errors.New("connection stalled: no data received")
	// errRestart means the server ignored our Range request or the file
	// changed since we started, so partial data can't be reused.
	errRestart = errors.New("file changed on the server")
)

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func permanent(err error) error { return &permanentError{err} }

type statusError struct {
	code   int
	status string
}

func (e *statusError) Error() string { return "server replied " + e.status }

func newStatusError(resp *http.Response) error {
	return &statusError{code: resp.StatusCode, status: resp.Status}
}

// retryable reports whether a failed request is worth trying again.
func retryable(err error) bool {
	var pe *permanentError
	if errors.As(err, &pe) || errors.Is(err, errRestart) {
		return false
	}
	var se *statusError
	if errors.As(err, &se) {
		return se.code == http.StatusRequestTimeout || se.code == http.StatusTooManyRequests || se.code >= 500
	}
	var ce *tls.CertificateVerificationError
	if errors.As(err, &ce) {
		return false
	}
	var ue *url.Error
	if errors.As(err, &ue) && strings.Contains(ue.Err.Error(), "unsupported protocol scheme") {
		return false
	}
	return true
}

func describeErr(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Sprintf("%s: %v", ue.Op, ue.Err)
	}
	return err.Error()
}
