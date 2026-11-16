package engine

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
	"sync/atomic"
)

// ChecksumStatus is where a download is in the checksum verification step.
type ChecksumStatus string

const (
	ChecksumNone      ChecksumStatus = "none"      // no checksum was given; nothing will be checked
	ChecksumPending   ChecksumStatus = "pending"   // will be verified automatically once the download finishes
	ChecksumVerifying ChecksumStatus = "verifying" // the finished file is being hashed
	ChecksumVerified  ChecksumStatus = "verified"  // hash matches
	ChecksumMismatch  ChecksumStatus = "mismatch"  // hash does not match
)

// Checksum is the expected hash of a download and the result of checking it.
type Checksum struct {
	Algo     string         `json:"algo,omitempty"`
	Expected string         `json:"expected,omitempty"`
	Actual   string         `json:"actual,omitempty"`
	Status   ChecksumStatus `json:"status"`
}

var algoAliases = map[string]string{
	"md5": "md5", "sha1": "sha1", "sha-1": "sha1",
	"sha256": "sha256", "sha-256": "sha256",
	"sha512": "sha512", "sha-512": "sha512",
}

var algoByHexLen = map[int]string{32: "md5", 40: "sha1", 64: "sha256", 128: "sha512"}

func newHash(algo string) hash.Hash {
	switch algo {
	case "md5":
		return md5.New()
	case "sha1":
		return sha1.New()
	case "sha256":
		return sha256.New()
	case "sha512":
		return sha512.New()
	}
	return nil
}

// ParseChecksum reads what the user typed in the Checksum field. It accepts:
//
//	""                         no checksum
//	"sha256:<hex>"             explicit algorithm (also "sha256=<hex>", "SHA-256:<hex>")
//	"<hex>"                    algorithm detected from length: md5, sha1, sha256 or sha512
//	"<hex>  file.iso"          a line copied from sha256sum-style output
func ParseChecksum(s string) (Checksum, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Checksum{Status: ChecksumNone}, nil
	}

	algo := ""
	if i := strings.IndexAny(s, ":="); i > 0 && !strings.ContainsAny(s[:i], " \t") {
		name := strings.ToLower(s[:i])
		a, ok := algoAliases[name]
		if !ok {
			return Checksum{}, fmt.Errorf("unknown checksum algorithm %q (use md5, sha1, sha256 or sha512)", name)
		}
		algo = a
		s = strings.TrimSpace(s[i+1:])
	}
	if fields := strings.Fields(s); len(fields) > 0 {
		s = fields[0]
	}

	value := strings.ToLower(s)
	if _, err := hex.DecodeString(value); err != nil || value == "" {
		return Checksum{}, fmt.Errorf("checksum must be a hexadecimal hash, e.g. sha256:e3b0c442…")
	}

	if algo == "" {
		a, ok := algoByHexLen[len(value)]
		if !ok {
			return Checksum{}, fmt.Errorf("can't tell the algorithm of a %d-character hash; prefix it, e.g. sha256:<hash>", len(value))
		}
		algo = a
	} else if want := newHash(algo).Size() * 2; len(value) != want {
		return Checksum{}, fmt.Errorf("a %s hash is %d hex characters, got %d", algo, want, len(value))
	}

	return Checksum{Algo: algo, Expected: value, Status: ChecksumPending}, nil
}

// hashFile hashes the file at path, adding the bytes read to progress as it goes.
func hashFile(ctx context.Context, path, algo string, progress *atomic.Int64) (string, error) {
	h := newHash(algo)
	if h == nil {
		return "", fmt.Errorf("unsupported checksum algorithm %q", algo)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	buf := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			progress.Add(int64(n))
		}
		if err == io.EOF {
			return hex.EncodeToString(h.Sum(nil)), nil
		}
		if err != nil {
			return "", err
		}
	}
}

func checksumsEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
