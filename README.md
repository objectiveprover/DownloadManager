# dlm

An HTTPS download manager with a terminal UI and a small browser UI. It uses
parallel range requests, resumes interrupted downloads (even after a restart),
and verifies a checksum when you give one.

## Run

```sh
go run ./cmd/dlm                      # TUI, plus browser UI at http://127.0.0.1:7878
go run ./cmd/dlm --headless           # browser UI / API only
go run ./cmd/dlm --checksum sha256:<hash> https://example.com/file.iso
```

| Flag | Default | |
|---|---|---|
| `--dir` | `~/Downloads` | where files are saved |
| `--addr` | `127.0.0.1:7878` | browser UI / API address (`""` disables it) |
| `--segments` | `4` | parallel connections per download |
| `--max-active` | `3` | downloads running at once |
| `--state` | `~/.local/share/dlm/state.json` | remembers downloads between runs |

## Checksums

Every "Add download" form has a **URL** field and a **Checksum** field. Paste the
hash published next to the file into the Checksum field:

- `sha256:<hash>`, `sha512:<hash>`, `sha1:<hash>`, `md5:<hash>`
- a bare hash (its algorithm is detected from its length)
- a line of `sha256sum` output (`<hash>  file.iso`)

When the Checksum field is filled in, the file is verified automatically as soon
as it finishes downloading. Every download shows one of these results:

- **✓ Verified**: the hash matches.
- **✗ Checksum mismatch**: the expected and actual hashes are shown. You can re-download or delete the file.
- **– No checksum**: the field was left empty, so nothing was checked.

## TUI keys

`a` add · `p`/space pause/resume · `r` retry / re-download · `d` remove from list ·
`x` delete finished file · `↑↓`/`jk` select · `q` quit

Quitting saves progress, and unfinished downloads resume the next time you start dlm.
