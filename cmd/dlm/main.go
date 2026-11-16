// Command dlm is an HTTPS download manager with a terminal UI and a small
// browser UI served on localhost.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"dlm/internal/engine"
	"dlm/internal/server"
	"dlm/internal/tui"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dlm:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		headless  = flag.Bool("headless", false, "run without the terminal UI (browser UI and API only)")
		addr      = flag.String("addr", "127.0.0.1:7878", "address for the browser UI and API; empty disables it")
		dir       = flag.String("dir", defaultDownloadDir(), "directory to save downloads in")
		statePath = flag.String("state", filepath.Join(dataDir(), "state.json"), "file that remembers downloads between runs")
		segments  = flag.Int("segments", 4, "parallel connections per download")
		maxActive = flag.Int("max-active", 3, "downloads running at the same time")
		checksum  = flag.String("checksum", "", "expected checksum for the URL given as an argument, e.g. sha256:<hash>")
	)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: dlm [flags] [URL...]\n\nURLs given as arguments are added to the queue.\n\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	urls := flag.Args()
	if *checksum != "" && len(urls) != 1 {
		return errors.New("--checksum needs exactly one URL argument")
	}

	// Logs would scribble over the TUI, so send them to a file in that mode.
	if !*headless {
		logPath := filepath.Join(dataDir(), "dlm.log")
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err == nil {
			if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
				defer f.Close()
				log.SetOutput(f)
			}
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mgr, err := engine.New(engine.Config{
		Dir:       *dir,
		StatePath: *statePath,
		Segments:  *segments,
		MaxActive: *maxActive,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := mgr.Close(); err != nil {
			log.Printf("saving state: %v", err)
		}
	}()

	for _, u := range urls {
		if _, err := mgr.Add(u, *checksum); err != nil {
			return fmt.Errorf("%s: %w", u, err)
		}
	}

	webInfo := "browser UI disabled"
	if *addr != "" {
		url, shutdown, err := serve(*addr, mgr)
		if err != nil {
			if *headless {
				return err
			}
			webInfo = "browser UI unavailable: " + err.Error()
		} else {
			defer shutdown()
			webInfo = "browser UI → " + url
		}
	}

	if *headless {
		log.Printf("%s · saving to %s · Ctrl+C to stop", webInfo, *dir)
		<-ctx.Done()
		log.Printf("stopping; unfinished downloads will resume next time")
		return nil
	}
	return tui.Run(ctx, mgr, webInfo)
}

// serve starts the browser UI/API and returns its URL and a shutdown func.
func serve(addr string, mgr *engine.Manager) (string, func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, err
	}
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	ip := net.ParseIP(host)
	loopback := ip != nil && ip.IsLoopback()

	// Cancelling baseCtx ends long-lived event streams so shutdown is quick.
	baseCtx, cancel := context.WithCancel(context.Background())
	srv := &http.Server{
		Handler:           server.Handler(mgr, server.Options{AllowAnyHost: !loopback}),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("web server: %v", err)
		}
	}()

	displayHost := host
	if ip != nil && ip.IsUnspecified() {
		displayHost = "localhost"
	}
	shutdown := func() {
		cancel()
		ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		srv.Shutdown(ctx)
	}
	return "http://" + net.JoinHostPort(displayHost, port), shutdown, nil
}

func dataDir() string {
	if d := os.Getenv("XDG_DATA_HOME"); d != "" {
		return filepath.Join(d, "dlm")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "share", "dlm")
	}
	return ".dlm"
}

func defaultDownloadDir() string {
	if home, err := os.UserHomeDir(); err == nil {
		d := filepath.Join(home, "Downloads")
		if fi, err := os.Stat(d); err == nil && fi.IsDir() {
			return d
		}
	}
	return "."
}
