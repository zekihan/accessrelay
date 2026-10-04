package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zekihan/accessrelay/internal/collector"
	"github.com/zekihan/accessrelay/internal/config"
	"github.com/zekihan/accessrelay/internal/renderer"
	"github.com/zekihan/accessrelay/internal/service"
	"github.com/zekihan/accessrelay/internal/source"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("config", "/config/accessrelay.json", "configuration file")
	validate := flag.Bool("validate", false, "validate configuration and exit")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return nil
	}
	c, err := config.Load(*path)
	if err != nil {
		return err
	}
	if *validate {
		return nil
	}
	listener, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return fmt.Errorf("http_listen")
	}
	defer func() { _ = listener.Close() }()
	backend := source.New(c)
	defer backend.Close()
	coll, err := collector.New(c, backend, nil)
	if err != nil {
		return err
	}
	r, err := renderer.New(c)
	if err != nil {
		_ = coll.Store.Close()
		return err
	}
	coll.Consumer = r
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go r.Run(ctx)
	server := &http.Server{Handler: service.Handler(coll, r), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768}
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	finished := make(chan error, 1)
	go func() { finished <- coll.Run(ctx) }()
	var result error
	collectorFinished := false
	select {
	case result = <-finished:
		collectorFinished = true
		cancel()
	case result = <-serving:
		cancel()
		if result == http.ErrServerClosed {
			result = nil
		}
	case <-ctx.Done():
	}
	shutdown, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	_ = server.Shutdown(shutdown)
	// The collector owns the database and stops/reaps the renderer before releasing its lock.
	if collectorFinished {
		return result
	}
	select {
	case err = <-finished:
		if result == nil {
			result = err
		}
	case <-shutdown.Done():
		if result == nil {
			result = fmt.Errorf("shutdown_timeout")
		}
	}
	return result
}
