package main

import (
	"context"
	"errors"
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
	if flag.NArg() != 0 {
		return errors.New("configuration_arguments")
	}
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
	startup := service.NewStartupHandler()
	server := &http.Server{Handler: startup, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768}
	defer func() { _ = server.Close() }()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
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
	startup.Activate(service.Handler(coll, r))
	finished := make(chan error, 1)
	go func(done chan<- error) { done <- coll.Run(ctx) }(finished)
	var result error
	select {
	case result = <-finished:
		finished = nil
		cancel()
	case result = <-serving:
		cancel()
		if errors.Is(result, http.ErrServerClosed) {
			result = nil
		}
	case <-ctx.Done():
	}
	shutdown, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	shutdownErr := waitForShutdown(shutdown, server, finished)
	if result != nil {
		return result
	}
	return shutdownErr
}

func waitForShutdown(ctx context.Context, server *http.Server, collectorDone <-chan error) error {
	httpErr := server.Shutdown(ctx)
	if errors.Is(httpErr, context.DeadlineExceeded) {
		return errors.New("shutdown_timeout")
	}
	// The collector owns the database and stops/reaps the renderer before releasing its lock.
	if collectorDone != nil {
		select {
		case err := <-collectorDone:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return errors.New("shutdown_timeout")
		}
	}
	if httpErr != nil {
		return errors.New("http_shutdown")
	}
	return nil
}
