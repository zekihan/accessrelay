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

	commonsmetrics "github.com/zekihan/go-commons/metrics"

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
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", c.Listen)
	if err != nil {
		return errors.New("http_listen")
	}
	defer func() { _ = listener.Close() }()
	var metricsListener net.Listener
	if c.MetricsAddr != "" {
		metricsListener, err = (&net.ListenConfig{}).Listen(
			context.Background(),
			"tcp",
			c.MetricsAddr,
		)
		if err != nil {
			return errors.New("metrics_listen")
		}
		defer func() { _ = metricsListener.Close() }()
	}
	startup := service.NewStartupHandler()
	registry := commonsmetrics.New()
	server := &http.Server{
		Handler:           registry.Middleware(startup),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32768,
	}
	defer func() { _ = server.Close() }()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	metricsDone := make(chan error, 1)
	var metricsServer *http.Server
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
	if metricsListener != nil {
		metricsServer = &http.Server{
			Handler:           service.MetricsHandler(registry, coll, r),
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    32768,
		}
		go func() { metricsDone <- metricsServer.Serve(metricsListener) }()
	} else {
		close(metricsDone)
	}
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
	if metricsListener != nil {
		metricsShutdownErr := metricsServer.Shutdown(shutdown)
		metricsDoneErr := <-metricsDone
		if errors.Is(metricsDoneErr, http.ErrServerClosed) {
			metricsDoneErr = nil
		}
		shutdownErr = errors.Join(shutdownErr, metricsShutdownErr, metricsDoneErr)
	}
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
