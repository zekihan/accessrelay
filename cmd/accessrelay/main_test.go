package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
)

// runForTest invokes run with a fresh flag set so the global flag package
// state from a previous invocation cannot panic on re-registration.
func runForTest(t *testing.T, args ...string) (string, error) {
	t.Helper()
	oldCommandLine := flag.CommandLine
	oldArgs := os.Args
	oldStdout := os.Stdout
	t.Cleanup(func() {
		flag.CommandLine = oldCommandLine
		os.Args = oldArgs
		os.Stdout = oldStdout
	})
	flag.CommandLine = flag.NewFlagSet("accessrelay", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	os.Args = append([]string{"accessrelay"}, args...)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	runErr := run()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout = oldStdout
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return string(out), runErr
}

func writeTestConfig(t *testing.T, mutate func(*config.Config)) string {
	t.Helper()
	root := t.TempDir()
	c := config.Default()
	c.Collection.Source.Cluster = "example"
	c.Listen = "127.0.0.1:0"
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.ConnectionFile = filepath.Join(root, "connection.json")
	if mutate != nil {
		mutate(&c)
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunVersion(t *testing.T) {
	out, err := runForTest(t, "--version")
	if err != nil || out != version+"\n" {
		t.Fatalf("out=%q err=%v, want version %q", out, err, version)
	}
}

func TestRunValidate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		out, err := runForTest(t, "--config", writeTestConfig(t, nil), "--validate")
		if err != nil || out != "" {
			t.Fatalf("out=%q err=%v", out, err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		_, err := runForTest(t, "--config", filepath.Join(t.TempDir(), "absent.json"), "--validate")
		if err == nil || err.Error() != "configuration_unreadable" {
			t.Fatalf("err=%v, want configuration_unreadable", err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := runForTest(t, "--config", path, "--validate")
		if err == nil || err.Error() != "configuration_invalid" {
			t.Fatalf("err=%v, want configuration_invalid", err)
		}
	})
	t.Run("invalid value", func(t *testing.T) {
		path := writeTestConfig(t, func(c *config.Config) { c.Report.HistoryDays = 0 })
		_, err := runForTest(t, "--config", path, "--validate")
		if err == nil || err.Error() != "configuration_report" {
			t.Fatalf("err=%v, want configuration_report", err)
		}
	})
}

func TestRunRejectsPositionalArguments(t *testing.T) {
	for _, args := range [][]string{
		{"unexpected"},
		{"--version", "unexpected"},
		{"--config", filepath.Join(t.TempDir(), "absent.json"), "unexpected", "--validate"},
	} {
		if _, err := runForTest(
			t,
			args...); err == nil ||
			err.Error() != "configuration_arguments" {
			t.Fatalf("args=%v err=%v, want configuration_arguments", args, err)
		}
	}
}

func freePort(t *testing.T) (string, net.Listener) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener.Addr().String(), listener
}

func TestRunListenFailure(t *testing.T) {
	addr, occupied := freePort(t)
	defer occupied.Close()
	path := writeTestConfig(t, func(c *config.Config) { c.Listen = addr })
	if _, err := runForTest(t, "--config", path); err == nil || err.Error() != "http_listen" {
		t.Fatalf("err=%v, want http_listen", err)
	}
}

func TestRunStartupStorageFailure(t *testing.T) {
	addr, probe := freePort(t)
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	var stateDir string
	path := writeTestConfig(t, func(c *config.Config) {
		c.Listen = addr
		stateDir = c.StateDir
	})
	// A directory where SQLite expects its database file makes collector
	// construction fail after the listener has been bound.
	if err := os.MkdirAll(filepath.Join(stateDir, "events.sqlite"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := runForTest(t, "--config", path); err == nil {
		t.Fatal("run succeeded with an unusable database path")
	} else if err.Error() == "http_listen" {
		t.Fatalf("err=%v, want a post-listen startup error", err)
	}
}

func TestWaitForShutdownReportsHTTPTimeoutAfterCollectorExit(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		<-release
	}))
	defer server.Close()
	defer close(release)

	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	ctx, cancel := context.WithDeadline(t.Context(), time.Now())
	defer cancel()
	if err := waitForShutdown(
		ctx,
		server.Config,
		nil,
	); err == nil ||
		err.Error() != "shutdown_timeout" {
		t.Fatalf("got %v, want shutdown_timeout with an active request", err)
	}
}

func TestWaitForShutdownCollectorOutcome(t *testing.T) {
	t.Run("collector error wins", func(t *testing.T) {
		server := httptest.NewServer(
			http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) },
			),
		)
		defer server.Close()
		done := make(chan error, 1)
		done <- errCollector
		if err := waitForShutdown(t.Context(), server.Config, done); !errors.Is(err, errCollector) {
			t.Fatalf("err=%v, want collector error", err)
		}
	})
	t.Run("clean collector and http", func(t *testing.T) {
		server := httptest.NewServer(
			http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) },
			),
		)
		defer server.Close()
		done := make(chan error, 1)
		done <- nil
		if err := waitForShutdown(t.Context(), server.Config, done); err != nil {
			t.Fatalf("err=%v, want nil", err)
		}
	})
	t.Run("collector never finishes", func(t *testing.T) {
		server := httptest.NewServer(
			http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) },
			),
		)
		defer server.Close()
		ctx, cancel := context.WithDeadline(t.Context(), time.Now())
		defer cancel()
		done := make(chan error)
		if err := waitForShutdown(
			ctx,
			server.Config,
			done,
		); err == nil ||
			err.Error() != "shutdown_timeout" {
			t.Fatalf("err=%v, want shutdown_timeout", err)
		}
	})
}

var errCollector = errors.New("collector_failed")

func TestRunServesHTTPAndExitsOnSignal(t *testing.T) {
	addr, probe := freePort(t)
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	metricsAddr, metricsProbe := freePort(t)
	if err := metricsProbe.Close(); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "goaccess")
	// #nosec G306 -- test executables need the execute bit.
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := writeTestConfig(t, func(c *config.Config) {
		c.Listen = addr
		c.MetricsAddr = metricsAddr
		c.GoAccessBinary = binary
	})
	oldCommandLine := flag.CommandLine
	oldArgs := os.Args
	t.Cleanup(func() { flag.CommandLine = oldCommandLine; os.Args = oldArgs })
	flag.CommandLine = flag.NewFlagSet("accessrelay", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	os.Args = []string{"accessrelay", "--config", path}

	result := make(chan error, 1)
	go func() { result <- run() }()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(15 * time.Second)
	var status int
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + metricsAddr + "/metrics")
		if err == nil {
			status = resp.StatusCode
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if status == http.StatusOK {
				break
			}
			status = 0
		}
		select {
		case err := <-result:
			t.Fatalf("run exited before serving: %v (config %s)", err, path)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if status != http.StatusOK {
		t.Fatalf("status=%d, want 200", status)
	}
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("run after interrupt: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not shut down after interrupt")
	}
}
