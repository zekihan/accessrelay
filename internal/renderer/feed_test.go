//go:build linux

package renderer

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
)

// TestMain doubles as a fake GoAccess that, like the real one, ends its initial
// parse at the first end of file and writes that snapshot once.
func TestMain(m *testing.M) {
	if os.Getenv("ACCESSRELAY_FAKE_GOACCESS") == "1" {
		fakeGoAccess(os.Args[1:])
		return
	}
	os.Exit(m.Run())
}

func fakeGoAccess(args []string) {
	var output string
	for _, arg := range args {
		if value, ok := strings.CutPrefix(arg, "--output="); ok {
			output = value
		}
	}
	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt, syscall.SIGTERM)
	f, err := os.Open(args[len(args)-1])
	if err != nil {
		os.Exit(3)
	}
	buffer := make([]byte, 4096)
	lines := 0
	for {
		n, err := f.Read(buffer)
		lines += bytes.Count(buffer[:n], []byte("\n"))
		if err == io.EOF {
			break
		}
		if err != nil {
			os.Exit(4)
		}
		time.Sleep(time.Millisecond)
	}
	if os.WriteFile(output, []byte("<html>"+strconv.Itoa(lines)+"</html>"), 0600) != nil {
		os.Exit(5)
	}
	<-interrupted
}

func newFeedRenderer(t *testing.T) (*Renderer, string) {
	t.Helper()
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "goaccess")
	script := "#!/bin/sh\nACCESSRELAY_FAKE_GOACCESS=1 exec " + executable + " \"$@\"\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.GoAccessBinary = binary
	r, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(c.RuntimeDir, "generations", "feed")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
	})
	return r, filepath.Join(dir, "access.log")
}

var feedLine = strings.Repeat("x", 63) + "\n"

func appendLines(t *testing.T, f *os.File, feed *Feed, lines int) {
	t.Helper()
	n, err := f.WriteString(strings.Repeat(feedLine, lines))
	if err != nil {
		t.Fatal(err)
	}
	feed.Advance(int64(n))
}

func TestFeedKeepsSlowWriterAheadOfRenderer(t *testing.T) {
	r, path := newFeedRenderer(t)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	feed := NewFeed(64 << 10)
	appendLines(t, f, feed, 2048)
	if err := r.StartFeed(t.Context(), "feed", feed); err != nil {
		t.Fatal(err)
	}
	// Write far slower than the reader consumes; unthrottled, it would reach
	// end of file and snapshot a partial replay.
	total := 2048
	for range 20 {
		time.Sleep(25 * time.Millisecond)
		appendLines(t, f, feed, 256)
		total += 256
	}
	r.tick()
	if r.Snapshot().Ready {
		t.Fatal("renderer became ready before the replay was complete")
	}
	feed.Close()
	want := "<html>" + strconv.Itoa(total) + "</html>"
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, _ := os.ReadFile(r.output)
		if string(data) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot=%q, want %q", data, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStoppingIncompleteFeedPublishesNothing(t *testing.T) {
	r, path := newFeedRenderer(t)
	if err := os.WriteFile(path, []byte(strings.Repeat(feedLine, 16)), 0600); err != nil {
		t.Fatal(err)
	}
	feed := NewFeed(1 << 20)
	feed.Advance(int64(16 * len(feedLine)))
	if err := r.StartFeed(t.Context(), "feed", feed); err != nil {
		t.Fatal(err)
	}
	// The child stays suspended within the margin; stopping must resume it so
	// it can exit, and its snapshot of a partial replay is not published.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := time.Now()
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatal("suspended child was not resumed for shutdown")
	}
	if _, err := os.Stat(r.ReportPath()); !os.IsNotExist(err) {
		t.Fatalf("partial replay report published: %v", err)
	}
}

func TestStreamedInitializationDeadlineStartsAtClose(t *testing.T) {
	r, path := newFeedRenderer(t)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	r.initTimeout = 50 * time.Millisecond
	feed := NewFeed(1 << 20)
	if err := r.StartFeed(t.Context(), "feed", feed); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	r.tick()
	if !r.Snapshot().Running {
		t.Fatal("renderer timed out while its replay was still being written")
	}
}
