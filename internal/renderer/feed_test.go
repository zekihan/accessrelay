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
	pause := f.Name() + ".pause"
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
		if lines == len(buffer)/len(feedLine) {
			// Let the test observe the throttle after the first read, even
			// when the child and throttle run at very different rates.
			for {
				if _, err := os.Stat(pause); os.IsNotExist(err) {
					break
				}
				select {
				case <-interrupted:
					return
				case <-time.After(time.Millisecond):
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	if os.WriteFile(output, []byte("<html>"+strconv.Itoa(lines)+"</html>"), 0o600) != nil {
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
	// #nosec G306 -- test executables need the execute bit.
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
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
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
	})
	return r, filepath.Join(dir, "access.log")
}

var feedLine = strings.Repeat("x", 63) + "\n"

func TestReadOffsetThroughSymlinkedRuntime(t *testing.T) {
	runtime := filepath.Join(t.TempDir(), "runtime")
	if err := os.Symlink(t.TempDir(), runtime); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(runtime, "access.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	n, err := f.WriteString(feedLine)
	if err != nil {
		t.Fatal(err)
	}
	fd := int(f.Fd())
	for _, cached := range []int{-1, fd} {
		gotFD, offset, ok := readOffset(os.Getpid(), path, cached)
		if !ok || gotFD != fd || offset != int64(n) {
			t.Fatalf(
				"cached fd %d: readOffset = (%d, %d, %t), want (%d, %d, true)",
				cached,
				gotFD,
				offset,
				ok,
				fd,
				n,
			)
		}
	}
}

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
	pause := path + ".pause"
	if err := os.WriteFile(pause, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	const margin = 64 << 10
	feed := NewFeed(margin)
	// Once the fake child reads 4 KiB, only 62 KiB remain. Its first
	// read must put it inside the margin and suspend it.
	initial := (margin + (2 << 10)) / len(feedLine)
	appendLines(t, f, feed, initial)
	if err := r.StartFeed(t.Context(), "feed", feed); err != nil {
		t.Fatal(err)
	}
	statusPath := filepath.Join("/proc", strconv.Itoa(r.cmd.Process.Pid), "status")
	stopped := func() bool {
		data, err := os.ReadFile(statusPath)
		if err != nil {
			t.Fatal(err)
		}
		return bytes.Contains(data, []byte("\nState:\tT "))
	}
	waitForFeed(t, stopped, "renderer did not suspend inside the margin")
	r.tick(t.Context())
	if r.Snapshot().Ready {
		t.Fatal("renderer became ready before the replay was complete")
	}
	extra := margin / len(feedLine)
	appendLines(t, f, feed, extra)
	waitForFeed(t, func() bool { return !stopped() }, "renderer did not resume after the write")
	feed.Close()
	if err := os.Remove(pause); err != nil {
		t.Fatal(err)
	}
	want := "<html>" + strconv.Itoa(initial+extra) + "</html>"
	waitForFeed(t, func() bool {
		data, _ := os.ReadFile(r.output)
		return string(data) == want
	}, "renderer did not snapshot the complete replay")
}

func waitForFeed(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStoppingIncompleteFeedPublishesNothing(t *testing.T) {
	r, path := newFeedRenderer(t)
	if err := os.WriteFile(path, []byte(strings.Repeat(feedLine, 16)), 0o600); err != nil {
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
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r.initTimeout = 50 * time.Millisecond
	feed := NewFeed(1 << 20)
	if err := r.StartFeed(t.Context(), "feed", feed); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	r.tick(t.Context())
	if !r.Snapshot().Running {
		t.Fatal("renderer timed out while its replay was still being written")
	}
}
