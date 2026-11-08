//go:build linux

package renderer

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const feedSupported = true

// readOffset reports the child's file offset for path from procfs. A path the
// child has not opened yet has offset zero. It returns the matching descriptor
// so later calls can check it first.
func readOffset(pid int, path string, cached int) (int, int64, bool) {
	base := "/proc/" + strconv.Itoa(pid)
	if cached >= 0 {
		if offset, ok := descriptorOffset(base, cached, path); ok {
			return cached, offset, true
		}
	}
	entries, err := os.ReadDir(base + "/fd")
	if err != nil {
		return -1, 0, false
	}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if offset, ok := descriptorOffset(base, fd, path); ok {
			return fd, offset, true
		}
	}
	return -1, 0, true
}

func descriptorOffset(base string, fd int, path string) (int64, bool) {
	name := strconv.Itoa(fd)
	target, err := os.Readlink(base + "/fd/" + name)
	if err != nil || target != path {
		return 0, false
	}
	f, err := os.Open(base + "/fdinfo/" + name)
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if value, found := strings.CutPrefix(scanner.Text(), "pos:"); found {
			offset, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			return offset, err == nil
		}
	}
	return 0, false
}

const throttleInterval = 10 * time.Millisecond

// startThrottle keeps the child suspended while it could reach the end of an
// incomplete replay. An unobservable offset keeps it suspended until Close.
func startThrottle(process *os.Process, exited <-chan struct{}, path string, feed *Feed) *throttle {
	t := &throttle{quit: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(t.done)
		suspended := false
		set := func(suspend bool) {
			if suspend == suspended {
				return
			}
			signal := syscall.SIGCONT
			if suspend {
				signal = syscall.SIGSTOP
			}
			if process.Signal(signal) == nil {
				suspended = suspend
			}
		}
		defer set(false)
		ticker := time.NewTicker(throttleInterval)
		defer ticker.Stop()
		fd := -1
		for {
			if _, ok := feed.closed(); ok {
				return
			}
			var offset int64
			var ok bool
			fd, offset, ok = readOffset(process.Pid, path, fd)
			set(!ok || offset+feed.Margin >= feed.Written())
			select {
			case <-t.quit:
				return
			case <-exited:
				return
			case <-ticker.C:
			}
		}
	}()
	return t
}
