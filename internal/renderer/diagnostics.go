package renderer

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// diagnosticCapture drains one GoAccess diagnostic FIFO. GoAccess's argument
// path remains the stable path under state/logs; the bounded, readable capture
// is stored beside it with a .snapshot suffix.
type diagnosticCapture struct {
	pipe       int
	snapshot   *os.File
	limit      int64
	buffer     []byte
	total      int64
	done       chan struct{}
	stop       chan struct{}
	stopOnce   sync.Once
	writeError atomic.Bool
}

func startDiagnosticCapture(path string, limit int64) (*diagnosticCapture, error) {
	pipe, err := openDiagnosticFIFO(path)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		_ = closeDiagnosticFIFO(pipe)
		return nil, errors.New("diagnostic_limit")
	}
	snapshotPath := path + ".snapshot"
	buffer := make([]byte, int(limit))
	var total int64
	if old, readErr := os.Open(snapshotPath); readErr == nil {
		oldData, copyErr := io.ReadAll(io.LimitReader(old, limit))
		closeErr := old.Close()
		if copyErr != nil || closeErr != nil {
			_ = closeDiagnosticFIFO(pipe)
			return nil, errors.New("diagnostic_snapshot")
		}
		copy(buffer, oldData)
		total = int64(len(oldData))
	} else if !os.IsNotExist(readErr) {
		_ = closeDiagnosticFIFO(pipe)
		return nil, readErr
	}
	snapshot, err := os.OpenFile(snapshotPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		_ = closeDiagnosticFIFO(pipe)
		return nil, err
	}
	// Carry forward a bounded previous capture; new diagnostics roll it out as
	// needed, so the snapshot always contains the newest bytes.
	c := &diagnosticCapture{pipe: pipe, snapshot: snapshot, limit: limit, buffer: buffer, total: total, done: make(chan struct{}), stop: make(chan struct{})}
	c.flush()
	go c.drain()
	return c, nil
}

func (c *diagnosticCapture) append(data []byte) {
	capBytes := int64(len(c.buffer))
	if int64(len(data)) >= capBytes {
		c.total += int64(len(data))
		start := int(c.total % capBytes)
		tail := data[len(data)-len(c.buffer):]
		first := len(c.buffer) - start
		copy(c.buffer[start:], tail[:first])
		copy(c.buffer[:start], tail[first:])
		return
	}
	index := int(c.total % capBytes)
	first := len(data)
	if first > len(c.buffer)-index {
		first = len(c.buffer) - index
	}
	copy(c.buffer[index:index+first], data[:first])
	copy(c.buffer[:len(data)-first], data[first:])
	c.total += int64(len(data))
}

func (c *diagnosticCapture) flush() {
	if _, err := c.snapshot.Seek(0, 0); err != nil {
		c.writeError.Store(true)
		return
	}
	if err := c.snapshot.Truncate(0); err != nil {
		c.writeError.Store(true)
		return
	}
	kept := c.total
	if kept > c.limit {
		kept = c.limit
	}
	start := int64(0)
	if c.total > c.limit {
		start = c.total % c.limit
	}
	first := kept
	if first > c.limit-start {
		first = c.limit - start
	}
	if _, err := c.snapshot.Write(c.buffer[int(start):int(start+first)]); err != nil {
		c.writeError.Store(true)
		return
	}
	if first < kept {
		if _, err := c.snapshot.Write(c.buffer[:int(kept-first)]); err != nil {
			c.writeError.Store(true)
			return
		}
	}
}

func (c *diagnosticCapture) drain() {
	defer close(c.done)
	buf := make([]byte, 32*1024)
	dirty := false
	lastFlush := time.Now()
	for {
		n, err := readDiagnosticFIFO(c.pipe, buf)
		if n > 0 {
			c.append(buf[:n])
			dirty = true
			if time.Since(lastFlush) >= 250*time.Millisecond {
				c.flush()
				dirty = false
				lastFlush = time.Now()
			}
		}
		if err != nil {
			if isWouldBlock(err) {
				if dirty && time.Since(lastFlush) >= 250*time.Millisecond {
					c.flush()
					dirty = false
					lastFlush = time.Now()
				}
				select {
				case <-c.stop:
					goto done
				default:
				}
				time.Sleep(20 * time.Millisecond)
				continue
			}
			c.writeError.Store(true)
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if n == 0 {
			select {
			case <-c.stop:
				goto done
			default:
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
done:
	if dirty {
		c.flush()
	}
	if c.snapshot.Sync() != nil {
		c.writeError.Store(true)
	}
	if c.snapshot.Close() != nil {
		c.writeError.Store(true)
	}
}

// close asks the nonblocking reader to drain until EAGAIN, which is safe after
// the child has been reaped, then waits for its bounded snapshot to flush.
func (c *diagnosticCapture) close() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() { close(c.stop) })
	<-c.done
	_ = closeDiagnosticFIFO(c.pipe)
}
