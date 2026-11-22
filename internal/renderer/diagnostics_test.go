package renderer

import (
	"os"
	"testing"
	"time"
)

func TestDiagnosticCaptureCloseOnPersistentReadError(t *testing.T) {
	snapshot, err := os.CreateTemp(t.TempDir(), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	// A descriptor that fails every read (EBADF) must not deadlock close:
	// drain only consulted the stop channel on would-block reads.
	capture := &diagnosticCapture{pipe: -1, snapshot: snapshot, limit: 16, buffer: make([]byte, 16), done: make(chan struct{}), stop: make(chan struct{})}
	go capture.drain()
	closed := make(chan struct{})
	go func() {
		capture.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close hung on a persistent diagnostic read error")
	}
	if !capture.writeError.Load() {
		t.Fatal("persistent read error was not recorded")
	}
}

func TestDiagnosticFlushFailureIsRecorded(t *testing.T) {
	snapshot, err := os.CreateTemp(t.TempDir(), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	capture := &diagnosticCapture{snapshot: snapshot, limit: 4, buffer: make([]byte, 4)}
	capture.append([]byte("data"))
	capture.flush()
	if !capture.writeError.Load() {
		t.Fatal("flush to a closed snapshot did not record a write error")
	}
}
