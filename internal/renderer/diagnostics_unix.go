//go:build linux || darwin

package renderer

import (
	"errors"
	"os"
	"syscall"
)

func ensureDiagnosticFIFO(path string) error {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeNamedPipe != 0 {
			return nil
		}
		if !info.Mode().IsRegular() {
			return errors.New("diagnostic_path")
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return syscall.Mkfifo(path, 0600)
}

func openDiagnosticFIFO(path string) (int, error) {
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	return fd, nil
}

func isWouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}

func readDiagnosticFIFO(fd int, buffer []byte) (int, error) { return syscall.Read(fd, buffer) }
func closeDiagnosticFIFO(fd int) error                      { return syscall.Close(fd) }
