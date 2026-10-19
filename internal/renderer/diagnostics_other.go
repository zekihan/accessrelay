//go:build !linux && !darwin

package renderer

import (
	"errors"
)

func ensureDiagnosticFIFO(string) error           { return errors.New("diagnostic_unavailable") }
func openDiagnosticFIFO(string) (int, error)      { return -1, errors.New("diagnostic_unavailable") }
func isWouldBlock(error) bool                     { return false }
func readDiagnosticFIFO(int, []byte) (int, error) { return 0, errors.New("diagnostic_unavailable") }
func closeDiagnosticFIFO(int) error               { return nil }
