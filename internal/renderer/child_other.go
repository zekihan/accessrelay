//go:build !linux

package renderer

import "os/exec"

func configureChild(_ *exec.Cmd) {}
