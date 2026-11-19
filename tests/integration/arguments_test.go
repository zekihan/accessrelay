//go:build integration

package integration

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRejectUnexpectedArguments(t *testing.T) {
	binary := os.Getenv("ACCESSRELAY_BINARY")
	if binary == "" {
		t.Skip("no application binary supplied")
	}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "after version flag", args: []string{"--version", "unexpected"}},
		{name: "before validate flag", args: []string{"--config", filepath.Join(t.TempDir(), "absent.json"), "unexpected", "--validate"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := exec.CommandContext(t.Context(), binary, tc.args...).CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 || string(output) != "configuration_arguments\n" {
				t.Fatalf("got output %q and error %v, want configuration_arguments and exit 1", output, err)
			}
		})
	}
}
