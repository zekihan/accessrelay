package assets

import (
	"strings"
	"testing"
)

func TestBrowsersListIsWellFormed(t *testing.T) {
	if Browsers == "" || !strings.HasSuffix(Browsers, "\n") {
		t.Fatal("browsers list is empty or lacks a trailing newline")
	}
	seen := map[string]bool{}
	for i, line := range strings.Split(strings.TrimSuffix(Browsers, "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Count(line, "\t") != 1 {
			t.Fatalf("line %d has %d tabs, want exactly one: %q", i+1, strings.Count(line, "\t"), line)
		}
		pattern, category, _ := strings.Cut(line, "\t")
		if pattern == "" || category == "" {
			t.Fatalf("line %d has an empty pattern or category: %q", i+1, line)
		}
		if seen[pattern] {
			t.Fatalf("line %d duplicates browser pattern %q", i+1, pattern)
		}
		seen[pattern] = true
	}
	if len(seen) == 0 {
		t.Fatal("browsers list has no classifications")
	}
}

func TestGoAccessConfigKeepsRuntimeContract(t *testing.T) {
	for _, want := range []string{
		"log-format TRAEFIKCLF", "tz UTC", "addr 127.0.0.1", "port 7890",
		"persist true", "restore false", "ws-url {{WS}}", "browsers-file {{BROWSERS}}", "keep-last {{DAYS}}",
	} {
		if !strings.Contains(Config, want+"\n") {
			t.Errorf("embedded config missing %q", want)
		}
	}
	for _, placeholder := range []string{"{{WS}}", "{{DAYS}}", "{{BROWSERS}}"} {
		if n := strings.Count(Config, placeholder); n != 1 {
			t.Errorf("placeholder %s occurs %d times, want 1", placeholder, n)
		}
	}
}
