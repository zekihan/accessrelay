package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidation(t *testing.T) {
	c := Default()
	c.Collection.Source.Cluster = "fixture"
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.Report.ConfigOverrides["restore"] = "true" }, func(c *Config) { c.Report.ConfigOverrides["http-method"] = "yes\nrestore true" }, func(c *Config) { c.Report.HistoryDays = 0 }, func(c *Config) { c.Collection.PollInterval = "0s" }, func(c *Config) { c.RuntimeDir = c.StateDir }, func(c *Config) { c.Report.ExtraBrowsers = []string{"bad\nBrowser\tCrawler"} }, func(c *Config) { c.Report.WebsocketURL = "wss://user:password@example.invalid/ws" }} {
		c = Default()
		c.Collection.Source.Cluster = "fixture"
		mutate(&c)
		if c.Validate() == nil {
			t.Fatal("unsafe config accepted")
		}
	}
	path := filepath.Join(t.TempDir(), "config.json")
	for _, raw := range []string{`{"unknown":"value"}`, `{} {}`, `{"collection":{"pollInterval":1}}`} {
		_ = os.WriteFile(path, []byte(raw), 0600)
		if _, e := Load(path); e == nil {
			t.Fatal(raw)
		}
	}
}

func TestGoAccessBooleanOverrideSpellingsRemainAccepted(t *testing.T) {
	for key, value := range map[string]string{
		"http-method": "true", "http-protocol": "no",
		"anonymize-ip": "yes", "ignore-crawlers": "false",
	} {
		c := Default()
		c.Collection.Source.Cluster = "fixture"
		c.Report.ConfigOverrides[key] = value
		if err := c.Validate(); err != nil {
			t.Errorf("%s=%s rejected: %v", key, value, err)
		}
	}
}

func TestStateRuntimeDirectorySeparation(t *testing.T) {
	for _, tc := range []struct {
		name, state, runtime string
		overlap              bool
	}{
		{"same", "/state", "/state", true},
		{"runtime below state", "/state", "/state/runtime", true},
		{"state below runtime", "/runtime/state", "/runtime", true},
		{"root state", "/", "/runtime", true},
		{"root runtime", "/state", "/", true},
		{"cleaned root state", "/unused/..", "/runtime", true},
		{"cleaned root runtime", "/state", "/unused/..", true},
		{"siblings", "/state", "/runtime", false},
		{"shared prefix", "/state", "/state-runtime", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			c.Collection.Source.Cluster = "fixture"
			c.StateDir, c.RuntimeDir = tc.state, tc.runtime
			err := c.Validate()
			if tc.overlap {
				if err == nil || err.Error() != "configuration_path_overlap" {
					t.Fatalf("overlapping state %q and runtime %q: got %v, want configuration_path_overlap", tc.state, tc.runtime, err)
				}
			} else if err != nil {
				t.Fatalf("separate directories rejected: %v", err)
			}
		})
	}
}
