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
