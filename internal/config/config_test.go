package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDatabaseIntegrityCheck(t *testing.T) {
	for _, tc := range []struct {
		name, setting, wantError string
		background               bool
	}{
		{name: "omitted"},
		{name: "startup", setting: `,"database":{"integrityCheck":"startup"}`},
		{name: "background", setting: `,"database":{"integrityCheck":"background"}`, background: true},
		{name: "unknown", setting: `,"database":{"integrityCheck":"never"}`, wantError: "configuration_database"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			raw := fmt.Sprintf(`{"collection":{"source":{"cluster":"fixture"}}%s}`, tc.setting)
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("got %v, want %s", err, tc.wantError)
				}
			} else if err != nil || c.Database.BackgroundIntegrity() != tc.background {
				t.Fatalf("background=%v err=%v, want %v", c.Database.BackgroundIntegrity(), err, tc.background)
			}
		})
	}
}

func TestLoadDatabaseSynchronous(t *testing.T) {
	for _, tc := range []struct{ name, setting, want, wantError string }{
		{name: "omitted"},
		{name: "full", setting: `,"database":{"synchronous":"full"}`, want: "full"},
		{name: "normal", setting: `,"database":{"synchronous":"normal"}`, want: "normal"},
		{name: "off", setting: `,"database":{"synchronous":"off"}`, wantError: "configuration_database"},
		{name: "uppercase", setting: `,"database":{"synchronous":"NORMAL"}`, wantError: "configuration_database"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			raw := fmt.Sprintf(`{"collection":{"source":{"cluster":"fixture"}}%s}`, tc.setting)
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("got %v, want %s", err, tc.wantError)
				}
			} else if err != nil || c.Database.Synchronous != tc.want {
				t.Fatalf("synchronous=%q err=%v, want %q", c.Database.Synchronous, err, tc.want)
			}
		})
	}
}

func TestLoadDatabaseMapping(t *testing.T) {
	for _, tc := range []struct {
		name, setting, wantError string
		wantBytes                int64
	}{
		{name: "omitted"},
		{name: "disabled", setting: `,"database":{"mmapBytes":0}`},
		{name: "enabled", setting: `,"database":{"mmapBytes":67108864}`, wantBytes: 64 << 20},
		{name: "maximum", setting: `,"database":{"mmapBytes":2147483648}`, wantBytes: 2 << 30},
		{name: "negative", setting: `,"database":{"mmapBytes":-1}`, wantError: "configuration_database"},
		{name: "too large", setting: `,"database":{"mmapBytes":2147483649}`, wantError: "configuration_database"},
		{name: "unknown option", setting: `,"database":{"cacheBytes":67108864}`, wantError: "configuration_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			raw := fmt.Sprintf(`{"collection":{"source":{"cluster":"fixture"}}%s}`, tc.setting)
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("got %v, want %s", err, tc.wantError)
				}
			} else if err != nil || c.Database.MmapBytes != tc.wantBytes {
				t.Fatalf("mmapBytes=%d err=%v, want %d", c.Database.MmapBytes, err, tc.wantBytes)
			}
		})
	}
}

func TestLoadReplayStartup(t *testing.T) {
	for _, tc := range []struct {
		name, setting, wantError string
		readers                  int
		stream                   bool
	}{
		{name: "omitted"},
		{name: "prefetch", setting: `,"database":{"replayPrefetch":16}`, readers: 16},
		{name: "prefetch maximum", setting: `,"database":{"replayPrefetch":64}`, readers: 64},
		{name: "prefetch negative", setting: `,"database":{"replayPrefetch":-1}`, wantError: "configuration_database"},
		{name: "prefetch too large", setting: `,"database":{"replayPrefetch":65}`, wantError: "configuration_database"},
		{name: "stream", setting: `,"report":{"historyDays":7,"jobs":6,"chunkSize":8192,"websocketURL":"ws://localhost:8080/ws","streamReplay":true}`, stream: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			raw := fmt.Sprintf(`{"collection":{"source":{"cluster":"fixture"}}%s}`, tc.setting)
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("got %v, want %s", err, tc.wantError)
				}
			} else if err != nil || c.Database.ReplayPrefetch != tc.readers || c.Report.StreamReplay != tc.stream {
				t.Fatalf("readers=%d stream=%v err=%v", c.Database.ReplayPrefetch, c.Report.StreamReplay, err)
			}
		})
	}
}

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

func TestLoadConfigurationSizeBoundary(t *testing.T) {
	const limit = 1 << 20
	const document = `{"collection":{"source":{"cluster":"fixture"}}}`
	for _, tc := range []struct {
		name, raw string
		wantError bool
	}{
		{"at limit", document + strings.Repeat(" ", limit-len(document)), false},
		{"one byte over limit", document + strings.Repeat(" ", limit+1-len(document)), true},
		{"hidden trailing document", document + strings.Repeat(" ", limit+1-len(document)) + `{}`, true},
		{"ordinary trailing document", document + `{}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(tc.raw), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if (err != nil) != tc.wantError {
				t.Fatalf("Load (%d bytes): error = %v, want error = %v", len(tc.raw), err, tc.wantError)
			}
		})
	}
}

func TestLoadExcludeRouters(t *testing.T) {
	long := strings.Repeat("r", 257)
	many := strings.TrimSuffix(strings.Repeat(`"r",`, 65), ",")
	for _, tc := range []struct {
		name, routers, wantError string
		want                     int
	}{
		{name: "omitted"},
		{name: "one", routers: `["traefik-mtls-journald-upload-fcc44d81276fc53b472d@kubernetescrd"]`, want: 1},
		{name: "empty name", routers: `[""]`, wantError: "configuration_exclusion"},
		{name: "space", routers: `["a b"]`, wantError: "configuration_exclusion"},
		{name: "quote", routers: `["a\"b"]`, wantError: "configuration_exclusion"},
		{name: "duplicate", routers: `["a","a"]`, wantError: "configuration_exclusion"},
		{name: "too long", routers: `["` + long + `"]`, wantError: "configuration_exclusion"},
		{name: "too many", routers: `[` + many + `]`, wantError: "configuration_exclusion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setting := ""
			if tc.routers != "" {
				setting = `,"excludeRouters":` + tc.routers
			}
			path := filepath.Join(t.TempDir(), "config.json")
			raw := fmt.Sprintf(`{"collection":{"source":{"cluster":"fixture"}%s}}`, setting)
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("got %v, want %s", err, tc.wantError)
				}
			} else if err != nil || len(c.Collection.ExcludeRouters) != tc.want {
				t.Fatalf("routers=%v err=%v", c.Collection.ExcludeRouters, err)
			}
		})
	}
}
