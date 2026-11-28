// Package config validates the portable application configuration.
package config

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Source struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Container string `json:"container"`
}

const (
	TraefikCLF    = "traefik-clf"
	GatehouseJSON = "gatehouse-json"
)

type Input struct {
	Source
	Format string `json:"format"`
}

type Collection struct {
	Source Source `json:"source"`
	// Sources replaces Source when configured.
	Sources                []Input `json:"sources"`
	PollInterval           string  `json:"pollInterval"`
	IngestionDelay         string  `json:"ingestionDelay"`
	Overlap                string  `json:"overlap"`
	ReconciliationInterval string  `json:"reconciliationInterval"`
	BackfillWindow         string  `json:"backfillWindow"`
	MinimumWindow          string  `json:"minimumWindow"`
	QueryTimeout           string  `json:"queryTimeout"`
	StaleAfter             string  `json:"staleAfter"`
	MaxLineBytes           int     `json:"maxLineBytes"`
	MaxWindowBytes         int64   `json:"maxWindowBytes"`
	// ExcludeRouters lists Traefik router names whose requests are neither
	// stored nor reported. Already stored events are purged once.
	ExcludeRouters []string `json:"excludeRouters"`
	ExcludeRoutes  []string `json:"excludeRoutes"`
}

func (c Collection) Inputs() []Input {
	if c.Sources != nil {
		return c.Sources
	}
	return []Input{{Source: c.Source, Format: TraefikCLF}}
}

func (s Source) Valid() bool {
	for _, part := range []string{s.Cluster, s.Namespace, s.Container} {
		if part == "" || strings.ContainsAny(part, "\r\n\x00") {
			return false
		}
	}
	return true
}

type Report struct {
	HistoryDays     int               `json:"historyDays"`
	Jobs            int               `json:"jobs"`
	ChunkSize       int               `json:"chunkSize"`
	WebsocketURL    string            `json:"websocketURL"`
	ConfigOverrides map[string]string `json:"configOverrides"`
	ExtraBrowsers   []string          `json:"extraBrowsers"`
	// StreamReplay starts GoAccess while a full replay is still being written,
	// suspending it whenever it gets close to the written length.
	StreamReplay bool `json:"streamReplay"`
}
type Limits struct {
	MaxRuntimeBytes  int64  `json:"maxRuntimeBytes"`
	MaxDatabaseBytes int64  `json:"maxDatabaseBytes"`
	MaxReplayBytes   int64  `json:"maxReplayBytes"`
	MaxReportBytes   int64  `json:"maxReportBytes"`
	DiagnosticBytes  int64  `json:"diagnosticBytes"`
	ReserveBytes     uint64 `json:"reserveBytes"`
}
type Database struct {
	MmapBytes int64 `json:"mmapBytes"`
	// IntegrityCheck is "startup" (default) or "background".
	IntegrityCheck string `json:"integrityCheck"`
	// ReplayPrefetch is the number of concurrent readers that warm the page
	// cache ahead of a full replay export; zero disables it.
	ReplayPrefetch int `json:"replayPrefetch"`
	// Synchronous is "full" (default) or "normal".
	Synchronous string `json:"synchronous"`
}

func (d Database) BackgroundIntegrity() bool { return d.IntegrityCheck == "background" }

type Config struct {
	Listen         string     `json:"listen"`
	MetricsAddr    string     `json:"metricsAddr"`
	StateDir       string     `json:"stateDir"`
	RuntimeDir     string     `json:"runtimeDir"`
	ConnectionFile string     `json:"connectionFile"`
	GoAccessBinary string     `json:"goaccessBinary"`
	Database       Database   `json:"database"`
	Collection     Collection `json:"collection"`
	Report         Report     `json:"report"`
	Limits         Limits     `json:"limits"`
}

func Default() Config {
	return Config{
		Listen:         ":8080",
		MetricsAddr:    ":9090",
		StateDir:       "/state",
		RuntimeDir:     "/runtime",
		ConnectionFile: "/connection/connection.json",
		GoAccessBinary: "goaccess",
		Collection: Collection{
			Source:                 Source{Namespace: "traefik", Container: "traefik"},
			PollInterval:           "5s",
			IngestionDelay:         "30s",
			Overlap:                "5m",
			ReconciliationInterval: "1h",
			BackfillWindow:         "5m",
			MinimumWindow:          "1s",
			QueryTimeout:           "20s",
			StaleAfter:             "2m",
			MaxLineBytes:           1048576,
			MaxWindowBytes:         268435456,
		},
		Report: Report{
			HistoryDays:     7,
			Jobs:            6,
			ChunkSize:       8192,
			WebsocketURL:    "ws://localhost:8080/ws",
			ConfigOverrides: map[string]string{},
		},
		Limits: Limits{
			MaxRuntimeBytes:  2684354560,
			MaxDatabaseBytes: 1610612736,
			MaxReplayBytes:   1073741824,
			MaxReportBytes:   67108864,
			DiagnosticBytes:  8388608,
			ReserveBytes:     33554432,
		},
	}
}

var positiveDuration = regexp.MustCompile(`^[1-9][0-9]*(s|m|h)$`)

func Duration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

func Load(path string) (Config, error) {
	c := Default()
	f, err := os.Open(path)
	if err != nil {
		return c, errors.New("configuration_unreadable")
	}
	defer func() { _ = f.Close() }()
	// Read one byte beyond the limit so an artificial EOF cannot hide
	// trailing data or make an oversized configuration appear complete.
	const maxConfigBytes = 1 << 20
	limited := &io.LimitedReader{R: f, N: maxConfigBytes + 1}
	dec := json.NewDecoder(limited)
	dec.DisallowUnknownFields()
	if dec.Decode(&c) != nil {
		return c, errors.New("configuration_invalid")
	}
	var tail any
	tailErr := dec.Decode(&tail)
	if limited.N == 0 {
		return c, errors.New("configuration_invalid")
	}
	if tailErr != io.EOF {
		return c, errors.New("configuration_trailing_data")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.MetricsAddr != "" {
		_, port, err := net.SplitHostPort(c.MetricsAddr)
		if err != nil {
			return errors.New("configuration_metrics_address")
		}
		if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
			return errors.New("configuration_metrics_address")
		}
	}
	if c.Database.MmapBytes < 0 || c.Database.MmapBytes > 2<<30 || c.Database.ReplayPrefetch < 0 ||
		c.Database.ReplayPrefetch > 64 {
		return errors.New("configuration_database")
	}
	switch c.Database.IntegrityCheck {
	case "", "startup", "background":
	default:
		return errors.New("configuration_database")
	}
	switch c.Database.Synchronous {
	case "", "full", "normal":
	default:
		return errors.New("configuration_database")
	}
	for _, s := range []string{c.Collection.PollInterval, c.Collection.IngestionDelay, c.Collection.Overlap, c.Collection.ReconciliationInterval, c.Collection.BackfillWindow, c.Collection.MinimumWindow, c.Collection.QueryTimeout, c.Collection.StaleAfter} {
		d, err := time.ParseDuration(s)
		if !positiveDuration.MatchString(s) || err != nil || d > 24*time.Hour {
			return errors.New("configuration_duration")
		}
	}
	if Duration(c.Collection.MinimumWindow) > Duration(c.Collection.BackfillWindow) ||
		c.Collection.MaxLineBytes < 1024 ||
		c.Collection.MaxLineBytes > 16777216 ||
		c.Collection.MaxWindowBytes < int64(c.Collection.MaxLineBytes) ||
		c.Collection.MaxWindowBytes > 1073741824 {
		return errors.New("configuration_window")
	}
	if c.Report.HistoryDays < 1 || c.Report.HistoryDays > 366 || c.Report.Jobs < 1 ||
		c.Report.Jobs > 12 ||
		c.Report.ChunkSize < 32 ||
		c.Report.ChunkSize > 1048576 {
		return errors.New("configuration_report")
	}
	if err := c.Collection.validateSources(); err != nil {
		return err
	}
	for _, s := range []string{c.StateDir, c.RuntimeDir, c.ConnectionFile} {
		if !filepath.IsAbs(s) || strings.ContainsAny(s, "\r\n\x00") {
			return errors.New("configuration_path")
		}
	}
	state, runtime := filepath.Clean(c.StateDir), filepath.Clean(c.RuntimeDir)
	// A root directory contains every absolute path; appending another slash
	// would otherwise turn its containment prefix into "//".
	if state == string(filepath.Separator) || runtime == string(filepath.Separator) ||
		state == runtime ||
		strings.HasPrefix(runtime, state+"/") ||
		strings.HasPrefix(state, runtime+"/") {
		return errors.New("configuration_path_overlap")
	}
	if !validWebsocketURL(c.Report.WebsocketURL) {
		return errors.New("configuration_websocket")
	}
	allowed := map[string]bool{
		"agent-list":           true,
		"http-method":          true,
		"http-protocol":        true,
		"anonymize-ip":         true,
		"ignore-crawlers":      true,
		"unknowns-as-crawlers": true,
		"real-os":              true,
		"with-output-resolver": true,
		"no-query-string":      true,
		"date-spec":            true,
		"hour-spec":            true,
		"all-static-files":     true,
		"4xx-to-unique-count":  true,
		"double-decode":        true,
		"crawlers-only":        true,
	}
	for k, v := range c.Report.ConfigOverrides {
		if !allowed[k] || strings.ContainsAny(v, "\r\n\x00 \t") {
			return errors.New("configuration_override")
		}
		switch k {
		case "date-spec":
			if v != "date" && v != "hr" {
				return errors.New("configuration_override")
			}
		case "hour-spec":
			if v != "hr" && v != "min" {
				return errors.New("configuration_override")
			}
		case "http-method", "http-protocol":
			// GoAccess parses these as yes/no settings. Older chart values also
			// used true/false, so the renderer normalizes those spellings.
			if v != "true" && v != "false" && v != "yes" && v != "no" {
				return errors.New("configuration_override")
			}
		default:
			// Keep accepting yes/no for existing values; the renderer converts
			// them to GoAccess's true/false spelling for flag-style settings.
			if v != "true" && v != "false" && v != "yes" && v != "no" {
				return errors.New("configuration_override")
			}
		}
	}
	for _, line := range c.Report.ExtraBrowsers {
		if strings.Count(line, "\t") != 1 || strings.ContainsAny(line, "\r\n\x00") ||
			strings.HasPrefix(line, "#") {
			return errors.New("configuration_browser")
		}
	}
	if c.Limits.MaxRuntimeBytes < 1048576 || c.Limits.MaxDatabaseBytes < 1048576 ||
		c.Limits.MaxReplayBytes < 1048576 ||
		c.Limits.MaxReportBytes < 1048576 ||
		c.Limits.DiagnosticBytes < 128 ||
		c.Limits.DiagnosticBytes > 67108864 ||
		c.Limits.ReserveBytes < 1048576 {
		return errors.New("configuration_limits")
	}
	return nil
}

func (c Collection) validateSources() error {
	inputs := c.Inputs()
	if len(inputs) == 0 || len(inputs) > 16 {
		return errors.New("configuration_source")
	}
	seen := map[Source]bool{}
	for _, input := range inputs {
		if !input.Valid() || seen[input.Source] ||
			(input.Format != TraefikCLF && input.Format != GatehouseJSON) {
			return errors.New("configuration_source")
		}
		seen[input.Source] = true
	}
	for _, exclusions := range [][]string{c.ExcludeRouters, c.ExcludeRoutes} {
		if len(exclusions) > 64 {
			return errors.New("configuration_exclusion")
		}
		excluded := map[string]bool{}
		for _, name := range exclusions {
			if name == "" || len(name) > 256 || excluded[name] ||
				strings.ContainsAny(name, "\" \t\r\n\x00\\") {
				return errors.New("configuration_exclusion")
			}
			excluded[name] = true
		}
	}
	return nil
}

func validWebsocketURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil ||
		u.Fragment != "" || u.RawQuery != "" || strings.ContainsAny(raw, "\r\n\t ") {
		return false
	}
	if u.Hostname() == "" || strings.HasSuffix(u.Host, ":") {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		return err == nil && n != 0
	}
	return true
}
