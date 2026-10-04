// Package config validates the portable application configuration.
package config

import (
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type Source struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Container string `json:"container"`
}
type Collection struct {
	Source                 Source `json:"source"`
	PollInterval           string `json:"pollInterval"`
	IngestionDelay         string `json:"ingestionDelay"`
	Overlap                string `json:"overlap"`
	ReconciliationInterval string `json:"reconciliationInterval"`
	BackfillWindow         string `json:"backfillWindow"`
	MinimumWindow          string `json:"minimumWindow"`
	QueryTimeout           string `json:"queryTimeout"`
	StaleAfter             string `json:"staleAfter"`
	MaxLineBytes           int    `json:"maxLineBytes"`
	MaxWindowBytes         int64  `json:"maxWindowBytes"`
}
type Report struct {
	HistoryDays     int               `json:"historyDays"`
	Jobs            int               `json:"jobs"`
	ChunkSize       int               `json:"chunkSize"`
	WebsocketURL    string            `json:"websocketURL"`
	ConfigOverrides map[string]string `json:"configOverrides"`
	ExtraBrowsers   []string          `json:"extraBrowsers"`
}
type Limits struct {
	MaxRuntimeBytes  int64  `json:"maxRuntimeBytes"`
	MaxDatabaseBytes int64  `json:"maxDatabaseBytes"`
	MaxReplayBytes   int64  `json:"maxReplayBytes"`
	MaxReportBytes   int64  `json:"maxReportBytes"`
	DiagnosticBytes  int64  `json:"diagnosticBytes"`
	ReserveBytes     uint64 `json:"reserveBytes"`
}
type Config struct {
	Listen         string     `json:"listen"`
	StateDir       string     `json:"stateDir"`
	RuntimeDir     string     `json:"runtimeDir"`
	ConnectionFile string     `json:"connectionFile"`
	GoAccessBinary string     `json:"goaccessBinary"`
	Collection     Collection `json:"collection"`
	Report         Report     `json:"report"`
	Limits         Limits     `json:"limits"`
}

func Default() Config {
	return Config{Listen: ":8080", StateDir: "/state", RuntimeDir: "/runtime", ConnectionFile: "/connection/connection.json", GoAccessBinary: "goaccess",
		Collection: Collection{Source: Source{Namespace: "traefik", Container: "traefik"}, PollInterval: "5s", IngestionDelay: "30s", Overlap: "5m", ReconciliationInterval: "1h", BackfillWindow: "5m", MinimumWindow: "1s", QueryTimeout: "20s", StaleAfter: "2m", MaxLineBytes: 1048576, MaxWindowBytes: 268435456},
		Report:     Report{HistoryDays: 7, Jobs: 6, ChunkSize: 8192, WebsocketURL: "ws://localhost:8080/ws", ConfigOverrides: map[string]string{}},
		Limits:     Limits{MaxRuntimeBytes: 2684354560, MaxDatabaseBytes: 1610612736, MaxReplayBytes: 1073741824, MaxReportBytes: 67108864, DiagnosticBytes: 8388608, ReserveBytes: 33554432}}
}

var positiveDuration = regexp.MustCompile(`^[1-9][0-9]*(s|m|h)$`)

func Duration(s string) time.Duration { d, _ := time.ParseDuration(s); return d }
func Load(path string) (Config, error) {
	c := Default()
	f, err := os.Open(path)
	if err != nil {
		return c, errors.New("configuration_unreadable")
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(io.LimitReader(f, 1048577))
	dec.DisallowUnknownFields()
	if dec.Decode(&c) != nil {
		return c, errors.New("configuration_invalid")
	}
	var tail any
	if dec.Decode(&tail) != io.EOF {
		return c, errors.New("configuration_trailing_data")
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	for _, s := range []string{c.Collection.PollInterval, c.Collection.IngestionDelay, c.Collection.Overlap, c.Collection.ReconciliationInterval, c.Collection.BackfillWindow, c.Collection.MinimumWindow, c.Collection.QueryTimeout, c.Collection.StaleAfter} {
		d, err := time.ParseDuration(s)
		if !positiveDuration.MatchString(s) || err != nil || d > 24*time.Hour {
			return errors.New("configuration_duration")
		}
	}
	if Duration(c.Collection.MinimumWindow) > Duration(c.Collection.BackfillWindow) || c.Collection.MaxLineBytes < 1024 || c.Collection.MaxLineBytes > 16777216 || c.Collection.MaxWindowBytes < int64(c.Collection.MaxLineBytes) || c.Collection.MaxWindowBytes > 1073741824 {
		return errors.New("configuration_window")
	}
	if c.Report.HistoryDays < 1 || c.Report.HistoryDays > 366 || c.Report.Jobs < 1 || c.Report.Jobs > 64 || c.Report.ChunkSize < 1 || c.Report.ChunkSize > 1048576 {
		return errors.New("configuration_report")
	}
	for _, s := range []string{c.Collection.Source.Cluster, c.Collection.Source.Namespace, c.Collection.Source.Container} {
		if s == "" || strings.ContainsAny(s, "\r\n\x00") {
			return errors.New("configuration_source")
		}
	}
	for _, s := range []string{c.StateDir, c.RuntimeDir, c.ConnectionFile} {
		if !filepath.IsAbs(s) || strings.ContainsAny(s, "\r\n\x00") {
			return errors.New("configuration_path")
		}
	}
	state, runtime := filepath.Clean(c.StateDir), filepath.Clean(c.RuntimeDir)
	if state == runtime || strings.HasPrefix(runtime, state+"/") || strings.HasPrefix(state, runtime+"/") {
		return errors.New("configuration_path_overlap")
	}
	u, err := url.Parse(c.Report.WebsocketURL)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || strings.ContainsAny(c.Report.WebsocketURL, "\r\n\t ") {
		return errors.New("configuration_websocket")
	}
	allowed := map[string]bool{"agent-list": true, "http-method": true, "http-protocol": true, "anonymize-ip": true, "ignore-crawlers": true, "unknowns-as-crawlers": true, "real-os": true, "with-output-resolver": true, "no-query-string": true, "date-spec": true, "hour-spec": true, "all-static-files": true, "4xx-to-unique-count": true, "double-decode": true, "crawlers-only": true}
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
		default:
			if v != "true" && v != "false" && v != "yes" && v != "no" {
				return errors.New("configuration_override")
			}
		}
	}
	for _, line := range c.Report.ExtraBrowsers {
		if strings.Count(line, "\t") != 1 || strings.ContainsAny(line, "\r\n\x00") || strings.HasPrefix(line, "#") {
			return errors.New("configuration_browser")
		}
	}
	if c.Limits.MaxRuntimeBytes < 1048576 || c.Limits.MaxDatabaseBytes < 1048576 || c.Limits.MaxReplayBytes < 1048576 || c.Limits.MaxReportBytes < 1048576 || c.Limits.DiagnosticBytes < 128 || c.Limits.DiagnosticBytes > 67108864 || c.Limits.ReserveBytes < 1048576 {
		return errors.New("configuration_limits")
	}
	return nil
}
