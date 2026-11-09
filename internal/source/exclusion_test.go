package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zekihan/accessrelay/internal/config"
)

const journaldLine = `79.76.97.184 - - [02/Nov/2026:00:00:00 +0000] "POST /insert/journald/upload HTTP/2.0" 200 0 "-" "systemd-journal-upload 255.4-1ubuntu8.18" 17335 "traefik-mtls-journald-upload-fcc44d81276fc53b472d@kubernetescrd" "http://192.168.2.75:9428" 9ms`

func TestRouter(t *testing.T) {
	for _, tc := range []struct {
		message, router string
		ok              bool
	}{
		{Line, "router@kubernetes", true},
		{journaldLine, "traefik-mtls-journald-upload-fcc44d81276fc53b472d@kubernetescrd", true},
		{strings.Replace(Line, `"router@kubernetes" "http://192.0.2.2:80"`, `"-" "-"`, 1), "-", true},
		// A router named inside the request or user agent is not the router.
		{strings.Replace(Line, "Miniflux Client Library", `x" 1 "decoy@kubernetes" "y`, 1), "router@kubernetes", true},
		{strings.TrimSuffix(Line, " 2ms"), "", false},
		{"level=info started", "", false},
	} {
		router, ok := Router(tc.message)
		if router != tc.router || ok != tc.ok {
			t.Errorf("Router(%q) = %q, %v; want %q, %v", tc.message, router, ok, tc.router, tc.ok)
		}
	}
	excluded := map[string]bool{"router@kubernetes": true}
	if !Excluded(Line, excluded) || Excluded(journaldLine, excluded) || Excluded("level=info started", excluded) {
		t.Fatal("exclusion does not follow the router field")
	}
	if Excluded(strings.Replace(Line, "router@kubernetes", "router@kubernetes-2", 1), excluded) {
		t.Fatal("router names must match exactly")
	}
}

func TestFetchSkipsExcludedRouters(t *testing.T) {
	root := t.TempDir()
	start, _ := ParseTimestamp("2026-11-02T00:00:00Z")
	end, _ := ParseTimestamp("2026-11-02T00:05:00Z")
	var body strings.Builder
	for i, message := range []string{journaldLine, Line, journaldLine} {
		raw, _ := json.Marshal(map[string]string{"_time": "2026-11-02T00:00:0" + string(rune('1'+i)) + "Z", "_stream_id": "a", "_msg": message})
		body.Write(append(raw, '\n'))
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body.String()))
	}))
	defer backend.Close()
	c := config.Default()
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.ConnectionFile = filepath.Join(root, "connection.json")
	c.Collection.Source.Cluster = "fixture"
	c.Collection.ExcludeRouters = []string{"traefik-mtls-journald-upload-fcc44d81276fc53b472d@kubernetescrd"}
	raw, _ := json.Marshal(map[string]string{"url": backend.URL})
	if err := os.WriteFile(c.ConnectionFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	client := New(c)
	defer client.Close()
	result, err := client.Fetch(context.Background(), start, end)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(result.Path) }()
	if result.Excluded != 2 || result.Operational != 0 {
		t.Fatalf("excluded=%d operational=%d", result.Excluded, result.Operational)
	}
	db, err := sql.Open("sqlite", result.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var message string
	var records int
	if err = db.QueryRow(`SELECT COUNT(*),MAX(message) FROM records`).Scan(&records, &message); err != nil || records != 1 || message != Line {
		t.Fatalf("records=%d message=%q err=%v", records, message, err)
	}
}
