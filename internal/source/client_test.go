package source

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zekihan/accessrelay/internal/config"
)

const Line = `192.0.2.1 - - [01/Oct/2026:00:01:00 +0000] "GET /hello?q=1 HTTP/2.0" 200 42 "-" "Miniflux Client Library" 1 "router@kubernetes" "http://192.0.2.2:80" 2ms`

func TestPrecisionCanonicalIdentity(t *testing.T) {
	ns, e := ParseTimestamp("2026-10-01T03:01:00.123456789+03:00")
	if e != nil || Timestamp(ns) != "2026-10-01T00:01:00.123456789Z" {
		t.Fatal(ns, e)
	}
	canonical, e := CanonicalJSON([]any{[]string{"ストリーム", "pod<&>", "😀"}, ns, Line})
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(canonical, `\u30b9`) || !strings.Contains(canonical, `\ud83d\ude00`) || !strings.Contains(canonical, "pod<&>") {
		t.Fatal(canonical)
	}
	row := map[string]string{"_time": Timestamp(ns), "_stream_id": "ストリーム", "kubernetes.pod_id": "pod<&>", "kubernetes.docker_id": "😀", "_msg": Line}
	raw, _ := json.Marshal(row)
	r, op, e := Decode(raw, ns, ns+1)
	if e != nil || op || r.Digest != sha256.Sum256([]byte(canonical)) {
		t.Fatal(r, op, e)
	}
	if _, _, e = Decode(raw, ns-1, ns); e == nil {
		t.Fatal("accepted exclusive end")
	}
	for _, stamp := range []string{"2026-10-01T00:00:00.1234567890Z", "2026-10-01", "0001-01-01T00:00:00Z"} {
		if _, e = ParseTimestamp(stamp); e == nil {
			t.Fatal(stamp)
		}
	}
}
func TestCompleteResponsesAndProjectedSecretRotation(t *testing.T) {
	root := t.TempDir()
	start, _ := ParseTimestamp("2026-10-01T00:00:00Z")
	end, _ := ParseTimestamp("2026-10-01T00:05:00Z")
	row := map[string]string{"_time": "2026-10-01T00:01:00.123456789Z", "_stream_id": "a", "_msg": Line}
	raw, _ := json.Marshal(row)
	body := string(raw) + "\n"
	code := 200
	short := false
	authorization := ""
	requests := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		authorization = r.Header.Get("Authorization")
		if r.URL.Path != "/select/logsql/query" {
			t.Error("redirect followed")
		}
		_ = r.ParseForm()
		if !strings.Contains(r.Form.Get("query"), "allow_partial_response=false") || r.Form.Get("end") != Timestamp(end) {
			t.Error(r.Form)
		}
		w.Header().Set("Location", "/leak")
		n := len(body)
		if short {
			n += 10
		}
		w.Header().Set("Content-Length", fmt.Sprint(n))
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	defer backend.Close()
	c := config.Default()
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.ConnectionFile = filepath.Join(root, "connection.json")
	c.Collection.Source.Cluster = "fixture"
	client := New(c)
	defer client.Close()
	rotate := func(token string) {
		dir := filepath.Join(root, token)
		_ = os.Mkdir(dir, 0700)
		data, _ := json.Marshal(map[string]any{"url": backend.URL, "headers": map[string]string{"Authorization": token}})
		_ = os.WriteFile(filepath.Join(dir, "connection.json"), data, 0600)
		_ = os.Symlink(dir, filepath.Join(root, "..data-next"))
		_ = os.Rename(filepath.Join(root, "..data-next"), filepath.Join(root, "..data"))
	}
	rotate("test-a")
	_ = os.Symlink("..data/connection.json", c.ConnectionFile)
	for _, token := range []string{"test-a", "test-b"} {
		rotate(token)
		result, e := client.Fetch(context.Background(), start, end)
		if e != nil {
			t.Fatal(e)
		}
		if authorization != token {
			t.Fatal("secret not reloaded")
		}
		_ = os.Remove(result.Path)
	}
	for _, status := range []int{302, 401, 429, 500, 504} {
		code = status
		if result, e := client.Fetch(context.Background(), start, end); e == nil {
			_ = os.Remove(result.Path)
			t.Fatal("accepted status", status)
		}
	}
	code = 200
	for _, invalid := range []string{string(raw), "{bad}\n", "\n", strings.Replace(string(raw), "_stream_id", "_missing", 1) + "\n", strings.Replace(string(raw), "2026-10-01T00:01:00.123456789Z", Timestamp(end), 1) + "\n"} {
		body = invalid
		if result, e := client.Fetch(context.Background(), start, end); e == nil {
			_ = os.Remove(result.Path)
			t.Fatal("accepted incomplete response")
		}
	}
	body = string(raw) + "\n"
	short = true
	if _, e := client.Fetch(context.Background(), start, end); e == nil {
		t.Fatal("accepted truncated content length")
	}
	short = false
	body = strings.Repeat("x", c.Collection.MaxLineBytes+2) + "\n"
	if _, e := client.Fetch(context.Background(), start, end); e == nil {
		t.Fatal("accepted oversized line")
	}
	entries, _ := os.ReadDir(c.RuntimeDir)
	if len(entries) != 0 {
		t.Fatal("unbounded failed stages", entries)
	}
	if requests != 12 {
		t.Log(requests)
	}
	body = string(raw) + "\n" + string(raw) + "\n"
	result, e := client.Fetch(context.Background(), start, end)
	if e != nil {
		t.Fatal(e)
	}
	defer os.Remove(result.Path)
	db, e := sql.Open("sqlite", result.Path)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var copies int
	if e = db.QueryRow("SELECT copies FROM records").Scan(&copies); e != nil || copies != 2 {
		t.Fatal(copies, e)
	}
}
func TestOperationalAndInvalidCLF(t *testing.T) {
	ns, _ := ParseTimestamp("2026-10-01T00:01:00Z")
	for _, tc := range []struct {
		message     string
		op, invalid bool
	}{{"level=info started", true, false}, {strings.Replace(Line, "200 42", "BAD BYTES", 1), false, false}, {strings.Replace(Line, "192.0.2.1", "2001:db8::1", 1) + "\r\n", false, false}, {Line + "\nsecond", false, true}} {
		raw, _ := json.Marshal(map[string]string{"_time": Timestamp(ns), "_stream_id": "a", "_msg": tc.message})
		_, op, e := Decode(raw, ns, ns+1)
		if op != tc.op || (e != nil) != tc.invalid {
			t.Fatal(tc, op, e)
		}
	}
}
