//go:build integration

package integration

import (
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
)

func TestStatusAvailableDuringDatabaseInitialization(t *testing.T) {
	binary := os.Getenv("ACCESSRELAY_BINARY")
	if binary == "" {
		t.Skip("no application binary supplied")
	}
	root := t.TempDir()
	c := config.Default()
	c.StateDir, c.RuntimeDir = filepath.Join(root, "state"), filepath.Join(root, "runtime")
	c.ConnectionFile = filepath.Join(root, "missing.json")
	c.Collection.Source.Cluster = "fixture"
	if err := os.Mkdir(c.StateDir, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(c.StateDir, "events.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA journal_mode=DELETE; CREATE TABLE blocking(value); BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`ROLLBACK`)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.Listen = listener.Addr().String()
	listener.Close()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "config.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "--config", path)
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	client := http.Client{Timeout: 250 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + c.Listen + "/status.json")
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		var status struct {
			Initialized bool
			Phase       string
			Renderer    struct{ Ready bool }
		}
		err = json.NewDecoder(response.Body).Decode(&status)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != 200 || status.Initialized || status.Renderer.Ready || status.Phase != "initializing" {
			t.Fatalf("invalid startup status: %+v", status)
		}
		found = true
		break
	}
	if !found {
		t.Fatal("status unavailable while database initialization is blocked")
	}
	for _, path := range []string{"/health", "/ready"} {
		response, err := client.Get("http://" + c.Listen + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 503 {
			t.Fatalf("%s returned %d during initialization", path, response.StatusCode)
		}
	}
}
