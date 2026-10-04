// Package renderer supervises GoAccess and publishes only complete reports.
package renderer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zekihan/accessrelay/internal/assets"
	"github.com/zekihan/accessrelay/internal/config"
)

type Snapshot struct {
	Running  bool   `json:"running"`
	Ready    bool   `json:"ready"`
	Restarts uint64 `json:"restarts"`
	Error    string `json:"error,omitempty"`
}
type Renderer struct {
	Config     config.Config
	mu         sync.Mutex
	cmd        *exec.Cmd
	done       chan error
	generation string
	output     string
	retry      time.Time
	backoff    time.Duration
	status     Snapshot
}

func New(c config.Config) (*Renderer, error) {
	r := &Renderer{Config: c, backoff: time.Second}
	for _, dir := range []string{filepath.Join(c.StateDir, "www"), filepath.Join(c.StateDir, "logs"), c.RuntimeDir} {
		if os.MkdirAll(dir, 0700) != nil {
			return nil, errors.New("renderer_storage")
		}
	}
	// Import an inherited complete report without writing to its generation.
	if _, err := os.Stat(r.ReportPath()); os.IsNotExist(err) {
		legacy := filepath.Join(c.StateDir, "www/current/index.html")
		if data, e := completeFile(legacy, c.Limits.MaxReportBytes); e == nil {
			if atomicFile(r.ReportPath(), annotate(data)) != nil {
				return nil, errors.New("report_publication")
			}
		}
	}
	return r, nil
}
func (r *Renderer) ReportPath() string { return filepath.Join(r.Config.StateDir, "www/report.html") }
func (r *Renderer) Snapshot() Snapshot { r.mu.Lock(); defer r.mu.Unlock(); return r.status }
func (r *Renderer) NeedsReplay() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cmd == nil && !time.Now().Before(r.retry)
}
func (r *Renderer) Generation() string { r.mu.Lock(); defer r.mu.Unlock(); return r.generation }
func (r *Renderer) Start(ctx context.Context, generation string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.stopLocked(ctx); err != nil {
		return err
	}
	dir := filepath.Join(r.Config.RuntimeDir, "generations", generation)
	r.output = filepath.Join(dir, "index.html")
	if os.MkdirAll(filepath.Join(dir, "db"), 0700) != nil {
		return errors.New("renderer_storage")
	}
	browsers := filepath.Join(dir, "browsers.list")
	conf := filepath.Join(dir, "goaccess.conf")
	if atomicFile(browsers, []byte(assets.Browsers+strings.Join(r.Config.Report.ExtraBrowsers, "\n")+"\n")) != nil {
		return errors.New("renderer_storage")
	}
	text := strings.NewReplacer("{{WS}}", r.Config.Report.WebsocketURL, "{{DAYS}}", strconv.Itoa(r.Config.Report.HistoryDays), "{{BROWSERS}}", browsers).Replace(assets.Config)
	keys := []string{}
	for key := range r.Config.Report.ConfigOverrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		lines := strings.Split(text, "\n")
		for i, line := range lines {
			if strings.HasPrefix(line, key+" ") {
				lines[i] = key + " " + r.Config.Report.ConfigOverrides[key]
			}
		}
		text = strings.Join(lines, "\n")
	}
	if atomicFile(conf, []byte(text)) != nil {
		return errors.New("renderer_storage")
	}
	args := []string{"--no-global-config", "--config-file=" + conf, "--jobs=" + strconv.Itoa(r.Config.Report.Jobs), "--chunk-size=" + strconv.Itoa(r.Config.Report.ChunkSize), "--db-path=" + filepath.Join(dir, "db"), "--pid-file=" + filepath.Join(dir, "child.pid"), "--unknowns-log=" + filepath.Join(r.Config.StateDir, "logs/unknowns.txt"), "--invalid-requests=" + filepath.Join(r.Config.StateDir, "logs/invalid-requests.log"), "--output=" + r.output, filepath.Join(dir, "access.log")}
	cmd := exec.Command(r.Config.GoAccessBinary, args...)
	configureChild(cmd)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "TZ=UTC", "LANG=C.UTF-8", "HOME=" + r.Config.RuntimeDir, "TMPDIR=" + r.Config.RuntimeDir}
	if cmd.Start() != nil {
		r.failedLocked("renderer_start")
		return errors.New("renderer_start")
	}
	r.cmd = cmd
	r.generation = generation
	r.done = make(chan error, 1)
	done := r.done
	go func() { done <- cmd.Wait() }()
	r.status.Running = true
	r.status.Ready = false
	r.status.Error = ""
	// Ownership transfers only after the old child has been reaped and the new child has started.
	if atomicFile(filepath.Join(r.Config.RuntimeDir, "consumer"), []byte(generation+"\n")) != nil {
		_ = r.stopLocked(ctx)
		return errors.New("renderer_handoff")
	}
	return nil
}
func (r *Renderer) failedLocked(reason string) {
	r.cmd = nil
	r.generation = ""
	r.status.Running = false
	r.status.Ready = false
	r.status.Error = reason
	r.status.Restarts++
	r.retry = time.Now().Add(r.backoff)
	r.backoff = min(60*time.Second, r.backoff*2)
}
func (r *Renderer) stopLocked(ctx context.Context) error {
	if r.cmd == nil {
		return nil
	}
	_ = r.cmd.Process.Signal(os.Interrupt)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-r.done:
	case <-ctx.Done():
		_ = r.cmd.Process.Kill()
		<-r.done
	case <-timer.C:
		_ = r.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-r.done:
		case <-time.After(2 * time.Second):
			_ = r.cmd.Process.Kill()
			<-r.done
		}
	}
	// GoAccess writes its last HTML snapshot on graceful exit.
	if data, err := completeFile(r.output, r.Config.Limits.MaxReportBytes); err == nil {
		_ = atomicFile(r.ReportPath(), annotate(data))
	}
	r.cmd = nil
	r.generation = ""
	r.status.Running = false
	r.status.Ready = false
	return nil
}
func (r *Renderer) Stop(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopLocked(ctx)
}
func (r *Renderer) Run(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick()
		}
	}
}
func (r *Renderer) tick() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, name := range []string{"unknowns.txt", "invalid-requests.log"} {
		path := filepath.Join(r.Config.StateDir, "logs", name)
		if info, err := os.Stat(path); err == nil && info.Size() > r.Config.Limits.DiagnosticBytes {
			if os.Truncate(path, 0) != nil {
				r.status.Error = "diagnostic_storage"
			}
		}
	}
	if r.cmd == nil {
		return
	}
	select {
	case <-r.done:
		r.failedLocked("renderer_exit")
		return
	default:
	}
	if !r.status.Ready {
		data, err := completeFile(r.output, r.Config.Limits.MaxReportBytes)
		if err != nil {
			return
		}
		conn, err := net.DialTimeout("tcp", "127.0.0.1:7890", 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		if atomicFile(r.ReportPath(), annotate(data)) != nil {
			r.status.Error = "report_publication"
			return
		}
		r.status.Ready = true
		r.status.Error = ""
		r.backoff = time.Second
		// Old diagnostic caches from the Python/shell deployment are derived data.
		for _, root := range []string{filepath.Join(r.Config.StateDir, "db"), filepath.Join(r.Config.StateDir, "www")} {
			entries, _ := os.ReadDir(root)
			for _, entry := range entries {
				if entry.IsDir() {
					_ = os.RemoveAll(filepath.Join(root, entry.Name()))
				} else if entry.Type()&os.ModeSymlink != 0 {
					_ = os.Remove(filepath.Join(root, entry.Name()))
				}
			}
		}
	}
}
func completeFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit || !strings.HasSuffix(strings.TrimSpace(string(data)), "</html>") {
		return nil, errors.New("report_incomplete")
	}
	return data, nil
}
func atomicFile(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".publish-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	if err = dir.Sync(); err != nil {
		return fmt.Errorf("publication_sync")
	}
	return nil
}

func annotate(data []byte) []byte {
	if strings.Contains(string(data), `id="accessrelay-status"`) {
		return data
	}
	banner := `<aside id="accessrelay-status" style="position:fixed;bottom:0;left:0;right:0;z-index:9999;background:#222;color:#fff;padding:6px 12px;font:14px sans-serif"><span id="accessrelay-freshness">Checking collection status</span> · <a style="color:#9cf" href="https://github.com/zekihan/accessrelay">accessrelay source</a></aside><script>(function(){async function refresh(){let el=document.getElementById('accessrelay-freshness');try{let r=await fetch('/status.json',{cache:'no-store'});if(!r.ok)throw Error();let s=await r.json();el.textContent=(s.stale?'Stale data':'Collecting')+(s.cursor?' · Collected through '+s.cursor:' · Waiting for the first complete query')+(s.unrecoverableIntervals.length?' · '+s.unrecoverableIntervals.length+' unrecoverable retention gap(s)':'');}catch(e){el.textContent='Collection status unavailable';}}refresh();setInterval(refresh,15000);})();</script>`
	return []byte(strings.Replace(string(data), "</body>", banner+"</body>", 1))
}
