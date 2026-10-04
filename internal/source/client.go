package source

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
	_ "modernc.org/sqlite"
)

type QueryError struct {
	Reason string
	Split  bool
}

func (e *QueryError) Error() string              { return e.Reason }
func queryError(reason string, split bool) error { return &QueryError{reason, split} }

type Result struct {
	Path        string
	Operational int64
	Bytes       int64
}
type Client struct {
	Config config.Config
	HTTP   *http.Client
}

func New(c config.Config) *Client {
	return &Client{Config: c, HTTP: &http.Client{Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: config.Duration(c.Collection.QueryTimeout) + 5*time.Second}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (c *Client) Close() { c.HTTP.CloseIdleConnections() }
func (c *Client) Fetch(ctx context.Context, start, end int64) (result Result, err error) {
	if start >= end {
		return result, queryError("invalid_window", false)
	}
	raw, e := os.ReadFile(c.Config.ConnectionFile)
	if e != nil || len(raw) > 1048576 {
		return result, queryError("connection_configuration", false)
	}
	var connection struct {
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
	}
	if json.Unmarshal(raw, &connection) != nil {
		return result, queryError("connection_configuration", false)
	}
	u, e := url.Parse(connection.URL)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return result, queryError("connection_configuration", false)
	}
	query := "options(allow_partial_response=false)"
	for _, field := range [][2]string{{"cluster", c.Config.Collection.Source.Cluster}, {"kubernetes.namespace_name", c.Config.Collection.Source.Namespace}, {"kubernetes.container_name", c.Config.Collection.Source.Container}} {
		v, _ := CanonicalJSON(field[1])
		query += " " + field[0] + ":" + v
	}
	query += " | fields _time, _stream_id, kubernetes.pod_id, kubernetes.docker_id, _msg"
	params := url.Values{"query": {query}, "start": {Timestamp(start)}, "end": {Timestamp(end)}, "timeout": {c.Config.Collection.QueryTimeout}, "allow_partial_response": {"0"}}
	requestCtx, cancel := context.WithTimeout(ctx, config.Duration(c.Config.Collection.QueryTimeout)+5*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(requestCtx, http.MethodPost, strings.TrimRight(connection.URL, "/")+"/select/logsql/query", strings.NewReader(params.Encode()))
	if e != nil {
		return result, queryError("connection_configuration", false)
	}
	for k, v := range connection.Headers {
		// Backend credentials cannot override framing or redirect the request.
		if k == "" || strings.ContainsAny(k, "\r\n :\t") || strings.ContainsAny(v, "\r\n") || strings.EqualFold(k, "Host") || strings.EqualFold(k, "Content-Length") || strings.EqualFold(k, "Transfer-Encoding") {
			return result, queryError("connection_headers", false)
		}
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept-Encoding", "identity")
	response, e := c.HTTP.Do(req)
	if e != nil {
		var ne net.Error
		return result, queryError("backend_request", errors.As(e, &ne) && ne.Timeout())
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != 200 {
		return result, queryError("backend_request", response.StatusCode == 408 || response.StatusCode == 504)
	}
	if response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" {
		return result, queryError("response_encoding", false)
	}
	if e = os.MkdirAll(c.Config.RuntimeDir, 0700); e != nil {
		return result, errors.New("stage_storage")
	}
	f, e := os.CreateTemp(c.Config.RuntimeDir, "window-*.sqlite")
	if e != nil {
		return result, errors.New("stage_storage")
	}
	result.Path = f.Name()
	_ = f.Close()
	defer func() {
		if err != nil {
			_ = os.Remove(result.Path)
		}
	}()
	db, e := sql.Open("sqlite", filepath.ToSlash(result.Path))
	if e != nil {
		return result, errors.New("stage_storage")
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if _, e = db.ExecContext(ctx, `CREATE TABLE records(digest BLOB PRIMARY KEY,time_ns INTEGER,source TEXT,message TEXT,copies INTEGER)`); e != nil {
		return result, errors.New("stage_storage")
	}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return result, errors.New("stage_storage")
	}
	defer func() { _ = tx.Rollback() }()
	statement, e := tx.PrepareContext(ctx, `INSERT INTO records VALUES(?,?,?,?,1) ON CONFLICT(digest) DO UPDATE SET copies=copies+1`)
	if e != nil {
		return result, errors.New("stage_storage")
	}
	defer func() { _ = statement.Close() }()
	reader := bufio.NewReaderSize(response.Body, c.Config.Collection.MaxLineBytes+1)
	for {
		line, readErr := reader.ReadSlice('\n')
		result.Bytes += int64(len(line))
		if len(line) > c.Config.Collection.MaxLineBytes || result.Bytes > c.Config.Collection.MaxWindowBytes {
			return result, queryError("window_limit", true)
		}
		if readErr != nil {
			if readErr == io.EOF && len(line) == 0 {
				break
			}
			return result, queryError("incomplete_response", readErr != bufio.ErrBufferFull)
		}
		record, operational, e := Decode(line, start, end)
		if e != nil {
			return result, queryError("invalid_record", false)
		}
		if operational {
			result.Operational++
			continue
		}
		if _, e = statement.ExecContext(ctx, record.Digest[:], record.TimeNS, record.Source, record.Message); e != nil {
			return result, errors.New("stage_storage")
		}
	}
	if response.ContentLength >= 0 && response.ContentLength != result.Bytes {
		return result, queryError("incomplete_response", true)
	}
	if e = tx.Commit(); e != nil {
		return result, errors.New("stage_storage")
	}
	return result, nil
}
