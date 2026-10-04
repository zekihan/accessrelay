// Package source reads complete VictoriaLogs windows into disposable SQLite stages.
package source

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

var stamp = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)$`)
var clf = regexp.MustCompile(`^\S+ - \S+ \[[^\]\r\n]+\] "(?:[^"\\]|\\.)*" `)

func Timestamp(ns int64) string {
	return time.Unix(0, ns).UTC().Format("2006-01-02T15:04:05.000000000Z")
}
func ParseTimestamp(s string) (int64, error) {
	if !stamp.MatchString(s) {
		return 0, errors.New("invalid_timestamp")
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.Year() < 1678 || t.Year() > 2261 {
		return 0, errors.New("invalid_timestamp")
	}
	return t.UnixNano(), nil
}

// CanonicalJSON matches Python json.dumps(..., separators=(",", ":")), including ensure_ascii.
// Retaining this encoding preserves the digest keys of the original SQLite database.
func CanonicalJSON(v any) (string, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return "", err
	}
	var out strings.Builder
	for _, r := range strings.TrimSuffix(b.String(), "\n") {
		if r < 127 {
			out.WriteRune(r)
		} else if r <= 0xffff {
			fmt.Fprintf(&out, "\\u%04x", r)
		} else {
			hi, lo := utf16.EncodeRune(r)
			fmt.Fprintf(&out, "\\u%04x\\u%04x", hi, lo)
		}
	}
	return out.String(), nil
}

type Record struct {
	Digest          [32]byte
	TimeNS          int64
	Source, Message string
}

func Decode(raw []byte, start, end int64) (Record, bool, error) {
	var r Record
	if !utf8.Valid(raw) {
		return r, false, errors.New("invalid_record")
	}
	var row map[string]json.RawMessage
	if json.Unmarshal(raw, &row) != nil || row == nil {
		return r, false, errors.New("invalid_record")
	}
	read := func(key string, required bool) (string, error) {
		v, ok := row[key]
		if !ok && !required {
			return "", nil
		}
		if !ok || bytes.Equal(v, []byte("null")) {
			return "", errors.New("invalid_record")
		}
		var s string
		if json.Unmarshal(v, &s) != nil {
			return "", errors.New("invalid_record")
		}
		return s, nil
	}
	ts, err := read("_time", true)
	if err != nil {
		return r, false, err
	}
	r.TimeNS, err = ParseTimestamp(ts)
	if err != nil || r.TimeNS < start || r.TimeNS >= end {
		return r, false, errors.New("invalid_record")
	}
	r.Message, err = read("_msg", true)
	if err != nil {
		return r, false, err
	}
	r.Message = strings.TrimSuffix(strings.TrimSuffix(r.Message, "\n"), "\r")
	if strings.ContainsAny(r.Message, "\r\n\x00") {
		return r, false, errors.New("invalid_record")
	}
	identity := []string{}
	for _, key := range []string{"_stream_id", "kubernetes.pod_id", "kubernetes.docker_id"} {
		s, e := read(key, key == "_stream_id")
		if e != nil {
			return r, false, e
		}
		identity = append(identity, s)
	}
	if !clf.MatchString(r.Message) {
		return r, true, nil
	}
	r.Source, err = CanonicalJSON(identity)
	if err != nil {
		return r, false, err
	}
	encoded, err := CanonicalJSON([]any{identity, r.TimeNS, r.Message})
	if err != nil {
		return r, false, err
	}
	r.Digest = sha256.Sum256([]byte(encoded))
	return r, false, nil
}
