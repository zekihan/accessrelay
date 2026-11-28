package source

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
)

type gatehouseRecord struct {
	RequestID  json.RawMessage `json:"request_id"`
	Time       string          `json:"time"`
	Listener   uint16          `json:"listener"`
	ClientIP   string          `json:"client_ip"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Query      string          `json:"query"`
	Proto      string          `json:"proto"`
	UserAgent  string          `json:"user_agent"`
	Referer    string          `json:"referer"`
	Status     int             `json:"status"`
	BytesOut   uint64          `json:"bytes_out"`
	DurationMS float64         `json:"duration_ms"`
	Route      string          `json:"route"`
	Backend    string          `json:"backend"`
}

type gatehouseIdentity struct {
	Format   string        `json:"format"`
	Selector config.Source `json:"selector"`
	Stream   []string      `json:"stream"`
	Route    string        `json:"route"`
}

func gatehouseCLF(message string) (string, string, bool, error) {
	var entry gatehouseRecord
	decodeErr := json.Unmarshal([]byte(message), &entry)
	if entry.RequestID == nil {
		return "", "", true, nil
	}
	invalid := errors.New("invalid_record")
	var id string
	if decodeErr != nil || !validUnicodeEscapes([]byte(message)) ||
		json.Unmarshal(entry.RequestID, &id) != nil || id == "" ||
		entry.Listener == 0 || entry.Status < 100 || entry.Status > 999 || entry.DurationMS < 0 {
		return "", "", false, invalid
	}
	stamp, err := ParseTimestamp(entry.Time)
	if err != nil {
		return "", "", false, invalid
	}
	addr := "0.0.0.0"
	if entry.ClientIP != "" {
		parsed, err := netip.ParseAddr(entry.ClientIP)
		if err != nil {
			return "", "", false, invalid
		}
		addr = parsed.WithZone("").String()
	}
	request := "-"
	if entry.Method != "" && entry.Proto != "" {
		target := entry.Path
		if target == "" {
			target = "/"
		}
		if entry.Query != "" {
			target += "?" + entry.Query
		}
		request = entry.Method + " " + target + " " + entry.Proto
	}
	// TRAEFIKCLF uses the router position as its virtual host. Retain the
	// Gatehouse route there and keep the renderer's existing log format.
	line := fmt.Sprintf(
		"%s - - [%s] %s %d %d %s %s 1 %s %s %sms",
		addr,
		time.Unix(0, stamp).UTC().Format("02/Jan/2006:15:04:05 -0700"),
		strconv.Quote(request),
		entry.Status,
		entry.BytesOut,
		quoteCLF(entry.Referer),
		quoteCLF(entry.UserAgent),
		quoteCLF(entry.Route),
		quoteCLF(entry.Backend),
		strconv.FormatFloat(entry.DurationMS, 'f', 3, 64),
	)
	return line, entry.Route, false, nil
}

func quoteCLF(value string) string {
	if value == "" {
		value = "-"
	}
	return strconv.Quote(value)
}

// ReplayExcluded keeps exclusions scoped to the input format after restart.
func ReplayExcluded(identity, message string, routers, routes map[string]bool) bool {
	if !strings.HasPrefix(identity, `{"format":"`+config.GatehouseJSON+`",`) {
		return len(routers) > 0 && Excluded(message, routers)
	}
	var src gatehouseIdentity
	return len(routes) > 0 && json.Unmarshal([]byte(identity), &src) == nil && routes[src.Route]
}
