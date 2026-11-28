package source

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zekihan/accessrelay/internal/config"
)

func TestGatehouseDecode(t *testing.T) {
	const access = `{"time":"2026-10-01T00:01:00.111222333Z","request_id":"id-1","listener":8443,"client_ip":"2001:db8::1","method":"GET","host":"app.example.test","path":"/objects","query":"Signature=[redacted]&page=1","proto":"HTTP/2.0","user_agent":"agent \"quoted\" \\tail\n","referer":"https://example.test/?state=[redacted]","status":200,"bytes_out":42,"duration_ms":3.125,"route":"app/web","backend":"app/web:8080"}`
	selection := config.Source{Cluster: "fixture", Namespace: "gatehouse", Container: "gatehouse"}
	formats := map[config.Source]string{selection: config.GatehouseJSON}
	row := map[string]string{
		"_time":                     "2026-10-01T00:01:01.123456789Z",
		"_stream_id":                "a",
		"cluster":                   selection.Cluster,
		"kubernetes.namespace_name": selection.Namespace,
		"kubernetes.container_name": selection.Container,
	}
	start, _ := ParseTimestamp(row["_time"])
	decodeMessage := func(message string) (Record, bool, error) {
		row["_msg"] = message
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		return decode(raw, start, start+1, formats)
	}
	got, operational, err := decodeMessage(access)
	const clf = `2001:db8::1 - - [01/Oct/2026:00:01:00 +0000] "GET /objects?Signature=[redacted]&page=1 HTTP/2.0" 200 42 "https://example.test/?state=[redacted]" "agent \"quoted\" \\tail\n" 1 "app/web" "app/web:8080" 3.125ms`
	if err != nil || operational || got.TimeNS != start || got.Message != clf ||
		got.route != "app/web" {
		t.Fatalf("record=%+v operational=%v error=%v", got, operational, err)
	}
	other, _, err := decodeMessage(strings.Replace(access, "id-1", "id-2", 1))
	if err != nil || got.Digest == other.Digest || got.Message != other.Message {
		t.Fatalf("requests with the same replay line must retain separate identities: %v", err)
	}
	_, operational, err = decodeMessage(
		`{"time":"2026-10-01T00:01:00Z","level":"INFO","msg":"listener started"}`,
	)
	if err != nil || !operational {
		t.Fatalf("operational=%v error=%v", operational, err)
	}
	_, operational, err = decodeMessage(
		strings.Replace(access, `"status":200`, `"status":"200"`, 1),
	)
	if err == nil || operational {
		t.Fatalf("malformed access event passed: operational=%v error=%v", operational, err)
	}
	const parseError = `{"time":"2026-10-01T00:01:00Z","request_id":"fail","listener":8443,"status":400,"client_ip":"","method":"","path":"","proto":"","route":"","backend":"","duration_ms":0,"bytes_out":0,"deny_reason":"http_parse_error"}`
	got, operational, err = decodeMessage(parseError)
	if err != nil || operational ||
		!strings.HasPrefix(got.Message, `0.0.0.0 - - [01/Oct/2026:00:01:00 +0000] "-" 400`) {
		t.Fatalf("parse refusal record=%+v operational=%v error=%v", got, operational, err)
	}
}
