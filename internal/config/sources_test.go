package config

import (
	"encoding/json"
	"testing"
)

func TestCollectionSources(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config string
		err    string
	}{
		{"legacy", `{"source":{"cluster":"example"}}`, ""},
		{"explicit", `{"sources":[{"cluster":"example","namespace":"gatehouse","container":"gatehouse","format":"gatehouse-json"}],"excludeRoutes":["logs/journald","backup/s3"]}`, ""},
		{"empty", `{"source":{"cluster":"example"},"sources":[]}`, "configuration_source"},
		{"format", `{"sources":[{"cluster":"example","namespace":"gatehouse","container":"gatehouse"}]}`, "configuration_source"},
		{"ambiguous", `{"sources":[
			{"cluster":"example","namespace":"ingress","container":"proxy","format":"traefik-clf"},
			{"cluster":"example","namespace":"ingress","container":"proxy","format":"gatehouse-json"}
		]}`, "configuration_source"},
		{"duplicate route", `{"source":{"cluster":"example"},"excludeRoutes":["logs/journald","logs/journald"]}`, "configuration_exclusion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			if err := json.Unmarshal([]byte(tc.config), &c.Collection); err != nil {
				t.Fatal(err)
			}
			err := c.Validate()
			if tc.err == "" && err != nil || tc.err != "" && (err == nil || err.Error() != tc.err) {
				t.Fatalf("Validate() = %v, want %q", err, tc.err)
			}
		})
	}
}
