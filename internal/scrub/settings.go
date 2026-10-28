package scrub

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadSettings reads per-project scrubbing settings from the projects file
// that ingest also reads. Keys in that file are ignored here; the worker
// only needs the settings:
//
//	{"projects": [{"id": "demo", "keys": ["..."], "store_ip_addresses": false}]}
func LoadSettings(path string) (map[string]Settings, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading projects file: %w", err)
	}
	return ParseSettings(raw)
}

// ParseSettings parses the projects file contents.
func ParseSettings(raw []byte) (map[string]Settings, error) {
	var f struct {
		Projects []struct {
			ID string `json:"id"`
			Settings
		} `json:"projects"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parsing projects file: %w", err)
	}
	out := make(map[string]Settings, len(f.Projects))
	for _, p := range f.Projects {
		if p.ID == "" {
			return nil, fmt.Errorf("a project has an empty id")
		}
		out[p.ID] = p.Settings
	}
	return out, nil
}
