package ingest

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// MinKeyLength rejects keys short enough to guess.
const MinKeyLength = 16

// Projects maps project keys to project IDs. It is read once at startup and
// never changes, so it needs no locking.
type Projects struct {
	// Keyed by the SHA-256 of the project key. A map lookup on the raw key
	// would take slightly different time depending on how much of a guessed
	// key matches; hashing first removes that signal.
	byKeyHash map[[32]byte]string
}

type projectsFile struct {
	Projects []struct {
		ID   string   `json:"id"`
		Keys []string `json:"keys"`
	} `json:"projects"`
}

// LoadProjects reads the projects file. Format:
//
//	{"projects": [{"id": "demo", "keys": ["<key>", "<another key>"]}]}
//
// Several keys per project allow rotating a key without downtime.
func LoadProjects(path string) (*Projects, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading projects file: %w", err)
	}
	return ParseProjects(raw)
}

// ParseProjects parses the projects file contents and checks them.
func ParseProjects(raw []byte) (*Projects, error) {
	var f projectsFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parsing projects file: %w", err)
	}
	if len(f.Projects) == 0 {
		return nil, errors.New("projects file defines no projects")
	}
	p := &Projects{byKeyHash: make(map[[32]byte]string)}
	seenIDs := make(map[string]bool)
	for _, proj := range f.Projects {
		if proj.ID == "" {
			return nil, errors.New("a project has an empty id")
		}
		if seenIDs[proj.ID] {
			return nil, fmt.Errorf("project %q is defined twice", proj.ID)
		}
		seenIDs[proj.ID] = true
		if len(proj.Keys) == 0 {
			return nil, fmt.Errorf("project %q has no keys", proj.ID)
		}
		for _, key := range proj.Keys {
			if len(key) < MinKeyLength {
				return nil, fmt.Errorf("project %q has a key shorter than %d characters", proj.ID, MinKeyLength)
			}
			h := sha256.Sum256([]byte(key))
			if owner, dup := p.byKeyHash[h]; dup {
				return nil, fmt.Errorf("a key of project %q is also used by project %q", proj.ID, owner)
			}
			p.byKeyHash[h] = proj.ID
		}
	}
	return p, nil
}

// Lookup returns the project that owns key.
func (p *Projects) Lookup(key string) (projectID string, ok bool) {
	if key == "" {
		return "", false
	}
	projectID, ok = p.byKeyHash[sha256.Sum256([]byte(key))]
	return projectID, ok
}

// Len returns the number of keys loaded.
func (p *Projects) Len() int { return len(p.byKeyHash) }
