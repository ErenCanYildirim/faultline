package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Migration is one numbered SQL file.
type Migration struct {
	Version    int
	Name       string
	Statements []string
}

var migrationName = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)

// LoadMigrations reads NNNN_name.sql files from dir in fsys, sorted by
// version. It fails on badly named files and duplicate versions, so a
// mistake is caught at startup instead of being skipped silently.
func LoadMigrations(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("reading migrations: %w", err)
	}
	var out []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration %q: name must look like 0001_description.sql", e.Name())
		}
		version, _ := strconv.Atoi(m[1])
		if other, dup := seen[version]; dup {
			return nil, fmt.Errorf("migrations %q and %q share version %d", other, e.Name(), version)
		}
		seen[version] = e.Name()

		raw, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		stmts := SplitStatements(string(raw))
		if len(stmts) == 0 {
			return nil, fmt.Errorf("migration %q has no statements", e.Name())
		}
		out = append(out, Migration{Version: version, Name: e.Name(), Statements: stmts})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// SplitStatements splits SQL on semicolons at the end of a line and drops
// full-line "--" comments. The ClickHouse HTTP interface runs one statement
// per request. Deliberately simple: migrations are our own files, so the
// rule "end each statement with ; at the end of a line" is enough.
func SplitStatements(sql string) []string {
	var stmts []string
	var cur strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		cur.WriteString(line)
		cur.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			s := strings.TrimSuffix(strings.TrimSpace(cur.String()), ";")
			if s != "" {
				stmts = append(stmts, s)
			}
			cur.Reset()
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

const createMigrationsTable = `CREATE TABLE IF NOT EXISTS schema_migrations
(
    version    UInt32,
    name       String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY version`

// Migrate applies every migration not yet recorded in schema_migrations, in
// order, and returns the names of those it applied.
//
// ClickHouse DDL is not transactional: if a statement fails, earlier
// statements of the same migration stay applied and the migration is not
// recorded. The next run starts that migration from the top again, which is
// why every statement must be safe to repeat (IF NOT EXISTS).
func Migrate(ctx context.Context, c *Client, migrations []Migration) ([]string, error) {
	if err := c.Exec(ctx, createMigrationsTable); err != nil {
		return nil, fmt.Errorf("creating schema_migrations: %w", err)
	}

	raw, err := c.Query(ctx, "SELECT version FROM schema_migrations FORMAT JSONEachRow")
	if err != nil {
		return nil, fmt.Errorf("reading applied migrations: %w", err)
	}
	applied := map[int]bool{}
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var row struct {
			Version int `json:"version"`
		}
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("parsing schema_migrations row %q: %w", line, err)
		}
		applied[row.Version] = true
	}

	var done []string
	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		for i, stmt := range m.Statements {
			if err := c.Exec(ctx, stmt); err != nil {
				return done, fmt.Errorf("migration %s, statement %d: %w", m.Name, i+1, err)
			}
		}
		record := fmt.Sprintf("INSERT INTO schema_migrations (version, name) VALUES (%d, '%s')",
			m.Version, strings.ReplaceAll(m.Name, "'", ""))
		if err := c.Exec(ctx, record); err != nil {
			return done, fmt.Errorf("recording migration %s: %w", m.Name, err)
		}
		done = append(done, m.Name)
	}
	return done, nil
}
