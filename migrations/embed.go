// Package migrations holds the database schema as numbered SQL files,
// embedded into the migrate binary so the container needs no extra files.
//
// Rules for ClickHouse migrations:
//   - File names are NNNN_description.sql and are applied in order.
//   - Never edit a migration that has been applied; add a new one.
//   - ClickHouse DDL is not transactional. If a migration fails halfway, it
//     is retried from the start, so every statement must be safe to run
//     twice (CREATE ... IF NOT EXISTS, ADD COLUMN IF NOT EXISTS, ...).
//   - End each statement with a semicolon at the end of a line.
package migrations

import "embed"

// ClickHouse contains the ClickHouse migrations.
//
//go:embed clickhouse/*.sql
var ClickHouse embed.FS
