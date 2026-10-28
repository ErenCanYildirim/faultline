-- Issues and hourly counts, kept up to date by materialized views on events.
--
-- The worker only inserts into events. Every insert also runs the two views
-- below, which add the new events to the per-issue aggregates. Nothing else
-- writes to issues or issue_hourly.
--
-- Counting: issues count DISTINCT event IDs (uniqCombined64), not rows.
-- At-least-once delivery means the same event can be inserted again, in a
-- different batch, after a worker crash. A row count (or a SummingMergeTree)
-- would count it twice; a distinct count of event_id cannot. Measured on
-- ClickHouse 26.8: exact up to 1,000 events per issue, within 0.33% up to
-- 10 million, and the state is at most ~98 KB per issue (an exact distinct
-- count would need ~16 MB per million events).
--
-- Deduplication window: each table remembers the last 1000 insert tokens.
-- The worker sends a token per batch, so retrying the exact same insert
-- (e.g. after a timeout where the first attempt actually succeeded) writes
-- nothing twice. The window must be set on the views' target tables too:
-- without it the source insert is skipped but the views still count it.
-- Tokens don't help when a batch is redelivered with different contents;
-- the distinct count is what covers that case.
--
-- Every statement is safe to run twice (see migrations/embed.go).

ALTER TABLE events MODIFY SETTING non_replicated_deduplication_window = 1000;

-- One row per issue after merges; until then, several partial rows per key
-- that queries combine with the -Merge functions (see issue_summary).
CREATE TABLE IF NOT EXISTS issues
(
    project_id  LowCardinality(String),
    fingerprint String,
    -- Title and culprit of the FIRST event: stable, so the issue list
    -- doesn't change wording with every new occurrence.
    title       AggregateFunction(argMin, String, DateTime64(3, 'UTC')),
    culprit     AggregateFunction(argMin, String, DateTime64(3, 'UTC')),
    -- Level of the LATEST event: shows when an issue escalates.
    level       AggregateFunction(argMax, String, DateTime64(3, 'UTC')),
    first_seen  SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen   SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    events      AggregateFunction(uniqCombined64, UUID)
)
ENGINE = AggregatingMergeTree
ORDER BY (project_id, fingerprint)
-- An issue disappears 90 days after its last event. Its count covers every
-- event seen while it was active, so it can exceed the events still stored
-- (those expire after 30 days).
TTL toDateTime(last_seen) + INTERVAL 90 DAY DELETE
SETTINGS non_replicated_deduplication_window = 1000;

-- Events per issue per hour, for the counts-over-time chart.
CREATE TABLE IF NOT EXISTS issue_hourly
(
    project_id  LowCardinality(String),
    fingerprint String,
    hour        DateTime('UTC'),
    events      AggregateFunction(uniqCombined64, UUID)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(hour)
ORDER BY (project_id, fingerprint, hour)
TTL hour + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1, non_replicated_deduplication_window = 1000;

-- Created before the backfill below, so no insert falls between the two.
-- An insert counted by both is harmless: every aggregate here is idempotent.
CREATE MATERIALIZED VIEW IF NOT EXISTS issues_mv TO issues AS
SELECT
    project_id,
    fingerprint,
    argMinState(leftUTF8(if(exception_type != '', concat(exception_type, ': ', exception_value), message), 200), timestamp) AS title,
    argMinState(
        if((arrayFirst((name, in_app) -> in_app, arrayMap((m, f) -> concat(m, '.', f), frames.module, frames.function), frames.in_app) AS app_frame) != '',
           app_frame,
           arrayMap((m, f) -> concat(m, '.', f), frames.module, frames.function)[1]),
        timestamp) AS culprit,
    argMaxState(toString(level), timestamp) AS level,
    min(timestamp) AS first_seen,
    max(timestamp) AS last_seen,
    uniqCombined64State(event_id) AS events
FROM events
WHERE fingerprint != ''
GROUP BY project_id, fingerprint;

CREATE MATERIALIZED VIEW IF NOT EXISTS issue_hourly_mv TO issue_hourly AS
SELECT
    project_id,
    fingerprint,
    toDateTime(toStartOfHour(timestamp), 'UTC') AS hour,
    uniqCombined64State(event_id) AS events
FROM events
WHERE fingerprint != ''
GROUP BY project_id, fingerprint, hour;

-- Backfill from events already stored (those with a fingerprint, i.e.
-- since step 2.1). Re-running it changes nothing: min, max, argMin, argMax
-- and distinct counts give the same result when fed the same events twice.
INSERT INTO issues
SELECT
    project_id,
    fingerprint,
    argMinState(leftUTF8(if(exception_type != '', concat(exception_type, ': ', exception_value), message), 200), timestamp),
    argMinState(
        if((arrayFirst((name, in_app) -> in_app, arrayMap((m, f) -> concat(m, '.', f), frames.module, frames.function), frames.in_app) AS app_frame) != '',
           app_frame,
           arrayMap((m, f) -> concat(m, '.', f), frames.module, frames.function)[1]),
        timestamp),
    argMaxState(toString(level), timestamp),
    min(timestamp),
    max(timestamp),
    uniqCombined64State(event_id)
FROM events
WHERE fingerprint != ''
GROUP BY project_id, fingerprint;

INSERT INTO issue_hourly
SELECT
    project_id,
    fingerprint,
    toDateTime(toStartOfHour(timestamp), 'UTC'),
    uniqCombined64State(event_id)
FROM events
WHERE fingerprint != ''
GROUP BY project_id, fingerprint, toDateTime(toStartOfHour(timestamp), 'UTC');

-- The issue list as one row per issue, merging partial aggregates.
-- Filter on project_id; ClickHouse pushes it into the GROUP BY.
CREATE VIEW IF NOT EXISTS issue_summary AS
SELECT
    project_id,
    fingerprint,
    argMinMerge(title) AS title,
    argMinMerge(culprit) AS culprit,
    argMaxMerge(level) AS level,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen,
    uniqCombined64Merge(events) AS events
FROM issues
GROUP BY project_id, fingerprint;