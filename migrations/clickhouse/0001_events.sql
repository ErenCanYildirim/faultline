-- Raw events, one row per event.
--
-- Engine: ReplacingMergeTree removes rows with the same sorting key during
-- background merges. The worker delivers at least once, so the same event
-- can be inserted twice; the copies share event_id and every other key
-- column, and merges collapse them. Until a merge runs, queries that need
-- exact counts use FINAL.
--
-- Partition: one per day of the event's timestamp. TTL drops whole days, and
-- queries over a time range skip the days outside it. 30 days = 30 partitions,
-- well within what ClickHouse handles comfortably.
--
-- Sorting key: (project_id, fingerprint, timestamp, event_id).
--   project_id first: every query is scoped to one project.
--   fingerprint next: "events of this issue" reads one contiguous range.
--   timestamp: time ranges within an issue; newest events cheaply.
--   event_id last: makes the key unique per event, which is what dedupe needs.
-- fingerprint is empty until grouping arrives in Phase 2. It is in the key
-- now because changing a sorting key later means rewriting the table.
--
-- Statements end with a semicolon at the end of a line; the migrator splits on that.

CREATE TABLE IF NOT EXISTS events
(
    project_id       LowCardinality(String),
    fingerprint      String DEFAULT '',
    timestamp        DateTime64(3, 'UTC'),
    event_id         UUID,
    received_at      DateTime64(3, 'UTC'),
    -- Set by ClickHouse at insert time. received_at to inserted_at is the
    -- pipeline's end-to-end latency, measured in Phase 3.
    inserted_at      DateTime64(3, 'UTC') DEFAULT now64(3),

    level            LowCardinality(String),
    message          String,
    exception_type   String,
    exception_value  String,
    frames Nested
    (
        module   String,
        function String,
        file     String,
        line     UInt32,
        in_app   Bool
    ),

    tags             Map(LowCardinality(String), String),
    user_id          String,
    user_username    String,
    user_email       String,
    user_ip_address  String,
    release          LowCardinality(String),
    environment      LowCardinality(String),
    sdk_name         LowCardinality(String),
    sdk_version      LowCardinality(String),

    -- Looking up one event by ID (event detail page) can't use the sorting
    -- key without the fingerprint; the bloom filter lets it skip most granules.
    INDEX idx_event_id event_id TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(timestamp)
ORDER BY (project_id, fingerprint, timestamp, event_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;