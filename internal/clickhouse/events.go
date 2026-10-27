package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/erencanyildirim/faultline/event"
)

// EventsTable is the table created by migration 0001.
const EventsTable = "events"

// ClickHouse parses DateTime64 from this layout by default. RFC 3339 with a
// "T" and "Z" needs a non-default setting, so the worker formats explicitly.
const dateTimeLayout = "2006-01-02 15:04:05.000"

// eventRow is one row of the events table in JSONEachRow form. Field names
// are column names; Nested columns are flattened as "frames.<field>".
type eventRow struct {
	ProjectID      string            `json:"project_id"`
	Fingerprint    string            `json:"fingerprint"`
	Timestamp      string            `json:"timestamp"`
	EventID        string            `json:"event_id"`
	ReceivedAt     string            `json:"received_at"`
	Level          string            `json:"level"`
	Message        string            `json:"message"`
	ExceptionType  string            `json:"exception_type"`
	ExceptionValue string            `json:"exception_value"`
	FrameModule    []string          `json:"frames.module"`
	FrameFunction  []string          `json:"frames.function"`
	FrameFile      []string          `json:"frames.file"`
	FrameLine      []uint32          `json:"frames.line"`
	FrameInApp     []bool            `json:"frames.in_app"`
	Tags           map[string]string `json:"tags"`
	UserID         string            `json:"user_id"`
	UserUsername   string            `json:"user_username"`
	UserEmail      string            `json:"user_email"`
	UserIPAddress  string            `json:"user_ip_address"`
	Release        string            `json:"release"`
	Environment    string            `json:"environment"`
	SDKName        string            `json:"sdk_name"`
	SDKVersion     string            `json:"sdk_version"`
	// inserted_at is left out: ClickHouse fills it with its default, now64(3).
}

func toRow(e *event.Event) eventRow {
	r := eventRow{
		ProjectID:   e.ProjectID,
		Fingerprint: "", // Phase 2
		Timestamp:   e.Timestamp.UTC().Format(dateTimeLayout),
		EventID:     e.EventID,
		ReceivedAt:  e.ReceivedAt.UTC().Format(dateTimeLayout),
		Level:       string(e.Level),
		Message:     e.Message,
		Tags:        e.Tags,
		Release:     e.Release,
		Environment: e.Environment,
		SDKName:     e.SDK.Name,
		SDKVersion:  e.SDK.Version,
		// Empty arrays and maps, never null: ClickHouse rejects null for
		// non-Nullable columns.
		FrameModule:   []string{},
		FrameFunction: []string{},
		FrameFile:     []string{},
		FrameLine:     []uint32{},
		FrameInApp:    []bool{},
	}
	if r.Tags == nil {
		r.Tags = map[string]string{}
	}
	if ex := e.Exception; ex != nil {
		r.ExceptionType = ex.Type
		r.ExceptionValue = ex.Value
		for _, f := range ex.Frames {
			r.FrameModule = append(r.FrameModule, f.Module)
			r.FrameFunction = append(r.FrameFunction, f.Function)
			r.FrameFile = append(r.FrameFile, f.File)
			r.FrameLine = append(r.FrameLine, uint32(f.Line)) // validated: 0 <= line <= MaxUint32
			r.FrameInApp = append(r.FrameInApp, f.InApp)
		}
	}
	if u := e.User; u != nil {
		r.UserID, r.UserUsername, r.UserEmail, r.UserIPAddress = u.ID, u.Username, u.Email, u.IPAddress
	}
	return r
}

// EncodeEvents turns events into a JSONEachRow body: one object per line.
func EncodeEvents(events []event.Event) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf) // Encode appends the newline JSONEachRow needs
	for i := range events {
		if err := enc.Encode(toRow(&events[i])); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// EventStore writes events to the events table.
type EventStore struct {
	Client *Client
}

// InsertEvents writes all events in one INSERT.
func (s *EventStore) InsertEvents(ctx context.Context, events []event.Event) error {
	body, err := EncodeEvents(events)
	if err != nil {
		return err
	}
	return s.Client.InsertJSONEachRow(ctx, EventsTable, body)
}
