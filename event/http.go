package event

// HTTP side of the contract between the SDK and the ingest API.
const (
	// EventsPath is the batch endpoint, relative to the DSN's host and path.
	EventsPath = "/api/v1/events"

	// KeyHeader carries the project key. A header instead of a query
	// parameter keeps keys out of access logs and proxy logs.
	KeyHeader = "X-Faultline-Key"
)

// BatchResponse is the body ingest returns for a batch request.
//
//	202 Accepted: at least one event was accepted and acknowledged by the queue.
//	400 Bad Request: no event in the batch was valid (Accepted is 0).
//
// Rejected lists events that failed validation. Retrying them won't help.
type BatchResponse struct {
	Accepted int         `json:"accepted"`
	Rejected []Rejection `json:"rejected,omitzero"`
}

// Rejection explains why one event of a batch was not accepted.
type Rejection struct {
	// Index is the position of the event in the request's events array.
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitzero"`
	Error   string `json:"error"`
}

// ErrorResponse is the body of every non-2xx response that is not a
// BatchResponse: 401, 413, 415, 503 and malformed requests.
type ErrorResponse struct {
	Error string `json:"error"`
}
