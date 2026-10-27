package event

import "time"

// Hard limits of the wire contract.
//
// Policy:
//   - The SDK enforces these before sending by truncating, so a well-behaved
//     client never has an event rejected for size.
//   - Ingest enforces them again (step 1.4) because clients can't be trusted.
//     An event that still breaks a limit is rejected on its own; the other
//     events in the same batch are still accepted.
//
// Sizes are in bytes of UTF-8 text, measured before compression.
const (
	// MaxEventBytes caps one event encoded as JSON. Keeps queue messages small
	// and well under Redpanda's default 1 MiB message limit.
	MaxEventBytes = 64 << 10 // 64 KiB

	// MaxBatchEvents caps the number of events in one request.
	MaxBatchEvents = 100

	// MaxBatchBytes caps one request body after gzip decompression. A batch
	// must respect both this and MaxBatchEvents; the SDK splits accordingly.
	MaxBatchBytes = 1 << 20 // 1 MiB

	// MaxMessageBytes caps Event.Message.
	MaxMessageBytes = 8 << 10 // 8 KiB

	// MaxExceptionTypeBytes caps Exception.Type.
	MaxExceptionTypeBytes = 256

	// MaxExceptionValueBytes caps Exception.Value.
	MaxExceptionValueBytes = 8 << 10 // 8 KiB

	// MaxFrames caps Exception.Frames. The SDK keeps the innermost frames,
	// because that is where the error happened and what grouping uses.
	MaxFrames = 64

	// MaxFrameFieldBytes caps Frame.Module, Frame.Function and Frame.File each.
	MaxFrameFieldBytes = 512

	// MaxTags caps the number of entries in Event.Tags.
	MaxTags = 32

	// MaxTagKeyBytes and MaxTagValueBytes cap each tag entry.
	MaxTagKeyBytes   = 32
	MaxTagValueBytes = 200

	// MaxUserFieldBytes caps each field of User.
	MaxUserFieldBytes = 256

	// MaxReleaseBytes and MaxEnvironmentBytes cap the release and environment names.
	MaxReleaseBytes     = 128
	MaxEnvironmentBytes = 64

	// MaxSDKFieldBytes caps SDKInfo.Name and SDKInfo.Version.
	MaxSDKFieldBytes = 64
)

// Accepted window for Event.Timestamp relative to ReceivedAt. Ingest replaces
// a timestamp outside this window with ReceivedAt (step 1.4) instead of
// rejecting the event: a wrong client clock should not lose an error report.
const (
	// MaxTimestampAge covers an SDK that buffered events while the backend
	// was down. It is well below the 30-day storage TTL.
	MaxTimestampAge = 7 * 24 * time.Hour

	// MaxTimestampFuture allows for ordinary client clock drift.
	MaxTimestampFuture = 5 * time.Minute
)
