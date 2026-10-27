package faultline

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// newEventID returns a UUIDv7 (RFC 9562): 48 bits of Unix milliseconds,
// then random bits. Time-ordered IDs sort close together in ClickHouse.
//
// Written here instead of importing a UUID library so the SDK adds no
// third-party dependencies to the host application.
func newEventID() string {
	var b [16]byte
	var ms [8]byte
	binary.BigEndian.PutUint64(ms[:], uint64(time.Now().UnixMilli()))
	copy(b[0:6], ms[2:8])
	_, _ = rand.Read(b[6:])     // never fails since Go 1.24
	b[6] = (b[6] & 0x0f) | 0x70 // version 7
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 9562 variant

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
