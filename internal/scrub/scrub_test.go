package scrub

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/erencanyildirim/faultline/event"
	"github.com/erencanyildirim/faultline/internal/grouping"
)

func TestText(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		// Secrets by key
		{"key=value", "login failed: password=hunter2 for admin", "login failed: password=[Filtered] for admin"},
		{"key: value", "db_password: s3cr3t!", "db_password: [Filtered]"},
		{"json style", `bad config {"api_key": "sk_live_abc123"}`, `bad config {"api_key": [Filtered]}`},
		{"compound key", "refresh failed access_token=eyJhbGciOi.abc.def&state=x", "refresh failed access_token=[Filtered]&state=x"},
		{"header with scheme", "Authorization: Bearer abc.def.ghi rejected", "Authorization: [Filtered] rejected"},
		{"cookie", "Cookie: sid=9f8e7d6c", "Cookie: [Filtered]"},
		{"closing brace kept", "params {token=abc} rejected", "params {token=[Filtered]} rejected"},
		{"bearer anywhere", "upstream said: bearer eyJhbGciOiJIUzI1NiJ9 expired", "upstream said: bearer [Filtered] expired"},

		// Emails
		{"email", "mail to jdoe@example.com bounced", "mail to <email> bounced"},
		{"two emails", "a@b.io and c.d+tag@corp.example.org", "<email> and <email>"},

		// Cards: Luhn decides
		{"visa", "charge 4111111111111111 declined", "charge <card> declined"},
		{"spaced card", "card 4111 1111 1111 1111 expired", "card <card> expired"},
		{"dashed amex", "amex 3782-822463-10005 ok", "amex <card> ok"},
		{"order number fails luhn", "order 4111111111111112 shipped", "order 4111111111111112 shipped"},
		{"timestamp fails luhn", "batch 20261028112900 done", "batch 20261028112900 done"},
		{"short number", "user 42 not found", "user 42 not found"},

		// IPs
		{"ipv4", "request from 203.0.113.7 denied", "request from <ip> denied"},
		{"ipv4 with port", "dial tcp 10.0.0.12:5432: refused", "dial tcp <ip>: refused"},
		{"not an ip", "version 999.1.1.1 released", "version 999.1.1.1 released"},
		{"ipv6", "client 2001:db8::8a2e:370:7334 blocked", "client <ip> blocked"},
		{"ipv6 loopback in brackets", "listen [::1]:8080", "listen [<ip>]:8080"},
		{"time is not ipv6", "at 10:27:12 the job failed", "at 10:27:12 the job failed"},
		{"scope operator is not ipv6", "panic in std::vector and Foo::bar", "panic in std::vector and Foo::bar"},

		// Home directories
		{"windows path", "open C:/Users/jdoe/Documents/app/cart.json: denied", "open C:/Users/<user>/Documents/app/cart.json: denied"},
		{"windows backslashes", `open C:\Users\jdoe\AppData\x.db`, `open C:\Users\<user>\AppData\x.db`},
		{"linux home", "stat /home/jdoe/.config/app.yaml: no such file", "stat /home/<user>/.config/app.yaml: no such file"},
		{"mac home", "/Users/jane.doe/src/app/main.go", "/Users/<user>/src/app/main.go"},
		{"system path untouched", "open /var/data/cart-8812.json: no such file", "open /var/data/cart-8812.json: no such file"},

		// Nothing to scrub
		{"clean", "checkout: load cart 8812: file does not exist", "checkout: load cart 8812: file does not exist"},
		{"empty", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Text(tc.in); got != tc.want {
				t.Errorf("Text(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}

// A redelivered event is scrubbed again; the result must not drift.
func TestScrubbingTwiceEqualsOnce(t *testing.T) {
	inputs := []string{
		"password=hunter2 Authorization: Bearer abc.def.ghi user jdoe@example.com",
		"card 4111 1111 1111 1111 from 10.0.0.1:443 and ::1 at C:/Users/jdoe/x",
		`{"token": "abc", "secret": 'x'} /home/jdoe/y`,
	}
	for _, in := range inputs {
		once := Text(in)
		if twice := Text(once); twice != once {
			t.Errorf("not idempotent:\n once  %q\n twice %q", once, twice)
		}
	}
}

// Turning scrubbing on must not split existing issues: for the patterns the
// grouping normalizer also handles, the fingerprint of a scrubbed event
// equals the fingerprint of the original.
func TestScrubbingKeepsFingerprints(t *testing.T) {
	messages := []string{
		"invite to jdoe@example.com failed",
		"connection reset by 203.0.113.7",
		"dial tcp 10.0.0.12:5432: connection refused",
		"user ana@x.io from 192.168.1.20 locked out",
	}
	for _, m := range messages {
		for _, e := range []*event.Event{
			{Level: event.LevelWarning, Message: m},
			{Level: event.LevelError, Exception: &event.Exception{Type: "*errors.errorString", Value: m,
				Frames: []event.Frame{{Module: "shop/api", Function: "handle", InApp: true}}}},
		} {
			before := grouping.Fingerprint(e)
			New(nil).Event(e)
			after := grouping.Fingerprint(e)
			if before.Fingerprint != after.Fingerprint {
				t.Errorf("scrubbing changed the fingerprint of %q\n before %s\n after  %s", m, before.Explain(), after.Explain())
			}
		}
	}
}

func loadEvent(t *testing.T, path string) event.Event {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var e event.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestEventFixture(t *testing.T) {
	dirty := loadEvent(t, "testdata/dirty.json")
	want := loadEvent(t, "testdata/clean.json")

	n := New(nil).Event(&dirty)
	if !reflect.DeepEqual(dirty, want) {
		got, _ := json.MarshalIndent(dirty, "", "  ")
		t.Errorf("scrubbed event differs from testdata/clean.json; got:\n%s", got)
	}
	// message 2 (email, card), exception value 2 (api_key, ip), frame path 1,
	// tags 2 (x-api-key, customer email), user 3 (username, email, ip).
	if n != 10 {
		t.Errorf("replaced %d values, want 10", n)
	}
}

func TestProjectMayKeepIPAddresses(t *testing.T) {
	s := New(map[string]Settings{"keeps-ips": {StoreIPAddresses: true}})
	mk := func(project string) *event.Event {
		return &event.Event{
			ProjectID: project,
			Message:   "login from 203.0.113.7 by jdoe@example.com",
			User:      &event.User{ID: "u1", IPAddress: "203.0.113.7"},
		}
	}

	kept := mk("keeps-ips")
	s.Event(kept)
	if kept.User.IPAddress != "203.0.113.7" || kept.Message != "login from 203.0.113.7 by <email>" {
		t.Errorf("project with store_ip_addresses: %+v %+v", kept.Message, kept.User)
	}

	for _, project := range []string{"demo", "unknown-project"} {
		e := mk(project)
		s.Event(e)
		if e.User.IPAddress != "" || e.Message != "login from <ip> by <email>" {
			t.Errorf("%s: IPs must be scrubbed by default: %q %+v", project, e.Message, e.User)
		}
		if e.User.ID != "u1" {
			t.Errorf("%s: user.id must be kept", project)
		}
	}
}

func TestUserFields(t *testing.T) {
	e := &event.Event{User: &event.User{
		ID:        "jdoe@example.com", // apps sometimes use the email as the ID
		Username:  "jdoe",
		Email:     "jdoe@example.com",
		IPAddress: "2001:db8::1",
	}}
	New(nil).Event(e)
	want := event.User{ID: "<email>", Username: "jdoe", Email: "<email>"}
	if *e.User != want {
		t.Errorf("user = %+v\nwant   %+v", *e.User, want)
	}

	opaque := &event.Event{User: &event.User{ID: "u_1029"}}
	New(nil).Event(opaque)
	if opaque.User.ID != "u_1029" {
		t.Errorf("an opaque user ID must pass unchanged, got %q", opaque.User.ID)
	}
}

func TestSecretTagKeys(t *testing.T) {
	e := &event.Event{Tags: map[string]string{
		"X-Api-Key":  "sk_live_1",
		"auth_token": "abc",
		"Password":   "x",
		"route":      "/checkout",
		"author":     "jdoe@example.com", // not a secret key, but the value is an email
	}}
	New(nil).Event(e)
	want := map[string]string{
		"X-Api-Key": Filtered, "auth_token": Filtered, "Password": Filtered,
		"route": "/checkout", "author": "<email>",
	}
	if !reflect.DeepEqual(e.Tags, want) {
		t.Errorf("tags = %v\nwant   %v", e.Tags, want)
	}
}

func TestLuhn(t *testing.T) {
	valid := []string{"4111111111111111", "5500 0000 0000 0004", "3782-822463-10005", "6011000990139424"}
	invalid := []string{"4111111111111112", "1234567890123", "0000"}
	for _, s := range valid {
		if !luhn(s) {
			t.Errorf("%s should pass Luhn", s)
		}
	}
	for _, s := range invalid {
		if luhn(s) {
			t.Errorf("%s should fail Luhn", s)
		}
	}
}

func TestParseSettings(t *testing.T) {
	got, err := ParseSettings([]byte(`{"projects":[
		{"id":"demo","keys":["k1"]},
		{"id":"internal","keys":["k2"],"store_ip_addresses":true}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got["demo"].StoreIPAddresses || !got["internal"].StoreIPAddresses {
		t.Errorf("settings = %+v", got)
	}
	if _, err := ParseSettings([]byte(`{"projects":[{"keys":["k"]}]}`)); err == nil || !strings.Contains(err.Error(), "empty id") {
		t.Errorf("empty id: err = %v", err)
	}
}
