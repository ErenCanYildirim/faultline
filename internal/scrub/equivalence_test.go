package scrub

import (
	"math/rand/v2"
	"strings"
	"testing"
)

// fragments cover every rule, near misses for each, case variations and
// the non-ASCII letters Go's case folding maps onto ASCII (ſ -> s,
// Kelvin sign -> k), which the prefilters must not wrongly skip.
var fragments = []string{
	// secrets by key, and near misses
	"password=hunter2", "PASSWORD: x", "db_passwd=abc", "pwd:1", `"api_key": "sk_live_1"`, "x-api-key=abc",
	"access_token=eyJ.abc.def", "Authorization: Bearer abc.def.ghi", "Cookie: sid=9f", "session_id=42",
	"private_key='k'", "token=abc}", "secret", "token", "apikey", "password", "=", ":", "pass word=x",
	"ſecret=abc", "toKen=abc", "API-KEY : v", `{"token": "[Filtered]"}`, "password=[Filtered]",
	// bearer
	"bearer eyJhbGciOiJIUzI1NiJ9", "Basic dXNlcjpwYXNz", "BEARER short", "bearer", "basically",
	// emails
	"jdoe@example.com", "a.b+tag@corp.example.org", "@", "user@", "@host.io", "x@y", "ana@x.io",
	// cards and numbers
	"4111111111111111", "4111 1111 1111 1111", "3782-822463-10005", "4111111111111112", "20261028112900",
	"1234567890123", "4222222222222", "4222 2222 22222", "123456789012", "42", "0", "999999999999999999999",
	// IPv4
	"203.0.113.7", "10.0.0.12:5432", "999.1.1.1", "1.2.3", "1.2.3.4.5", "v1.2.3.4", "192.168.1.20",
	// IPv6
	"2001:db8::8a2e:370:7334", "::1", "[::1]:8080", "fe80::1%eth0", "10:27:12", "std::vector", "Foo::bar",
	"a:b", "::", "aa:bb:cc", "2001:DB8::1",
	// home directories
	"C:/Users/jdoe/x", `C:\Users\jdoe\x`, "/home/jdoe/.config", "/Users/jane.doe/src", "/users/x/y",
	"C:/Documents and Settings/bob/x", "/var/data/x", "users", "/home/", "C:/Users/<user>/x", "/hoMe/x/y",
	"C:/Uſers/bob/x",
	// filler
	"checkout failed", "order", "after 1830ms", "bug aab:", "run-1029-100658-okfm", "app-007", "ü", "日本",
	",", ";", "&", "{", "}", "(", ")", `"`, "'", "\t", "\n",
}

func randomText(r *rand.Rand) string {
	n := 1 + r.IntN(6)
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fragments[r.IntN(len(fragments))]
	}
	seps := []string{" ", "", ", ", ":", "=", "/", " - "}
	return strings.Join(parts, seps[r.IntN(len(seps))])
}

// The prefiltered scrubber must produce exactly what running every regex
// produced, for every input. Any difference is a bug in a prefilter.
func TestPrefiltersMatchReference(t *testing.T) {
	r := rand.New(rand.NewPCG(42, 7))
	inputs := append([]string(nil), fragments...)
	// 50,000 random combinations take a few seconds; -short keeps 5,000.
	n := 50_000
	if testing.Short() {
		n = 5_000
	}
	for range n {
		inputs = append(inputs, randomText(r))
	}
	changedByRef := 0
	for _, in := range inputs {
		for _, keepIPs := range []bool{false, true} {
			want, wantN := refScrubText(in, keepIPs)
			got, gotN := scrubText(in, keepIPs)
			if got != want || gotN != wantN {
				t.Fatalf("scrubText(%q, keepIPs=%v)\n got  %q (%d)\n want %q (%d)", in, keepIPs, got, gotN, want, wantN)
			}
			if want != in {
				changedByRef++
			}
		}
		if got, want := isSecretKey(in), refIsSecretKey(in); got != want {
			t.Fatalf("isSecretKey(%q) = %v, want %v", in, got, want)
		}
		gotP, gotPN := scrubPath(in)
		wantP, wantPN := refScrubPath(in)
		if gotP != wantP || gotPN != wantPN {
			t.Fatalf("scrubPath(%q) = %q (%d), want %q (%d)", in, gotP, gotPN, wantP, wantPN)
		}
	}
	// The corpus must actually exercise the rules, or matching outputs
	// prove nothing.
	if share := float64(changedByRef) / float64(2*len(inputs)); share < 0.3 {
		t.Errorf("only %.0f%% of inputs had something to scrub; the corpus is too weak", share*100)
	}
}

func TestFoldHelpers(t *testing.T) {
	if !containsAnyFold("Authorization: BEARER x", "bearer") || containsAnyFold("bear", "bearer") || containsAnyFold("", "a") {
		t.Error("containsAnyFold")
	}
	if !isASCII("plain") || isASCII("ſecret") {
		t.Error("isASCII")
	}
	if countDigits("a1b22c333") != 6 {
		t.Error("countDigits")
	}
}

// With the prefilters switched off, every regex runs again: same output.
func TestPrefiltersOffMatchesReference(t *testing.T) {
	SetPrefilters(false)
	defer SetPrefilters(true)
	r := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 2000; i++ {
		in := randomText(r)
		for _, keepIPs := range []bool{false, true} {
			want, wantN := refScrubText(in, keepIPs)
			if got, gotN := scrubText(in, keepIPs); got != want || gotN != wantN {
				t.Fatalf("prefilters off: scrubText(%q) = %q (%d), want %q (%d)", in, got, gotN, want, wantN)
			}
		}
	}
}
