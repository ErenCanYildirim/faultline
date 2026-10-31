// Package scrub removes personal data and secrets from events before they
// are grouped and stored.
//
// It runs in the worker, before grouping, so nothing it removes reaches a
// stored row, an issue title or a fingerprint.
//
// What it removes, in this order:
//
//  1. Values of secret-looking keys in text: password=..., "token": "...",
//     Authorization: Bearer ... (the key stays, the value becomes [Filtered]).
//  2. Bearer and Basic credentials anywhere in text.
//  3. Email addresses.
//  4. Credit card numbers, only when they pass the Luhn check, so order
//     numbers and timestamps are left alone.
//  5. IPv4 and IPv6 addresses, unless the project allows storing them.
//  6. The user name in home directory paths: C:/Users/jdoe/..., /home/jdoe/...
//
// Emails and IPv4 addresses are replaced with exactly the placeholders and
// patterns the grouping normalizer uses (<email>, <ip>). Turning scrubbing on
// therefore leaves the fingerprints of existing issues unchanged; a test
// checks this. Running the scrubber twice gives the same result as once.
package scrub

import (
	"net"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/erencanyildirim/faultline/event"
)

// Filtered replaces the value of a secret.
const Filtered = "[Filtered]"

// Placeholders. Email and IP match internal/grouping, see the package doc.
const (
	emailPlaceholder = "<email>"
	ipPlaceholder    = "<ip>"
	cardPlaceholder  = "<card>"
	userPlaceholder  = "<user>"
)

// Settings are per-project choices.
type Settings struct {
	// StoreIPAddresses keeps IP addresses in user.ip_address and in text.
	// Off by default: an IP address can identify a person.
	StoreIPAddresses bool `json:"store_ip_addresses"`
}

// Scrubber holds per-project settings. It is safe for concurrent use.
type Scrubber struct {
	projects map[string]Settings
}

// New creates a scrubber. Projects not in the map get the zero Settings,
// which scrub everything.
func New(projects map[string]Settings) *Scrubber {
	if projects == nil {
		projects = map[string]Settings{}
	}
	return &Scrubber{projects: projects}
}

// Event scrubs e in place and returns how many values it replaced.
func (s *Scrubber) Event(e *event.Event) int {
	keepIPs := s.projects[e.ProjectID].StoreIPAddresses
	n := 0
	text := func(v string) string {
		out, c := scrubText(v, keepIPs)
		n += c
		return out
	}

	e.Message = text(e.Message)
	if ex := e.Exception; ex != nil {
		ex.Value = text(ex.Value)
		for i := range ex.Frames {
			// Only the path: module and function names are code, not data.
			f, c := scrubPath(ex.Frames[i].File)
			ex.Frames[i].File = f
			n += c
		}
	}

	for k, v := range e.Tags {
		if isSecretKey(k) {
			if v != Filtered {
				e.Tags[k] = Filtered
				n++
			}
			continue
		}
		e.Tags[k] = text(v)
	}

	if u := e.User; u != nil {
		// user.id goes through the same rules as any text. An opaque ID
		// (u_1029) passes unchanged and stays usable for counting affected
		// users; an email used as ID becomes <email>, so apps that do that
		// lose per-user counts rather than leak addresses.
		u.ID = text(u.ID)
		u.Username = text(u.Username)
		u.Email = text(u.Email)
		if u.IPAddress != "" && !keepIPs {
			u.IPAddress = ""
			n++
		}
	}
	return n
}

// Text scrubs one string with the default settings (IPs removed). It is
// exported for tests and tools.
func Text(v string) string {
	out, _ := scrubText(v, false)
	return out
}

var (
	// key=value, key: value, "key": "value" for secret-looking keys. The key
	// may be part of a longer name: access_token, x-api-key, db_password.
	// The value may be quoted, or a Bearer/Basic scheme plus credential.
	// An unquoted value ends at whitespace, a separator or a closing bracket,
	// so "token=abc}" keeps its brace.
	secretKeyValue = regexp.MustCompile(`(?i)([a-z0-9_-]*(?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|authorization|cookie|session[_-]?id|private[_-]?key)[a-z0-9_-]*)("?\s*[=:]\s*)((?:(?:bearer|basic)\s+)?(?:"[^"]*"|'[^']*'|[^\s,;&"'})]+))`)

	bearer = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`)

	// Same pattern as grouping's normalizer.
	email = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)

	// 13 to 19 digits, optionally separated by single spaces or dashes.
	cardCandidate = regexp.MustCompile(`\b\d(?:[ -]?\d){12,18}\b`)

	// Same pattern as grouping's normalizer, port included, so a scrubbed
	// "10.0.0.1:5432" normalizes exactly like the original.
	ipv4 = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`)

	// Candidates only; net.ParseIP decides. Times like 10:27:12 fail it.
	ipv6Candidate = regexp.MustCompile(`(?i)(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}`)

	// The segment after a home directory root is a user name.
	homeDir = regexp.MustCompile(`(?i)([a-z]:[\\/](?:users|documents and settings)[\\/]|/home/|/users/)([^\\/\s"'<>:]+)`)
)

// prefiltersOff disables the cheap checks below, so every regex runs on
// every string as before decision 135. Off gives the same output, only
// slower. It exists for A/B benchmarks on one binary, and as a switch back
// to the plain regexes if a prefilter is ever suspected of a bug.
var prefiltersOff atomic.Bool

// SetPrefilters turns the cheap checks before each regex on (the default)
// or off. Safe to call at any time.
func SetPrefilters(on bool) { prefiltersOff.Store(!on) }

// Each rule below first checks for something its regex cannot match
// without: an @ for an email, 13 digits for a card, three dots for an IPv4
// address. Most strings in an event (tag values, user IDs, ordinary
// messages) have none of these, and the check costs a fraction of a regex
// scan. Measured: the scrubber took over half of the worker's time, almost
// all of it in regexes that could not match (decision 135).
//
// Every check is a necessary condition of its regex, so the result is
// exactly the same as running every regex; a test compares the two on the
// fixtures and on generated strings.
func scrubText(v string, keepIPs bool) (string, int) {
	if v == "" {
		return v, 0
	}
	n := 0
	all := prefiltersOff.Load() // run every regex, no checks

	if all || maySecretKeyValue(v) {
		v = secretKeyValue.ReplaceAllStringFunc(v, func(m string) string {
			parts := secretKeyValue.FindStringSubmatch(m)
			if strings.HasPrefix(parts[3], Filtered) { // already scrubbed
				return m
			}
			n++
			return parts[1] + parts[2] + Filtered
		})
	}
	if all || !isASCII(v) || containsAnyFold(v, "bearer", "basic") {
		v = bearer.ReplaceAllStringFunc(v, func(m string) string {
			n++
			return strings.Fields(m)[0] + " " + Filtered
		})
	}
	if all || strings.IndexByte(v, '@') >= 0 {
		v = email.ReplaceAllStringFunc(v, func(string) string {
			n++
			return emailPlaceholder
		})
	}
	if all || countDigits(v) >= 13 {
		v = cardCandidate.ReplaceAllStringFunc(v, func(m string) string {
			if !luhn(m) {
				return m
			}
			n++
			return cardPlaceholder
		})
	}
	if !keepIPs && (all || strings.Count(v, ".") >= 3) {
		v = ipv4.ReplaceAllStringFunc(v, func(m string) string {
			host := m
			if i := strings.IndexByte(m, ':'); i >= 0 {
				host = m[:i]
			}
			if net.ParseIP(host) == nil { // 999.1.1.1 is a version string, not an address
				return m
			}
			n++
			return ipPlaceholder
		})
	}
	if !keepIPs {
		var c int
		v, c = replaceIPv6(v)
		n += c
	}
	v, c := scrubPath(v)
	return v, n + c
}

// maySecretKeyValue: the key=value rule needs a separator and one of the
// key words. The regex is case-insensitive, and Go's case folding also maps
// two non-ASCII letters onto ASCII ones (ſ to s, the Kelvin sign to k), so
// for non-ASCII text the check steps aside and the regex decides.
func maySecretKeyValue(v string) bool {
	if strings.IndexByte(v, '=') < 0 && strings.IndexByte(v, ':') < 0 {
		return false
	}
	return !isASCII(v) || containsAnyFold(v, "password", "passwd", "pwd", "secret", "token",
		"api", "access", "authorization", "cookie", "session", "private")
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func countDigits(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			n++
		}
	}
	return n
}

// containsAnyFold reports whether s contains any of subs, ignoring ASCII
// case. subs must be lowercase. It doesn't allocate, unlike ToLower.
func containsAnyFold(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			j := 0
			for ; j < len(sub); j++ {
				c := s[i+j]
				if 'A' <= c && c <= 'Z' {
					c += 'a' - 'A'
				}
				if c != sub[j] {
					break
				}
			}
			if j == len(sub) {
				return true
			}
		}
	}
	return false
}

// replaceIPv6 replaces IPv6 addresses. A candidate counts only if it stands
// alone (no letter, digit or colon right before or after it), has at least
// one hex digit, and parses as IPv6. Without the boundary check, "d::" in
// "std::vector" would parse as an address.
func replaceIPv6(v string) (string, int) {
	if !prefiltersOff.Load() && strings.Count(v, ":") < 2 { // the pattern needs at least two colons
		return v, 0
	}
	locs := ipv6Candidate.FindAllStringIndex(v, -1)
	if locs == nil {
		return v, 0
	}
	var b strings.Builder
	n, last := 0, 0
	for _, loc := range locs {
		start, end := loc[0], loc[1]
		m := v[start:end]
		standalone := (start == 0 || !isAddrChar(v[start-1])) && (end == len(v) || !isAddrChar(v[end]))
		ip := net.ParseIP(m)
		if !standalone || ip == nil || ip.To4() != nil || !strings.ContainsAny(m, "0123456789abcdefABCDEF") {
			continue
		}
		b.WriteString(v[last:start])
		b.WriteString(ipPlaceholder)
		last = end
		n++
	}
	if n == 0 {
		return v, 0
	}
	b.WriteString(v[last:])
	return b.String(), n
}

func isAddrChar(c byte) bool {
	return c == ':' || c == '.' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func scrubPath(v string) (string, int) {
	// The pattern needs /home/, or "users" or "documents and settings"
	// after a drive letter or slash. Non-ASCII: let the regex decide, as in
	// maySecretKeyValue.
	if !prefiltersOff.Load() && isASCII(v) && !containsAnyFold(v, "users", "documents and settings", "/home/") {
		return v, 0
	}
	n := 0
	v = homeDir.ReplaceAllStringFunc(v, func(m string) string {
		parts := homeDir.FindStringSubmatch(m)
		if parts[2] == userPlaceholder {
			return m
		}
		n++
		return parts[1] + userPlaceholder
	})
	return v, n
}

// luhn reports whether the digits in s pass the Luhn checksum used by card
// numbers. Separators are ignored.
func luhn(s string) bool {
	sum, digits := 0, 0
	double := false
	for i := len(s) - 1; i >= 0; i-- {
		c := s[i]
		if c < '0' || c > '9' {
			continue
		}
		d := int(c - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
		digits++
	}
	return digits >= 13 && sum%10 == 0
}

// secretKeyParts are matched against tag keys lowercased with - and _
// removed, so "X-Api-Key", "api_key" and "apikey" all match "apikey".
var secretKeyParts = []string{
	"password", "passwd", "pwd", "secret", "token", "apikey", "accesskey",
	"authorization", "cookie", "sessionid", "privatekey", "credential",
}

// keySeparators is built once: building a Replacer on every call was 4% of
// the worker's CPU in the baseline profile.
var keySeparators = strings.NewReplacer("-", "", "_", "")

func isSecretKey(k string) bool {
	k = keySeparators.Replace(strings.ToLower(k))
	for _, p := range secretKeyParts {
		if strings.Contains(k, p) {
			return true
		}
	}
	return false
}
