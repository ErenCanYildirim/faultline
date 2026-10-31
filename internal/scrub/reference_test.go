package scrub

import (
	"net"
	"strings"
)

// The scrubber as it was before the prefilters (decision 135), kept as the
// reference the faster version must match exactly. Same regexes, every one
// run on every string.

func refScrubText(v string, keepIPs bool) (string, int) {
	if v == "" {
		return v, 0
	}
	n := 0
	v = secretKeyValue.ReplaceAllStringFunc(v, func(m string) string {
		parts := secretKeyValue.FindStringSubmatch(m)
		if strings.HasPrefix(parts[3], Filtered) {
			return m
		}
		n++
		return parts[1] + parts[2] + Filtered
	})
	v = bearer.ReplaceAllStringFunc(v, func(m string) string {
		n++
		return strings.Fields(m)[0] + " " + Filtered
	})
	v = email.ReplaceAllStringFunc(v, func(string) string {
		n++
		return emailPlaceholder
	})
	v = cardCandidate.ReplaceAllStringFunc(v, func(m string) string {
		if !luhn(m) {
			return m
		}
		n++
		return cardPlaceholder
	})
	if !keepIPs {
		v = ipv4.ReplaceAllStringFunc(v, func(m string) string {
			host := m
			if i := strings.IndexByte(m, ':'); i >= 0 {
				host = m[:i]
			}
			if net.ParseIP(host) == nil {
				return m
			}
			n++
			return ipPlaceholder
		})
		var c int
		v, c = refReplaceIPv6(v)
		n += c
	}
	v, c := refScrubPath(v)
	return v, n + c
}

func refScrubPath(v string) (string, int) {
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

func refIsSecretKey(k string) bool {
	k = strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(k))
	for _, p := range secretKeyParts {
		if strings.Contains(k, p) {
			return true
		}
	}
	return false
}

func refReplaceIPv6(v string) (string, int) {
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
