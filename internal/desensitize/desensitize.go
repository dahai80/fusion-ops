package desensitize

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// Desensitize v1 (flywheel plan §3, regex layer only — deterministic, Rule 5).
// Applies the standard PII scrubbing pipeline to free text before it is
// persisted or served. Hard-forbidden payloads (passwords, raw sessions,
// captcha answers, payment credentials, ID photos) must never reach this
// layer; emitters enforce that upstream.

var (
	emailRe   = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	phoneRe   = regexp.MustCompile(`\b(?:\+?86[- ]?)?1[3-9]\d{9}\b|\b\d{3,4}-\d{7,8}\b`)
	idcardRe  = regexp.MustCompile(`\b\d{17}[\dXx]\b|\b\d{15}\b`)
	secretRe  = regexp.MustCompile(`(?:sk-[A-Za-z0-9_\-]{8,}|eyJ[A-Za-z0-9_\-]{8,}|[A-Fa-f0-9]{32,}|[A-Za-z0-9+/]{32,}={0,2})`)
	ipRe      = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
	credURlRe = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.\-]*://)[^/@\s:]+:[^/@\s]+@`)
)

// luhn validates digit strings (credit card candidates).
func luhn(digits string) bool {
	sum := 0
	alt := false
	count := 0
	for i := len(digits) - 1; i >= 0; i-- {
		c := digits[i]
		if c < '0' || c > '9' {
			return false
		}
		d := int(c - '0')
		if alt {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		alt = !alt
		count++
	}
	return count >= 13 && count <= 19 && sum%10 == 0
}

var cardRe = regexp.MustCompile(`\b[0-9]{13,19}\b`)

func hashPrefix(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])[:8]
}

// Text scrubs PII from free text per desensitize standard v1.
func Text(s string) string {
	if s == "" {
		return s
	}
	// Strip credentials embedded in URLs first (user:pass@host).
	s = credURlRe.ReplaceAllString(s, "${1}[CRED]@")
	s = emailRe.ReplaceAllString(s, "[EMAIL]")
	s = idcardRe.ReplaceAllString(s, "[IDCARD]")
	s = cardRe.ReplaceAllStringFunc(s, func(m string) string {
		if luhn(m) {
			return "[CARD]"
		}
		return m
	})
	s = phoneRe.ReplaceAllString(s, "[PHONE]")
	s = secretRe.ReplaceAllString(s, "[SECRET]")
	s = ipRe.ReplaceAllString(s, "[IP]")
	return s
}

// Tenant hashes a tenant/user name to the standard 8-hex-prefix form.
func Tenant(name string) string {
	return fmt.Sprintf("[TENANT:%s]", hashPrefix(name))
}

// User hashes a username to the standard form.
func User(name string) string {
	return fmt.Sprintf("[USER:%s]", hashPrefix(name))
}

// HasPII reports whether text still contains PII shapes after scrubbing —
// used by tests and the sampling gate to prove outputs are clean.
func HasPII(s string) bool {
	scrubbed := Text(s)
	return scrubbed != s
}

// Join is a small helper for building signature strings.
func Join(parts ...string) string {
	return strings.Join(parts, "|")
}
