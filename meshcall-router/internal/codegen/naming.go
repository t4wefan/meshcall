package codegen

import (
	"regexp"
	"strings"
	"unicode"
)

// Naming helpers shared by the single-file targets: contract names split into
// words on anything that is not a letter or digit, then joined in camel case.
// A target passes its own initialisms (Swift writes "sessionID", Kotlin's
// conventions "sessionId").

var camelSeparators = regexp.MustCompile(`[^A-Za-z0-9]+`)

func camelWords(value string) []string {
	var words []string
	for _, part := range camelSeparators.Split(value, -1) {
		if part != "" {
			words = append(words, part)
		}
	}
	return words
}

// camelCase joins value's words: UpperCamelCase when upper ("tool_part" →
// "ToolPart"), else lowerCamelCase ("session_id" → "sessionID"); a word in
// initialisms is written as given, a name already in camel case is kept, and
// one starting with a digit gets a leading "_".
func camelCase(value, fallback string, upper bool, initialisms map[string]string) string {
	var b strings.Builder
	for i, word := range camelWords(value) {
		if i == 0 && !upper {
			if isUpper(word) {
				b.WriteString(strings.ToLower(word))
			} else {
				runes := []rune(word)
				runes[0] = unicode.ToLower(runes[0])
				b.WriteString(string(runes))
			}
			continue
		}
		if spelled, ok := initialisms[strings.ToLower(word)]; ok {
			b.WriteString(spelled)
			continue
		}
		runes := []rune(word)
		runes[0] = unicode.ToUpper(runes[0])
		b.WriteString(string(runes))
	}
	out := b.String()
	if out == "" {
		return fallback
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "_" + out
	}
	return out
}

func isUpper(word string) bool {
	for _, r := range word {
		if unicode.IsLower(r) {
			return false
		}
	}
	return true
}

// serviceBaseName is the service's last name segment that is not a version:
// "demo.v1.Greeting" → "Greeting".
func serviceBaseName(name string) string {
	parts := strings.Split(name, ".")
	for i := len(parts) - 1; i >= 0; i-- {
		if !majorVersion.MatchString(parts[i]) && !numericPart.MatchString(parts[i]) {
			return parts[i]
		}
	}
	return parts[0]
}
