package codegen

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// Generated Python carries contract defaults as Python literals and generated
// TypeScript as JSON; both must match what Python's repr() and json.dumps()
// print so that output is identical across generator implementations.

// pyRepr renders a decoded JSON value the way Python's repr() would.
func pyRepr(value any) string {
	switch v := value.(type) {
	case nil:
		return "None"
	case bool:
		if v {
			return "True"
		}
		return "False"
	case string:
		return pyStringRepr(v)
	case json.Number:
		return pyNumber(v)
	case []any:
		parts := make([]string, len(v))
		for i, item := range v {
			parts[i] = pyRepr(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *contract.Object:
		parts := make([]string, 0, len(v.Keys))
		for _, k := range v.Keys {
			parts = append(parts, pyRepr(k)+": "+pyRepr(v.Values[k]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprint(value)
}

// pyNumber keeps integer literals verbatim and formats floats like Python's repr.
func pyNumber(n json.Number) string {
	s := string(n)
	if !strings.ContainsAny(s, ".eE") {
		return s
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return s
	}
	return pyFloatRepr(f)
}

// pyFloatRepr: shortest round-trip digits, fixed notation for exponents in
// [-4, 16), otherwise d.ddde±XX, always with a decimal point in fixed form.
func pyFloatRepr(f float64) string {
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	if math.IsNaN(f) {
		return "nan"
	}
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	e := strconv.FormatFloat(f, 'e', -1, 64) // d.ddddde±XX
	mantissa, expText, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(expText)
	neg := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(mantissa, "-")
	digits := strings.Replace(mantissa, ".", "", 1)
	sign := ""
	if neg {
		sign = "-"
	}
	if exp >= -4 && exp < 16 {
		var out string
		switch {
		case exp < 0:
			out = "0." + strings.Repeat("0", -exp-1) + digits
		case exp+1 >= len(digits):
			out = digits + strings.Repeat("0", exp+1-len(digits)) + ".0"
		default:
			out = digits[:exp+1] + "." + digits[exp+1:]
		}
		return sign + out
	}
	m := digits[:1]
	if len(digits) > 1 {
		m += "." + digits[1:]
	}
	expSign := "+"
	if exp < 0 {
		expSign = "-"
		exp = -exp
	}
	return fmt.Sprintf("%s%se%s%02d", sign, m, expSign, exp)
}

// pyStringRepr follows CPython: single quotes unless the text contains a
// single quote and no double quote; non-printable characters are escaped.
func pyStringRepr(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, "\"") {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString("\\n")
		case r == '\r':
			b.WriteString("\\r")
		case r == '\t':
			b.WriteString("\\t")
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, "\\x%02x", r)
		case r > 0x7f && !unicode.IsPrint(r):
			if r > 0xffff {
				fmt.Fprintf(&b, "\\U%08x", r)
			} else {
				fmt.Fprintf(&b, "\\u%04x", r)
			}
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}

// jsonDumps renders a value like Python's json.dumps(value): ASCII-escaped
// strings, ", " and ": " separators, Python float formatting.
func jsonDumps(value any) string {
	var b strings.Builder
	jsonDumpInto(&b, value)
	return b.String()
}

func jsonDumpInto(b *strings.Builder, value any) {
	switch v := value.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if v {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		jsonString(b, v)
	case json.Number:
		b.WriteString(pyNumber(v))
	case []any:
		b.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				b.WriteString(", ")
			}
			jsonDumpInto(b, item)
		}
		b.WriteByte(']')
	case *contract.Object:
		b.WriteByte('{')
		for i, k := range v.Keys {
			if i > 0 {
				b.WriteString(", ")
			}
			jsonString(b, k)
			b.WriteString(": ")
			jsonDumpInto(b, v.Values[k])
		}
		b.WriteByte('}')
	default:
		fmt.Fprint(b, v)
	}
}

func jsonString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString("\\\"")
		case '\\':
			b.WriteString("\\\\")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		case '\b':
			b.WriteString("\\b")
		case '\f':
			b.WriteString("\\f")
		default:
			switch {
			case r < 0x20:
				fmt.Fprintf(b, "\\u%04x", r)
			case r > 0x7e:
				if r > 0xffff {
					hi, lo := utf16.EncodeRune(r)
					fmt.Fprintf(b, "\\u%04x\\u%04x", hi, lo)
				} else {
					fmt.Fprintf(b, "\\u%04x", r)
				}
			default:
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}
