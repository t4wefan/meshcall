// Package contract reads MeshCall portable contract documents.
//
// JSON Schemas keep their key order (Pydantic emits properties in declaration
// order and the generators rely on it), so schemas are decoded into an
// order-preserving Object rather than a map.
package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Object is a JSON object whose key order is preserved.
type Object struct {
	Keys   []string
	Values map[string]any
}

// Get returns the value for key and whether it is present.
func (o *Object) Get(key string) (any, bool) {
	if o == nil {
		return nil, false
	}
	v, ok := o.Values[key]
	return v, ok
}

// Has reports whether key is present.
func (o *Object) Has(key string) bool { _, ok := o.Get(key); return ok }

// Str returns the string at key, or "" when absent or not a string.
func (o *Object) Str(key string) string {
	v, _ := o.Get(key)
	s, _ := v.(string)
	return s
}

// Obj returns the nested object at key, or nil.
func (o *Object) Obj(key string) *Object {
	v, _ := o.Get(key)
	n, _ := v.(*Object)
	return n
}

// Strings returns the string elements of the array at key.
func (o *Object) Strings(key string) []string {
	v, _ := o.Get(key)
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// Set adds or replaces key, keeping first-insertion order.
func (o *Object) Set(key string, value any) {
	if o.Values == nil {
		o.Values = map[string]any{}
	}
	if _, ok := o.Values[key]; !ok {
		o.Keys = append(o.Keys, key)
	}
	o.Values[key] = value
}

// With returns a shallow copy with key replaced (the Python `{**schema, "type": item}`).
func (o *Object) With(key string, value any) *Object {
	out := &Object{Keys: append([]string(nil), o.Keys...), Values: make(map[string]any, len(o.Values)+1)}
	for k, v := range o.Values {
		out.Values[k] = v
	}
	out.Set(key, value)
	return out
}

// DecodeOrdered parses JSON into Object / []any / string / json.Number / bool / nil.
func DecodeOrdered(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	value, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	return value, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			obj := &Object{Values: map[string]any{}}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyTok.(string)
				value, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				obj.Set(key, value)
			}
			if _, err := dec.Token(); err != nil { // '}'
				return nil, err
			}
			return obj, nil
		case '[':
			var items []any
			for dec.More() {
				value, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				items = append(items, value)
			}
			if _, err := dec.Token(); err != nil { // ']'
				return nil, err
			}
			if items == nil {
				items = []any{}
			}
			return items, nil
		}
		return nil, fmt.Errorf("unexpected delimiter %v", t)
	default:
		return tok, nil
	}
}

// Equal is Python dict/list equality: order-insensitive for objects.
func Equal(a, b any) bool {
	switch x := a.(type) {
	case *Object:
		y, ok := b.(*Object)
		if !ok || len(x.Keys) != len(y.Keys) {
			return false
		}
		for _, k := range x.Keys {
			yv, ok := y.Values[k]
			if !ok || !Equal(x.Values[k], yv) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !Equal(x[i], y[i]) {
				return false
			}
		}
		return true
	case json.Number:
		y, ok := b.(json.Number)
		return ok && numberEqual(x, y)
	default:
		return a == b
	}
}

func numberEqual(a, b json.Number) bool {
	if a == b {
		return true
	}
	fa, errA := a.Float64()
	fb, errB := b.Float64()
	return errA == nil && errB == nil && fa == fb
}

// Encode renders a value as JSON preserving object key order (compact).
func Encode(value any) []byte {
	var b strings.Builder
	encode(&b, value)
	return []byte(b.String())
}

func encode(b *strings.Builder, value any) {
	switch v := value.(type) {
	case *Object:
		b.WriteByte('{')
		for i, k := range v.Keys {
			if i > 0 {
				b.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			b.Write(kb)
			b.WriteByte(':')
			encode(b, v.Values[k])
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				b.WriteByte(',')
			}
			encode(b, item)
		}
		b.WriteByte(']')
	case json.Number:
		b.WriteString(string(v))
	default:
		data, _ := json.Marshal(v)
		b.Write(data)
	}
}
