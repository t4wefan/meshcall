package meshcall

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// JSON Schema from Go types, in the dialect the other runtimes emit (and the
// generators read): objects with "properties", "required" and "title", named
// structs under "$defs" referenced as "#/$defs/Name", optional values as
// anyOf with {"type": "null"}.
//
// Struct fields follow encoding/json: the json tag names a property, "-"
// skips it, a field is required unless it is a pointer or tagged omitempty,
// and embedded structs are flattened. Tags `doc:"…"`, `enum:"a,b"` and
// `default:"<json>"` add a description, an enumeration and a default.

// Schema is a JSON Schema object with its keys in insertion order.
type Schema = orderedObject

type orderedObject struct {
	keys   []string
	values map[string]any
}

func newObject() *orderedObject { return &orderedObject{values: map[string]any{}} }

func (o *orderedObject) set(key string, value any) *orderedObject {
	if _, ok := o.values[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
	return o
}

func (o *orderedObject) get(key string) (any, bool) {
	v, ok := o.values[key]
	return v, ok
}

func (o *orderedObject) MarshalJSON() ([]byte, error) {
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		key, _ := json.Marshal(k)
		b.Write(key)
		b.WriteByte(':')
		value, err := json.Marshal(o.values[k])
		if err != nil {
			return nil, err
		}
		b.Write(value)
	}
	b.WriteByte('}')
	return []byte(b.String()), nil
}

var (
	timeType = reflect.TypeOf(time.Time{})
	rawType  = reflect.TypeOf(json.RawMessage{})
)

type schemaBuilder struct {
	defs  *orderedObject
	names map[reflect.Type]string
	taken map[string]reflect.Type
}

// schemaOf returns the schema of t, with every nested named struct in $defs.
func schemaOf(t reflect.Type) *orderedObject {
	b := &schemaBuilder{defs: newObject(), names: map[reflect.Type]string{}, taken: map[string]reflect.Type{}}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	var root *orderedObject
	if t.Kind() == reflect.Struct && t != timeType {
		root = b.structSchema(t)
	} else {
		root = b.typeSchema(t)
	}
	if len(b.defs.keys) > 0 {
		withDefs := newObject().set("$defs", b.defs)
		for _, k := range root.keys {
			withDefs.set(k, root.values[k])
		}
		root = withDefs
	}
	return root
}

func (b *schemaBuilder) typeSchema(t reflect.Type) *orderedObject {
	switch {
	case t == timeType:
		return newObject().set("type", "string").set("format", "date-time")
	case t == rawType:
		return newObject()
	}
	switch t.Kind() {
	case reflect.Pointer:
		return newObject().set("anyOf", []any{b.typeSchema(t.Elem()), newObject().set("type", "null")})
	case reflect.String:
		return newObject().set("type", "string")
	case reflect.Bool:
		return newObject().set("type", "boolean")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return newObject().set("type", "integer")
	case reflect.Float32, reflect.Float64:
		return newObject().set("type", "number")
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 && t.Kind() == reflect.Slice {
			return newObject().set("type", "string").set("contentEncoding", "base64")
		}
		return newObject().set("type", "array").set("items", b.typeSchema(t.Elem()))
	case reflect.Map:
		return newObject().set("type", "object").set("additionalProperties", b.typeSchema(t.Elem()))
	case reflect.Struct:
		if t.Name() == "" {
			return b.structSchema(t)
		}
		return newObject().set("$ref", "#/$defs/"+b.define(t))
	default: // interface{} and anything else: any JSON value
		return newObject()
	}
}

// define puts t's schema under $defs once and returns its name.
func (b *schemaBuilder) define(t reflect.Type) string {
	if name, ok := b.names[t]; ok {
		return name
	}
	name := t.Name()
	for i := 2; b.taken[name] != nil && b.taken[name] != t; i++ {
		name = t.Name() + strconv.Itoa(i)
	}
	b.names[t], b.taken[name] = name, t
	b.defs.set(name, nil) // reserve the slot (and its position) before recursing
	b.defs.set(name, b.structSchema(t))
	return name
}

type structField struct {
	name     string
	index    []int
	typ      reflect.Type
	required bool
	tag      reflect.StructTag
}

// fieldsOf lists t's JSON fields in declaration order, flattening embedded structs.
func fieldsOf(t reflect.Type) []structField {
	var out []structField
	var walk func(t reflect.Type, prefix []int)
	walk = func(t reflect.Type, prefix []int) {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			index := append(append([]int{}, prefix...), i)
			tag := f.Tag.Get("json")
			if tag == "-" {
				continue
			}
			name, opts, _ := strings.Cut(tag, ",")
			if f.Anonymous && name == "" {
				ft := f.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					walk(ft, index)
					continue
				}
			}
			if !f.IsExported() {
				continue
			}
			if name == "" {
				name = f.Name
			}
			omitempty := strings.Contains(","+opts+",", ",omitempty,") || strings.Contains(","+opts+",", ",omitzero,")
			out = append(out, structField{
				name: name, index: index, typ: f.Type, tag: f.Tag,
				required: !omitempty && f.Type.Kind() != reflect.Pointer,
			})
		}
	}
	walk(t, nil)
	return out
}

func (b *schemaBuilder) structSchema(t reflect.Type) *orderedObject {
	properties := newObject()
	var required []any
	for _, f := range fieldsOf(t) {
		s := b.typeSchema(f.typ)
		if doc := f.tag.Get("doc"); doc != "" {
			s.set("description", doc)
		}
		if enum := f.tag.Get("enum"); enum != "" {
			values := []any{}
			for _, v := range strings.Split(enum, ",") {
				values = append(values, v)
			}
			s.set("enum", values)
		}
		if def := f.tag.Get("default"); def != "" {
			var v any
			if json.Unmarshal([]byte(def), &v) == nil {
				s.set("default", v)
			}
		}
		properties.set(f.name, s)
		if f.required {
			required = append(required, f.name)
		}
	}
	s := newObject().set("properties", properties)
	if len(required) > 0 {
		s.set("required", required)
	}
	if t.Name() != "" {
		s.set("title", t.Name())
	}
	return s.set("type", "object")
}

// --- validation ------------------------------------------------------------

// Violation is one way a value breaks its schema: a dotted path ("" for the
// root), a short code (missing, type, enum) and a message. It never contains
// the offending value (docs/protocol.md → Validation failures).
type Violation struct {
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// validate checks raw JSON against schema (as produced by schemaOf).
func validate(schema *orderedObject, raw []byte) []Violation {
	var value any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return []Violation{{Path: "", Code: "json_invalid", Message: "Invalid JSON"}}
	}
	v := &validator{defs: map[string]*orderedObject{}}
	if defs, ok := schema.get("$defs"); ok {
		if d, ok := defs.(*orderedObject); ok {
			for _, k := range d.keys {
				if s, ok := d.values[k].(*orderedObject); ok {
					v.defs[k] = s
				}
			}
		}
	}
	v.check(schema, value, nil)
	return v.out
}

type validator struct {
	defs map[string]*orderedObject
	out  []Violation
}

func (v *validator) fail(path []string, code, message string) {
	v.out = append(v.out, Violation{Path: strings.Join(path, "."), Code: code, Message: message})
}

func (v *validator) check(s *orderedObject, value any, path []string) {
	if ref, ok := s.get("$ref"); ok {
		name := strings.TrimPrefix(ref.(string), "#/$defs/")
		if target := v.defs[name]; target != nil {
			v.check(target, value, path)
		}
		return
	}
	if anyOf, ok := s.get("anyOf"); ok {
		for _, option := range anyOf.([]any) {
			probe := &validator{defs: v.defs}
			probe.check(option.(*orderedObject), value, path)
			if len(probe.out) == 0 {
				return
			}
		}
		v.fail(path, "type", "Input does not match any allowed type")
		return
	}
	if enum, ok := s.get("enum"); ok {
		found := false
		for _, e := range enum.([]any) {
			if fmt.Sprint(e) == fmt.Sprint(value) {
				found = true
			}
		}
		if !found {
			v.fail(path, "enum", "Input is not one of the allowed values")
			return
		}
	}
	kind, _ := s.get("type")
	switch kind {
	case nil:
		return
	case "null":
		if value != nil {
			v.fail(path, "type", "Input should be null")
		}
	case "string":
		if _, ok := value.(string); !ok {
			v.fail(path, "type", "Input should be a valid string")
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			v.fail(path, "type", "Input should be a valid boolean")
		}
	case "integer":
		n, ok := value.(json.Number)
		if f, err := n.Float64(); !ok || err != nil || f != math.Trunc(f) {
			v.fail(path, "type", "Input should be a valid integer")
		}
	case "number":
		if _, ok := value.(json.Number); !ok {
			v.fail(path, "type", "Input should be a valid number")
		}
	case "array":
		items, ok := value.([]any)
		if !ok {
			v.fail(path, "type", "Input should be a valid array")
			return
		}
		if item, ok := s.get("items"); ok {
			for i, element := range items {
				v.check(item.(*orderedObject), element, append(path, strconv.Itoa(i)))
			}
		}
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			v.fail(path, "type", "Input should be a valid object")
			return
		}
		if required, ok := s.get("required"); ok {
			for _, name := range required.([]any) {
				if _, present := object[name.(string)]; !present {
					v.fail(append(path, name.(string)), "missing", "Field required")
				}
			}
		}
		properties, _ := s.get("properties")
		props, _ := properties.(*orderedObject)
		additional, _ := s.get("additionalProperties")
		for _, key := range sortedKeys(object) {
			child := append(append([]string{}, path...), key)
			if props != nil {
				if p, ok := props.values[key]; ok {
					v.check(p.(*orderedObject), object[key], child)
					continue
				}
			}
			if a, ok := additional.(*orderedObject); ok {
				v.check(a, object[key], child)
			}
		}
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func violationList(vs []Violation) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = map[string]any{"path": v.Path, "code": v.Code, "message": v.Message}
	}
	return out
}
