package codegen

import (
	"strconv"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// hoistInlineObjects returns a copy of service whose payload schemas carry
// every inline object that declares properties as a named "$defs" entry, so
// the TypeScript generator renders an interface for it instead of
// Record<string, unknown>. TypeScript (zod) contracts inline every nested
// object; Python (Pydantic) contracts already name their models in "$defs".
//
// Names follow the path from the payload type: a property appends its name
// in UpperCamelCase ("ListResponse" + "sessions" → "ListResponseSessions"),
// an array item appends "Item", a map value appends "Value", and a union
// member keeps the name when it is the union's only object (a nullable
// object) and is numbered otherwise. A name already used in the same schema
// gets a numeric suffix. The order of the input fixes every name, so the
// same contract always generates the same types.
//
// The second result maps each payload type to its original schema, which is
// what the generated client validates received payloads against.
func hoistInlineObjects(service *contract.Service) (*contract.Service, map[refKey]*contract.Object) {
	out := *service
	out.Methods = make([]*contract.Method, len(service.Methods))
	copies := map[refKey]*contract.TypeRef{}
	originals := map[refKey]*contract.Object{}
	hoist := func(ref *contract.TypeRef) *contract.TypeRef {
		if ref == nil {
			return nil
		}
		key := refKey{ref.Module, ref.Qualname}
		if copied, ok := copies[key]; ok {
			return copied
		}
		copied := &contract.TypeRef{Module: ref.Module, Qualname: ref.Qualname, Schema: hoistSchema(ref.Schema, lastSegment(ref.Qualname))}
		copies[key] = copied
		originals[key] = ref.Schema
		return copied
	}
	for i, m := range service.Methods {
		method := *m
		method.Request = hoist(m.Request)
		method.Response = hoist(m.Response)
		method.InputItem = hoist(m.InputItem)
		method.OutputItem = hoist(m.OutputItem)
		out.Methods[i] = &method
	}
	return &out, originals
}

type schemaHoister struct {
	defs *contract.Object
	used map[string]bool
}

func hoistSchema(root *contract.Object, rootName string) *contract.Object {
	if root == nil {
		return nil
	}
	h := &schemaHoister{defs: &contract.Object{}, used: map[string]bool{rootName: true}}
	existing := root.Obj("$defs")
	if existing != nil {
		for _, key := range existing.Keys {
			h.used[key] = true
		}
		for _, key := range existing.Keys {
			if definition := existing.Obj(key); definition != nil {
				h.defs.Set(key, h.members(definition, key))
			} else {
				h.defs.Set(key, existing.Values[key])
			}
		}
	}
	out := h.members(root, rootName)
	if len(h.defs.Keys) > 0 {
		out.Set("$defs", h.defs)
	}
	return out
}

// members copies schema with every subschema it contains hoisted.
func (h *schemaHoister) members(schema *contract.Object, name string) *contract.Object {
	out := &contract.Object{}
	for _, key := range schema.Keys {
		value := schema.Values[key]
		switch key {
		case "$defs":
			continue // collected once, at the root
		case "properties":
			if properties, ok := value.(*contract.Object); ok {
				hoisted := &contract.Object{}
				for _, prop := range properties.Keys {
					hoisted.Set(prop, h.child(properties.Values[prop], name+camelCase(prop, "Field", true, nil)))
				}
				value = hoisted
			}
		case "items":
			value = h.child(value, name+"Item")
		case "additionalProperties":
			value = h.child(value, name+"Value")
		case "anyOf", "oneOf", "allOf":
			if variants, ok := value.([]any); ok {
				objects := 0
				for _, variant := range variants {
					if obj, ok := variant.(*contract.Object); ok && isInlineObject(obj) {
						objects++
					}
				}
				hoisted := make([]any, len(variants))
				n := 0
				for i, variant := range variants {
					variantName := name
					if obj, ok := variant.(*contract.Object); ok && isInlineObject(obj) && objects > 1 {
						n++
						variantName = name + strconv.Itoa(n)
					}
					hoisted[i] = h.child(variant, variantName)
				}
				value = hoisted
			}
		}
		out.Set(key, value)
	}
	return out
}

// child returns value with inline objects replaced by a "$ref" to a new
// definition named name (or name with a numeric suffix).
func (h *schemaHoister) child(value any, name string) any {
	schema, ok := value.(*contract.Object)
	if !ok {
		return value
	}
	if !isInlineObject(schema) {
		return h.members(schema, name)
	}
	unique := name
	for i := 2; h.used[unique]; i++ {
		unique = name + strconv.Itoa(i)
	}
	h.used[unique] = true
	// Reserve the slot first so definitions come out parent before child.
	h.defs.Set(unique, nil)
	h.defs.Set(unique, h.members(schema, unique))
	ref := &contract.Object{}
	ref.Set("$ref", "#/$defs/"+unique)
	if schema.Has("default") {
		// A property's default stays visible where the generator looks for it.
		ref.Set("default", schema.Values["default"])
	}
	return ref
}

func isInlineObject(schema *contract.Object) bool {
	if schema.Has("$ref") {
		return false
	}
	switch t := schema.Values["type"].(type) {
	case string:
		if t != "object" {
			return false
		}
	case nil:
	default:
		return false // ["object", "null"] and the like keep their inline type
	}
	properties := schema.Obj("properties")
	return properties != nil && len(properties.Keys) > 0
}
