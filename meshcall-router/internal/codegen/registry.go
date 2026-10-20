package codegen

import (
	"strconv"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// typeRegistry assigns one class/interface name to every payload type of a
// service and to every nested "$defs" entry, exactly as both language
// generators do: root types in first-use order, numbered on clashes; nested
// definitions keep their own name when unambiguous across the contract and
// get the root name as a prefix otherwise. Each definition renders once.
type typeRegistry struct {
	refs        []*contract.TypeRef
	names       map[refKey]string
	defNames    map[refKey]map[string]string
	definitions []namedSchema
}

type refKey struct{ module, qualname string }

type namedSchema struct {
	name   string
	schema *contract.Object
}

func newTypeRegistry(service *contract.Service, identifier func(string, string) string) *typeRegistry {
	return newMultiTypeRegistry([]*contract.Service{service}, identifier)
}

func newMultiTypeRegistry(services []*contract.Service, identifier func(string, string) string) *typeRegistry {
	r := &typeRegistry{names: map[refKey]string{}, defNames: map[refKey]map[string]string{}}
	seen := map[refKey]bool{}
	var allRefs []*contract.TypeRef
	for _, service := range services {
		for _, m := range service.Methods {
			for _, ref := range []*contract.TypeRef{m.Request, m.Response, m.InputItem, m.OutputItem} {
				if ref == nil {
					continue
				}
				k := refKey{ref.Module, ref.Qualname}
				if !seen[k] {
					seen[k] = true
					allRefs = append(allRefs, ref)
				}
			}
		}
	}
	counts := map[string]int{}
	byBase := map[string][]namedSchema{}
	for _, ref := range allRefs {
		base := identifier(lastSegment(ref.Qualname), "Payload")
		existing := ""
		for _, candidate := range byBase[base] {
			if contract.Equal(candidate.schema, ref.Schema) {
				existing = candidate.name
				break
			}
		}
		if existing != "" {
			r.names[refKey{ref.Module, ref.Qualname}] = existing
			continue
		}
		r.refs = append(r.refs, ref)
		counts[base]++
		name := base
		if counts[base] > 1 {
			name = base + itoa(counts[base])
		}
		r.names[refKey{ref.Module, ref.Qualname}] = name
		byBase[base] = append(byBase[base], namedSchema{name, ref.Schema})
	}
	taken := map[string]bool{}
	for _, n := range r.names {
		taken[n] = true
	}
	byKey := map[string][]namedSchema{}
	for _, ref := range allRefs {
		names := map[string]string{}
		if defs := ref.Schema.Obj("$defs"); defs != nil {
			for _, key := range defs.Keys {
				definition, _ := defs.Values[key].(*contract.Object)
				existing := ""
				for _, candidate := range byKey[key] {
					if contract.Equal(candidate.schema, definition) {
						existing = candidate.name
						break
					}
				}
				if existing == "" {
					bare := identifier(key, "Value")
					existing = bare
					if taken[bare] {
						existing = r.Name(ref) + bare
					}
					taken[existing] = true
					byKey[key] = append(byKey[key], namedSchema{existing, definition})
					r.definitions = append(r.definitions, namedSchema{existing, definition})
				}
				names[key] = existing
			}
		}
		r.defNames[refKey{ref.Module, ref.Qualname}] = names
	}
	return r
}

func (r *typeRegistry) Name(ref *contract.TypeRef) string {
	return r.names[refKey{ref.Module, ref.Qualname}]
}

func (r *typeRegistry) DefinitionNames(ref *contract.TypeRef) map[string]string {
	return r.defNames[refKey{ref.Module, ref.Qualname}]
}

// TypeNames lists root type names in first-use order.
func (r *typeRegistry) TypeNames() []string {
	out := make([]string, 0, len(r.refs))
	for _, ref := range r.refs {
		out = append(out, r.Name(ref))
	}
	return out
}

func lastSegment(qualname string) string {
	if i := strings.LastIndexByte(qualname, '.'); i >= 0 {
		return qualname[i+1:]
	}
	return qualname
}

func itoa(n int) string { return strconv.Itoa(n) }

// requiredRef panics with a clear message when a shape lacks a payload type;
// contracts from the Python/TypeScript exporters always have them.
func requiredRef(ref *contract.TypeRef, what string) *contract.TypeRef {
	if ref == nil {
		panic(genError("RPC method is missing a required payload type: " + what))
	}
	return ref
}

type genError string

func (e genError) Error() string { return string(e) }

func hasStream(service *contract.Service, kind string) bool {
	for _, m := range service.Methods {
		if m.Stream == kind {
			return true
		}
	}
	return false
}
