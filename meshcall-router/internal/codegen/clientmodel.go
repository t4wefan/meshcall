package codegen

import (
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// The client model is a language-neutral reading of one service for the
// single-file client generators (Swift; next Kotlin/Java). It decides what
// is a type and what is nested, which fields are optional, how a method's
// parameters map onto its request and what each shape takes and returns; a
// target only spells it. Every rule here is shared, so two targets generated
// from one contract agree on names, nesting and parameter order.

// Naming is a target's spelling of contract names.
type Naming struct {
	// TypeName: "tool_part" → "ToolPart".
	TypeName func(value, fallback string) string
	// MemberName: fields, parameters, methods and enum constants: "session_id" → "sessionID".
	MemberName func(value, fallback string) string
	// ReservedParams are parameter names generated methods add themselves
	// (options, instance, items, …); a field with one of them gets a "Value" suffix.
	ReservedParams map[string]bool
	// ReservedTypes are type names the generated file uses besides its own:
	// the language's (String, Int), the runtime's (CallOptions, Flow) and its
	// own helpers (CodingKeys). A payload type, definition or nested type
	// spelled like one gets a "Value" suffix.
	ReservedTypes map[string]bool
	// ReservedMembers are member names the generated code uses itself (self,
	// init, rawValue, withOptions, serializer, …). A field, enum constant or
	// method spelled like one gets a "Value" suffix.
	ReservedMembers map[string]bool
}

// ExprKind is the kind of a field or alias type.
type ExprKind int

const (
	ExprJSON       ExprKind = iota // any JSON value
	ExprInt                        // 64-bit integer
	ExprDouble                     // float64
	ExprString                     // string
	ExprBool                       // boolean
	ExprArray                      // [Elem]
	ExprMap                        // string → Elem
	ExprJSONObject                 // an object of any JSON values
	ExprNamed                      // a declared type: Path names it
)

// TypeExpr is a type as the contract states it.
type TypeExpr struct {
	Kind ExprKind
	Elem *TypeExpr
	// Path names an ExprNamed type: one element for a root or definition
	// type, the enclosing types first for a nested one (["Request", "Diff"]).
	Path []string
}

// Local is the name used inside the declaration that encloses it.
func (e TypeExpr) Local() string { return e.Path[len(e.Path)-1] }

// Qualified is the name used anywhere else.
func (e TypeExpr) Qualified(sep string) string { return strings.Join(e.Path, sep) }

// DeclKind is what a declared type is.
type DeclKind int

const (
	DeclStruct DeclKind = iota // fields
	DeclEnum                   // an open set of strings: unknown values still decode
	DeclAlias                  // another type under a name
)

type TypeDecl struct {
	Name        string
	Kind        DeclKind
	Description string
	Fields      []Field
	// Nested are the structs inline objects need. A struct's are declared
	// inside it; an alias's next to it.
	Nested []*TypeDecl
	Enum   []EnumValue
	// Alias is the aliased type; nullable when AliasNullable.
	Alias         TypeExpr
	AliasNullable bool
}

type EnumValue struct {
	Name  string // MemberName of the value
	Value string
}

type Field struct {
	Key  string // the wire name
	Name string // MemberName, numbered on clashes
	Type TypeExpr
	// Optional fields may be absent (or null); the target's type is nullable.
	Optional bool
	// RequiredNull fields are required but nullable: sent as null, never omitted.
	RequiredNull bool
	Description  string
	// Default is the schema default, when HasDefault.
	Default    any
	HasDefault bool
}

// Param is one method parameter: a field of the request.
type Param struct {
	Label string // MemberName, suffixed when reserved, numbered on clashes
	Field Field  // with a nested type's Path qualified by the request type
}

type ClientMethod struct {
	Wire        string // the method name on the wire
	Name        string // MemberName
	Description string
	Deprecated  *string
	Shape       string // unary | server_stream | client_stream | duplex
	Request     string // the request type
	// Params in the contract's parameter order. WholeRequest: the method takes
	// the request itself (no parameters, yet required fields).
	Params       []Param
	WholeRequest bool
	// RequestArgs are Params in the request's field order, for building it.
	RequestArgs []Param
	Response    string // unary, client_stream
	Input       string // client_stream, duplex
	Output      string // server_stream, duplex
	// Idempotent unary methods may be retried without an idempotency key.
	Idempotent bool
}

type ClientModel struct {
	Service     *contract.Service
	ClientName  string
	Description string
	// Targeted services take the instance as the first parameter of every method.
	Targeted bool
	Types    []*TypeDecl
	Methods  []ClientMethod
}

// BuildClientModel reads a service with a target's naming.
func BuildClientModel(service *contract.Service, naming Naming, clientName string) *ClientModel {
	registry := newTypeRegistry(service, naming.TypeName)
	b := &modelBuilder{naming: naming, taken: map[string]bool{clientName: true}, renamed: map[string]string{}}
	for _, name := range registry.names {
		b.taken[name] = true
	}
	for _, def := range registry.definitions {
		b.taken[def.name] = true
	}
	// The registry's names are shared with the Python and TypeScript
	// generators; a target's reserved type names are avoided here.
	for _, name := range append(registry.TypeNames(), definitionNames(registry)...) {
		if naming.ReservedTypes[name] && b.renamed[name] == "" {
			b.renamed[name] = b.unique(name, func(n string) bool { return b.taken[n] || naming.ReservedTypes[n] })
			b.taken[b.renamed[name]] = true
		}
	}
	model := &ClientModel{
		Service: service, ClientName: clientName, Description: derefOr(service.Description), Targeted: service.Targeted,
	}
	roots := map[string]*TypeDecl{}
	rendered := map[string]bool{}
	methodNames := map[string]bool{}
	for _, ref := range registry.refs {
		names := b.definitionNames(registry, ref)
		if defs := ref.Schema.Obj("$defs"); defs != nil {
			for _, key := range defs.Keys {
				if rendered[names[key]] {
					continue
				}
				rendered[names[key]] = true
				model.Types = append(model.Types, b.typeDecl(names[key], defs.Obj(key), names))
			}
		}
		name := b.typeName(registry.Name(ref))
		if !rendered[name] {
			rendered[name] = true
			decl := b.typeDecl(name, ref.Schema, names)
			roots[name] = decl
			model.Types = append(model.Types, decl)
		}
	}
	for _, m := range service.Methods {
		model.Methods = append(model.Methods, b.method(m, registry, roots, methodNames))
	}
	return model
}

type modelBuilder struct {
	naming  Naming
	taken   map[string]bool   // root, definition, client and top-level nested type names
	renamed map[string]string // registry name → name, for reserved type names
}

func definitionNames(registry *typeRegistry) []string {
	out := make([]string, 0, len(registry.definitions))
	for _, def := range registry.definitions {
		out = append(out, def.name)
	}
	return out
}

// typeName is a registry name as this target spells it.
func (b *modelBuilder) typeName(name string) string {
	if renamed := b.renamed[name]; renamed != "" {
		return renamed
	}
	return name
}

func (b *modelBuilder) definitionNames(registry *typeRegistry, ref *contract.TypeRef) map[string]string {
	names := map[string]string{}
	for key, name := range registry.DefinitionNames(ref) {
		names[key] = b.typeName(name)
	}
	return names
}

// unique is name, or name with a "Value" suffix (numbered from 2) when name clashes.
func (b *modelBuilder) unique(name string, clash func(string) bool) string {
	if !clash(name) {
		return name
	}
	candidate := name + "Value"
	for i := 2; clash(candidate); i++ {
		candidate = name + "Value" + itoa(i)
	}
	return candidate
}

// memberName spells a field, enum constant or method, avoiding reserved members.
func (b *modelBuilder) memberName(value, fallback string) string {
	name := b.naming.MemberName(value, fallback)
	if b.naming.ReservedMembers[name] {
		name += "Value"
	}
	return name
}

// nestedName names the struct for an inline object, unique in the scope it
// is declared in: its siblings, the types enclosing it, every top-level type
// and the target's reserved type names.
func (b *modelBuilder) nestedName(hint string, owner []string, siblings []*TypeDecl) string {
	name := b.unique(b.naming.TypeName(hint, "Value"), func(n string) bool {
		if b.taken[n] || b.naming.ReservedTypes[n] {
			return true
		}
		for _, enclosing := range owner {
			if enclosing == n {
				return true
			}
		}
		for _, sibling := range siblings {
			if sibling.Name == n {
				return true
			}
		}
		return false
	})
	if len(owner) == 0 {
		b.taken[name] = true // declared at top level, next to an alias
	}
	return name
}

func (b *modelBuilder) typeDecl(name string, schema *contract.Object, defNames map[string]string) *TypeDecl {
	if enum, ok := schema.Values["enum"].([]any); ok && allStrings(enum) {
		decl := &TypeDecl{Name: name, Kind: DeclEnum, Description: schema.Str("description")}
		used := map[string]bool{}
		for _, v := range enum {
			member := b.memberName(v.(string), "value")
			if used[member] {
				continue
			}
			used[member] = true
			decl.Enum = append(decl.Enum, EnumValue{Name: member, Value: v.(string)})
		}
		return decl
	}
	if props := schema.Obj("properties"); props != nil && len(props.Keys) > 0 {
		return b.structDecl(name, schema, defNames, []string{name})
	}
	plain := !schema.Has("additionalProperties") && !schema.Has("anyOf") && !schema.Has("oneOf") &&
		!schema.Has("allOf") && !schema.Has("$ref") && !schema.Has("enum") && !schema.Has("const")
	if t := schema.Str("type"); plain && (t == "object" || t == "") {
		return b.structDecl(name, schema, defNames, []string{name})
	}
	decl := &TypeDecl{Name: name, Kind: DeclAlias, Description: schema.Str("description")}
	decl.Alias, decl.AliasNullable = b.expr(schema, defNames, name+"Value", nil, &decl.Nested)
	return decl
}

// structDecl reads an object schema; path names the struct (enclosing types first).
func (b *modelBuilder) structDecl(name string, schema *contract.Object, defNames map[string]string, path []string) *TypeDecl {
	decl := &TypeDecl{Name: name, Kind: DeclStruct, Description: schema.Str("description")}
	decl.Fields = b.fields(schema, defNames, path, &decl.Nested)
	return decl
}

func (b *modelBuilder) fields(schema *contract.Object, defNames map[string]string, owner []string, nested *[]*TypeDecl) []Field {
	props := schema.Obj("properties")
	if props == nil {
		return nil
	}
	required := stringSet(schema.Strings("required"))
	used := map[string]int{}
	var out []Field
	for _, key := range props.Keys {
		prop := props.Obj(key)
		name := b.memberName(key, "field")
		if used[name]++; used[name] > 1 {
			name += itoa(used[name])
		}
		typ, nullable := b.expr(prop, defNames, key, owner, nested)
		field := Field{
			Key: key, Name: name, Type: typ, Optional: nullable || !required[key], RequiredNull: nullable && required[key],
			Description: prop.Str("description"),
		}
		field.Default, field.HasDefault = prop.Get("default")
		out = append(out, field)
	}
	return out
}

// expr maps a JSON Schema to a type; inline objects become structs named
// after hint, nested in owner (top level when owner is nil).
func (b *modelBuilder) expr(schema *contract.Object, defNames map[string]string, hint string, owner []string, nested *[]*TypeDecl) (TypeExpr, bool) {
	jsonValue := TypeExpr{Kind: ExprJSON}
	if schema == nil {
		return jsonValue, false
	}
	if ref := schema.Str("$ref"); strings.HasPrefix(ref, "#/$defs/") {
		key := ref[strings.LastIndexByte(ref, '/')+1:]
		if name, ok := defNames[key]; ok {
			return TypeExpr{Kind: ExprNamed, Path: []string{name}}, false
		}
		return TypeExpr{Kind: ExprNamed, Path: []string{b.typeName(b.naming.TypeName(key, "Value"))}}, false
	}
	for _, keyword := range []string{"anyOf", "oneOf"} {
		if variants, ok := schema.Values[keyword].([]any); ok && len(variants) > 0 {
			var others []*contract.Object
			nullable := false
			for _, v := range variants {
				o, _ := v.(*contract.Object)
				if o != nil && o.Str("type") == "null" {
					nullable = true
					continue
				}
				others = append(others, o)
			}
			if len(others) == 1 {
				t, _ := b.expr(others[0], defNames, hint, owner, nested)
				return t, nullable
			}
			return jsonValue, false
		}
	}
	if allOf, ok := schema.Values["allOf"].([]any); ok && len(allOf) == 1 {
		o, _ := allOf[0].(*contract.Object)
		return b.expr(o, defNames, hint, owner, nested)
	}
	if enum, ok := schema.Values["enum"].([]any); ok && schema.Str("type") == "" {
		if allStrings(enum) {
			return TypeExpr{Kind: ExprString}, false
		}
		return jsonValue, false
	}
	if value, ok := schema.Values["const"]; ok && schema.Str("type") == "" {
		if _, isString := value.(string); isString {
			return TypeExpr{Kind: ExprString}, false
		}
		return jsonValue, false
	}
	switch t := schema.Values["type"].(type) {
	case []any:
		var kinds []string
		nullable := false
		for _, k := range t {
			if k == "null" {
				nullable = true
			} else if str, ok := k.(string); ok {
				kinds = append(kinds, str)
			}
		}
		if len(kinds) == 1 {
			base, _ := b.expr(schema.With("type", kinds[0]), defNames, hint, owner, nested)
			return base, nullable
		}
		return jsonValue, false
	case string:
		switch t {
		case "integer":
			return TypeExpr{Kind: ExprInt}, false
		case "number":
			return TypeExpr{Kind: ExprDouble}, false
		case "string":
			return TypeExpr{Kind: ExprString}, false
		case "boolean":
			return TypeExpr{Kind: ExprBool}, false
		case "null":
			return jsonValue, false
		case "array":
			item, _ := b.expr(schema.Obj("items"), defNames, singular(hint), owner, nested)
			return TypeExpr{Kind: ExprArray, Elem: &item}, false
		case "object":
			if additional := schema.Obj("additionalProperties"); additional != nil {
				value, _ := b.expr(additional, defNames, hint+"Value", owner, nested)
				return TypeExpr{Kind: ExprMap, Elem: &value}, false
			}
			if props := schema.Obj("properties"); props != nil && len(props.Keys) > 0 {
				name := b.nestedName(hint, owner, *nested)
				path := append(append([]string{}, owner...), name)
				*nested = append(*nested, b.structDecl(name, schema, defNames, path))
				return TypeExpr{Kind: ExprNamed, Path: path}, false
			}
			return TypeExpr{Kind: ExprJSONObject}, false
		}
	}
	return jsonValue, false
}

func allStrings(values []any) bool {
	for _, v := range values {
		if _, ok := v.(string); !ok {
			return false
		}
	}
	return len(values) > 0
}

// singular names the item type of an array field: "diffs" → "Diff".
func singular(name string) string {
	if len(name) > 3 && strings.HasSuffix(name, "s") && !strings.HasSuffix(name, "ss") {
		return name[:len(name)-1]
	}
	return name + "Item"
}

func derefOr(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}

func (b *modelBuilder) method(m *contract.Method, registry *typeRegistry, roots map[string]*TypeDecl, methodNames map[string]bool) ClientMethod {
	// get_user and getUser are both getUser: the second becomes getUser2.
	base := b.memberName(m.Name, "call")
	name := base
	for i := 2; methodNames[name]; i++ {
		name = base + itoa(i)
	}
	methodNames[name] = true
	typeName := func(ref *contract.TypeRef) string { return b.typeName(registry.Name(ref)) }
	out := ClientMethod{
		Wire: m.Name, Name: name, Description: derefOr(m.Description),
		Deprecated: m.Deprecated, Shape: m.Stream, Request: typeName(m.Request),
		Idempotent: m.Annotations.Idempotent && m.Stream == "unary",
	}
	switch m.Stream {
	case "unary":
		out.Response = typeName(requiredRef(m.Response, m.Name+" response"))
	case "server_stream":
		out.Output = typeName(requiredRef(m.OutputItem, m.Name+" output item"))
	case "client_stream":
		out.Input = typeName(requiredRef(m.InputItem, m.Name+" input item"))
		out.Response = typeName(requiredRef(m.Response, m.Name+" response"))
	case "duplex":
		out.Input = typeName(requiredRef(m.InputItem, m.Name+" input item"))
		out.Output = typeName(requiredRef(m.OutputItem, m.Name+" output item"))
	}
	var fields []Field
	if decl := roots[out.Request]; decl != nil && decl.Kind == DeclStruct {
		fields = decl.Fields
	}
	if len(m.Parameters) == 0 {
		for _, f := range fields {
			if !f.Optional {
				// Nothing to expand into, and the request needs values: take it whole.
				out.WholeRequest = true
				break
			}
		}
		return out
	}
	byKey := map[string]Field{}
	for _, f := range fields {
		byKey[f.Key] = f
	}
	used := map[string]int{}
	given := map[string]Param{}
	for _, key := range m.Parameters {
		f, ok := byKey[key]
		if !ok {
			panic(genError("Request parameter " + pyStringRepr(key) + " is missing from " + m.Request.Qualname))
		}
		label := f.Name
		if b.naming.ReservedParams[label] {
			label += "Value"
		}
		if used[label]++; used[label] > 1 {
			label += itoa(used[label])
		}
		// A nested type's Path already starts with the request type, so
		// TypeExpr.Qualified names it outside the request.
		param := Param{Label: label, Field: f}
		out.Params = append(out.Params, param)
		given[key] = param
	}
	for _, f := range fields {
		if p, ok := given[f.Key]; ok {
			out.RequestArgs = append(out.RequestArgs, p)
		}
	}
	return out
}
