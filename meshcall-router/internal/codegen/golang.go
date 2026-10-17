package codegen

import (
	"go/format"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// GoOptions configures the Go client generator.
type GoOptions struct {
	// PackageName is the generated file's package (default: the service's
	// last name segment, lower case, plus "client").
	PackageName string
	// RuntimeImport is the Go runtime's module path.
	RuntimeImport string
}

func (o GoOptions) withDefaults(service *contract.Service) GoOptions {
	if o.RuntimeImport == "" {
		o.RuntimeImport = "github.com/t4wefan/meshcall/meshcall-go"
	}
	if o.PackageName == "" {
		o.PackageName = GoPackageName(service)
	}
	return o
}

var goNonIdent = regexp.MustCompile(`[^A-Za-z0-9]+`)

// GoPackageName is the default package: "demo.v1.Greeting" → "greetingclient".
func GoPackageName(service *contract.Service) string {
	parts := strings.Split(service.Name, ".")
	name := parts[0]
	for i := len(parts) - 1; i >= 0; i-- {
		if !majorVersion.MatchString(parts[i]) && !numericPart.MatchString(parts[i]) {
			name = parts[i]
			break
		}
	}
	return strings.ToLower(goNonIdent.ReplaceAllString(name, "")) + "client"
}

var (
	majorVersion = regexp.MustCompile(`^v\d+$`)
	numericPart  = regexp.MustCompile(`^\d+(-.*)?$`)
)

var goInitialisms = map[string]string{"id": "ID", "url": "URL", "http": "HTTP", "json": "JSON", "api": "API", "ip": "IP", "uri": "URI", "uuid": "UUID"}

// goIdentifier makes an exported Go name: "session_id" → "SessionID".
func goIdentifier(value, fallback string) string {
	var b strings.Builder
	for _, part := range goNonIdent.Split(strings.ReplaceAll(value, "_", " "), -1) {
		for _, word := range strings.Fields(part) {
			if upper, ok := goInitialisms[strings.ToLower(word)]; ok {
				b.WriteString(upper)
				continue
			}
			runes := []rune(word)
			runes[0] = unicode.ToUpper(runes[0])
			b.WriteString(string(runes))
		}
	}
	out := b.String()
	if out == "" {
		return fallback
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "X" + out
	}
	return out
}

type goWriter struct {
	registry *typeRegistry
	raw      bool // json.RawMessage used
}

// goType maps a JSON Schema to a Go type; optional values become pointers
// (an omitted field must not overwrite the server's default with a zero).
func (g *goWriter) goType(schema *contract.Object, defNames map[string]string, optional bool) string {
	base, nullable := g.baseType(schema, defNames)
	if (optional || nullable) && !strings.HasPrefix(base, "[]") && !strings.HasPrefix(base, "map[") && base != "any" && base != "json.RawMessage" {
		return "*" + base
	}
	return base
}

func (g *goWriter) baseType(schema *contract.Object, defNames map[string]string) (string, bool) {
	if schema == nil {
		return "any", false
	}
	if ref := schema.Str("$ref"); strings.HasPrefix(ref, "#/$defs/") {
		key := ref[strings.LastIndexByte(ref, '/')+1:]
		if name, ok := defNames[key]; ok {
			return name, false
		}
		return goIdentifier(key, "Value"), false
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
				t, _ := g.baseType(others[0], defNames)
				return t, nullable
			}
			g.raw = true
			return "json.RawMessage", false
		}
	}
	if allOf, ok := schema.Values["allOf"].([]any); ok && len(allOf) == 1 {
		o, _ := allOf[0].(*contract.Object)
		return g.baseType(o, defNames)
	}
	if _, ok := schema.Values["enum"]; ok && schema.Str("type") == "" {
		return "string", false
	}
	switch t := schema.Values["type"].(type) {
	case []any:
		var kinds []string
		nullable := false
		for _, k := range t {
			if k == "null" {
				nullable = true
			} else if s, ok := k.(string); ok {
				kinds = append(kinds, s)
			}
		}
		if len(kinds) == 1 {
			base, _ := g.baseType(schema.With("type", kinds[0]), defNames)
			return base, nullable
		}
		g.raw = true
		return "json.RawMessage", false
	case string:
		switch t {
		case "integer":
			return "int64", false
		case "number":
			return "float64", false
		case "string":
			return "string", false
		case "boolean":
			return "bool", false
		case "null":
			return "any", false
		case "array":
			item, _ := g.baseType(schema.Obj("items"), defNames)
			return "[]" + item, false
		case "object":
			if additional := schema.Obj("additionalProperties"); additional != nil {
				value, _ := g.baseType(additional, defNames)
				return "map[string]" + value, false
			}
			if props := schema.Obj("properties"); props != nil && len(props.Keys) > 0 {
				return g.structBody(schema, defNames, ""), false
			}
			return "map[string]any", false
		}
	}
	return "any", false
}

// structBody renders "struct { … }" for an object schema.
func (g *goWriter) structBody(schema *contract.Object, defNames map[string]string, indent string) string {
	props := schema.Obj("properties")
	required := stringSet(schema.Strings("required"))
	var b strings.Builder
	b.WriteString("struct {\n")
	used := map[string]int{}
	for _, key := range props.Keys {
		prop := props.Obj(key)
		name := goIdentifier(key, "Field")
		if used[name]++; used[name] > 1 {
			name += itoa(used[name])
		}
		if desc := prop.Str("description"); desc != "" {
			for _, line := range strings.Split(strings.TrimSpace(desc), "\n") {
				b.WriteString(indent + "\t// " + strings.TrimRight(line, " \t") + "\n")
			}
		}
		tag := key
		if !required[key] {
			tag += ",omitempty"
		}
		b.WriteString(indent + "\t" + name + " " + g.goType(prop, defNames, !required[key]) + " `json:\"" + tag + "\"`\n")
	}
	b.WriteString(indent + "}")
	return b.String()
}

func goComment(text *string, indent string) string {
	if text == nil || strings.TrimSpace(*text) == "" {
		return ""
	}
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(*text), "\n") {
		b.WriteString(strings.TrimRight(indent+"// "+line, " \t") + "\n")
	}
	return b.String()
}

// RenderGoClient renders one Go file: every payload type and a typed client.
func RenderGoClient(service *contract.Service, opts GoOptions) string {
	opts = opts.withDefaults(service)
	registry := newTypeRegistry(service, goIdentifier)
	g := &goWriter{registry: registry}
	var types strings.Builder
	rendered := map[string]bool{}
	for _, ref := range registry.refs {
		names := registry.DefinitionNames(ref)
		if defs := ref.Schema.Obj("$defs"); defs != nil {
			keys := append([]string{}, defs.Keys...)
			for _, key := range keys {
				if rendered[names[key]] {
					continue
				}
				rendered[names[key]] = true
				g.writeType(&types, names[key], defs.Obj(key), names)
			}
		}
		name := registry.Name(ref)
		if !rendered[name] {
			rendered[name] = true
			g.writeType(&types, name, ref.Schema, names)
		}
	}

	className := goIdentifier(lastSegment(service.Name), "Service")
	if majorVersion.MatchString(lastSegment(service.Name)) || numericPart.MatchString(lastSegment(service.Name)) {
		className = goIdentifier(strings.TrimSuffix(GoPackageName(service), "client"), "Service")
	}
	className += "Client"
	var client strings.Builder
	client.WriteString(goComment(service.Description, ""))
	client.WriteString("type " + className + " struct {\n\trpc meshcall.Caller\n}\n\n")
	client.WriteString("// New" + className + " wraps a WebSocket client, a Router or an HTTPRouter.\n")
	client.WriteString("func New" + className + "(rpc meshcall.Caller) *" + className + " {\n\treturn &" + className + "{rpc: rpc}\n}\n\n")
	client.WriteString("// WithOptions returns a shallow copy of the client with default CallOptions applied to every call.\n")
	client.WriteString("func (c *" + className + ") WithOptions(opts meshcall.CallOptions) *" + className + " {\n\treturn &" + className + "{rpc: c.rpc.WithCallOptions(opts)}\n}\n\n")
	client.WriteString("// ServiceName is the service these calls address.\nconst ServiceName = " + jsonDumps(service.Name) + "\n")
	for _, m := range service.Methods {
		g.writeMethod(&client, service, m, className)
	}

	var file strings.Builder
	file.WriteString("// Code generated by MeshCall. DO NOT EDIT.\n\n")
	file.WriteString("package " + opts.PackageName + "\n\n")
	file.WriteString("import (\n\t\"context\"\n")
	if g.raw {
		file.WriteString("\t\"encoding/json\"\n")
	}
	file.WriteString("\n\tmeshcall " + jsonDumps(opts.RuntimeImport) + "\n)\n\n")
	file.WriteString(types.String())
	file.WriteString(client.String())
	formatted, err := format.Source([]byte(file.String()))
	if err != nil {
		panic(genError("generated Go does not parse: " + err.Error()))
	}
	return string(formatted)
}

func (g *goWriter) writeType(b *strings.Builder, name string, schema *contract.Object, defNames map[string]string) {
	if desc := schema.Str("description"); desc != "" {
		b.WriteString(goComment(&desc, ""))
	}
	if enum, ok := schema.Values["enum"].([]any); ok {
		b.WriteString("type " + name + " = string\n\n")
		values := make([]string, 0, len(enum))
		for _, v := range enum {
			if s, ok := v.(string); ok {
				values = append(values, s)
			}
		}
		sort.Strings(values)
		if len(values) > 0 {
			b.WriteString("// " + name + " values: " + strings.Join(values, ", ") + ".\n\n")
		}
		return
	}
	if props := schema.Obj("properties"); props == nil || len(props.Keys) == 0 {
		if schema.Str("type") == "object" || schema.Str("type") == "" {
			b.WriteString("type " + name + " struct{}\n\n")
			return
		}
	}
	if schema.Str("type") != "object" && schema.Obj("properties") == nil {
		base, _ := g.baseType(schema, defNames)
		b.WriteString("type " + name + " = " + base + "\n\n")
		return
	}
	b.WriteString("type " + name + " " + g.structBody(schema, defNames, "") + "\n\n")
}

var goReservedParams = map[string]bool{
	"break": true, "case": true, "chan": true, "const": true, "continue": true,
	"default": true, "defer": true, "else": true, "fallthrough": true, "for": true,
	"func": true, "go": true, "goto": true, "if": true, "import": true,
	"interface": true, "map": true, "package": true, "range": true, "return": true,
	"select": true, "struct": true, "switch": true, "type": true, "var": true,
	"any": true, "bool": true, "byte": true, "comparable": true, "complex64": true,
	"complex128": true, "error": true, "float32": true, "float64": true, "int": true,
	"int8": true, "int16": true, "int32": true, "int64": true, "nil": true,
	"rune": true, "string": true, "true": true, "false": true, "uint": true,
	"uint8": true, "uint16": true, "uint32": true, "uint64": true, "uintptr": true,
	"c": true, "ctx": true, "instance": true, "items": true, "opts": true,
	"context": true, "meshcall": true, "json": true,
}

func goLowerIdentifier(value, fallback string) string {
	id := goIdentifier(value, fallback)
	runes := []rune(id)
	upperCount := 0
	for upperCount < len(runes) && unicode.IsUpper(runes[upperCount]) {
		upperCount++
	}
	switch {
	case upperCount == 0:
		return id
	case upperCount == len(runes):
		return strings.ToLower(id)
	case upperCount > 1:
		for i := 0; i < upperCount-1; i++ {
			runes[i] = unicode.ToLower(runes[i])
		}
	default:
		runes[0] = unicode.ToLower(runes[0])
	}
	return string(runes)
}

func goStructFieldNames(schema *contract.Object) map[string]string {
	out := map[string]string{}
	props := schema.Obj("properties")
	if props == nil {
		return out
	}
	used := map[string]int{}
	for _, key := range props.Keys {
		name := goIdentifier(key, "Field")
		if used[name]++; used[name] > 1 {
			name += itoa(used[name])
		}
		out[key] = name
	}
	return out
}

func (g *goWriter) goMethodParams(m *contract.Method, requestType string) ([]string, string) {
	if len(m.Parameters) == 0 {
		return nil, requestType + "{}"
	}
	props := m.Request.Schema.Obj("properties")
	required := stringSet(m.Request.Schema.Strings("required"))
	defNames := g.registry.DefinitionNames(m.Request)
	fieldNames := goStructFieldNames(m.Request.Schema)
	used := map[string]int{}
	var decls []string
	var fields []string
	for _, field := range m.Parameters {
		prop := props.Obj(field)
		if prop == nil {
			panic(genError("Request parameter " + pyStringRepr(field) + " is missing from " + m.Request.Qualname))
		}
		paramName := goLowerIdentifier(field, "param")
		if goReservedParams[paramName] {
			paramName += "Param"
		}
		if used[paramName]++; used[paramName] > 1 {
			paramName += itoa(used[paramName])
		}
		paramType := g.goType(prop, defNames, !required[field])
		decls = append(decls, paramName+" "+paramType)
		fields = append(fields, fieldNames[field]+": "+paramName)
	}
	return decls, requestType + "{" + strings.Join(fields, ", ") + "}"
}

func (g *goWriter) writeMethod(b *strings.Builder, service *contract.Service, m *contract.Method, className string) {
	reg := g.registry
	request := reg.Name(m.Request)
	name := goIdentifier(m.Name, "Call")
	b.WriteString("\n")
	b.WriteString(goComment(m.Description, ""))
	if m.Deprecated != nil {
		note := "use " + *m.Deprecated
		if *m.Deprecated == "" {
			note = "no replacement"
		}
		b.WriteString("//\n// Deprecated: " + note + ".\n")
	}
	sigParams := []string{"ctx context.Context"}
	if service.Targeted {
		sigParams = append(sigParams, "instance string")
	}
	paramDecls, reqExpr := g.goMethodParams(m, request)
	sigParams = append(sigParams, paramDecls...)
	var defaults []string
	if service.Targeted {
		defaults = append(defaults, "meshcall.WithInstance(instance)")
	}
	if m.Annotations.Idempotent && m.Stream == "unary" {
		defaults = append(defaults, "meshcall.IdempotentCall()")
	}
	options := "opts..."
	if len(defaults) > 0 {
		options = "append([]meshcall.CallOption{" + strings.Join(defaults, ", ") + "}, opts...)..."
	}
	call := func(fn, types string, args ...string) string {
		all := append([]string{"ctx", "c.rpc", "ServiceName", jsonDumps(m.Name)}, args...)
		return "meshcall." + fn + "[" + types + "](" + strings.Join(all, ", ") + ", " + options + ")"
	}
	switch m.Stream {
	case "unary":
		response := reg.Name(requiredRef(m.Response, m.Name+" response"))
		fullSig := append(append([]string{}, sigParams...), "opts ...meshcall.CallOption")
		b.WriteString("func (c *" + className + ") " + name + "(" + strings.Join(fullSig, ", ") + ") (" + response + ", error) {\n")
		b.WriteString("\treturn " + call("Unary", request+", "+response, reqExpr) + "\n}\n")
	case "server_stream":
		item := reg.Name(requiredRef(m.OutputItem, m.Name+" output item"))
		fullSig := append(append([]string{}, sigParams...), "opts ...meshcall.CallOption")
		if s := m.Subscription; s != nil {
			spec := "meshcall.SubscriptionSpec{}"
			if s.Cursor != nil {
				spec = "meshcall.SubscriptionSpec{Cursor: " + jsonDumps(*s.Cursor) + ", Resume: " + jsonDumps(*s.Resume) + "}"
			}
			b.WriteString("func (c *" + className + ") " + name + "(" + strings.Join(fullSig, ", ") + ") (*meshcall.Subscription[" + item + "], error) {\n")
			b.WriteString("\treturn " + call("Subscribe", request+", "+item, reqExpr, spec) + "\n}\n")
			return
		}
		b.WriteString("func (c *" + className + ") " + name + "(" + strings.Join(fullSig, ", ") + ") (*meshcall.Stream[" + item + "], error) {\n")
		b.WriteString("\treturn " + call("ServerStream", request+", "+item, reqExpr) + "\n}\n")
	case "client_stream":
		input := reg.Name(requiredRef(m.InputItem, m.Name+" input item"))
		response := reg.Name(requiredRef(m.Response, m.Name+" response"))
		fullSig := append(append([]string{}, sigParams...), "items func(yield func("+input+") bool)", "opts ...meshcall.CallOption")
		b.WriteString("func (c *" + className + ") " + name + "(" + strings.Join(fullSig, ", ") + ") (" + response + ", error) {\n")
		b.WriteString("\treturn " + call("ClientStream", request+", "+input+", "+response, reqExpr, "items") + "\n}\n")
	case "duplex":
		input := reg.Name(requiredRef(m.InputItem, m.Name+" input item"))
		output := reg.Name(requiredRef(m.OutputItem, m.Name+" output item"))
		fullSig := append(append([]string{}, sigParams...), "opts ...meshcall.CallOption")
		b.WriteString("func (c *" + className + ") " + name + "(" + strings.Join(fullSig, ", ") + ") (*meshcall.DuplexCall[" + input + ", " + output + "], error) {\n")
		b.WriteString("\treturn " + call("Duplex", request+", "+input+", "+output, reqExpr) + "\n}\n")
	}
}
