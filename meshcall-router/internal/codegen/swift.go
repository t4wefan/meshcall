package codegen

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// SwiftOptions configures the Swift client generator.
type SwiftOptions struct {
	// ClassName is the client type (default: the service's last name
	// segment plus "Client").
	ClassName string
	// RuntimeImport is the Swift runtime's module (default "MeshCall").
	RuntimeImport string
}

func (o SwiftOptions) withDefaults(service *contract.Service) SwiftOptions {
	if o.RuntimeImport == "" {
		o.RuntimeImport = "MeshCall"
	}
	if o.ClassName == "" {
		o.ClassName = SwiftClientName(service)
	}
	return o
}

// SwiftClientName is the default client type: "demo.v1.Greeting" → "GreetingClient".
func SwiftClientName(service *contract.Service) string {
	parts := strings.Split(service.Name, ".")
	name := parts[0]
	for i := len(parts) - 1; i >= 0; i-- {
		if !majorVersion.MatchString(parts[i]) && !numericPart.MatchString(parts[i]) {
			name = parts[i]
			break
		}
	}
	return swiftTypeName(name, "Service") + "Client"
}

var swiftNonIdent = regexp.MustCompile(`[^A-Za-z0-9]+`)

var swiftInitialisms = map[string]string{"id": "ID", "url": "URL", "http": "HTTP", "json": "JSON", "api": "API", "ip": "IP", "uri": "URI", "uuid": "UUID"}

func swiftWords(value string) []string {
	var words []string
	for _, part := range swiftNonIdent.Split(value, -1) {
		if part != "" {
			words = append(words, part)
		}
	}
	return words
}

// swiftTypeName makes an UpperCamelCase type name: "tool_part" → "ToolPart".
func swiftTypeName(value, fallback string) string {
	var b strings.Builder
	for _, word := range swiftWords(value) {
		if upper, ok := swiftInitialisms[strings.ToLower(word)]; ok {
			b.WriteString(upper)
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

// swiftMemberName makes a lowerCamelCase name: "session_id" → "sessionID",
// "createdAt" stays as it is.
func swiftMemberName(value, fallback string) string {
	words := swiftWords(value)
	var b strings.Builder
	for i, word := range words {
		if i == 0 {
			if isUpper(word) {
				b.WriteString(strings.ToLower(word))
			} else {
				runes := []rune(word)
				runes[0] = unicode.ToLower(runes[0])
				b.WriteString(string(runes))
			}
			continue
		}
		if upper, ok := swiftInitialisms[strings.ToLower(word)]; ok {
			b.WriteString(upper)
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

var swiftKeywords = map[string]bool{
	"associatedtype": true, "class": true, "deinit": true, "enum": true, "extension": true, "fileprivate": true,
	"func": true, "import": true, "init": true, "inout": true, "internal": true, "let": true, "open": true,
	"operator": true, "private": true, "precedencegroup": true, "protocol": true, "public": true, "rethrows": true,
	"static": true, "struct": true, "subscript": true, "typealias": true, "var": true, "break": true, "case": true,
	"catch": true, "continue": true, "default": true, "defer": true, "do": true, "else": true, "fallthrough": true,
	"for": true, "guard": true, "if": true, "in": true, "repeat": true, "return": true, "throw": true, "switch": true,
	"where": true, "while": true, "Any": true, "as": true, "await": true, "false": true, "is": true, "nil": true,
	"self": true, "Self": true, "super": true, "throws": true, "true": true, "try": true, "Type": true, "Protocol": true,
}

// swiftEscape backquotes a keyword used as a name.
func swiftEscape(name string) string {
	if swiftKeywords[name] {
		return "`" + name + "`"
	}
	return name
}

var swiftStructName = regexp.MustCompile(`public struct (\w+):`)

// Names a generated method may not take as parameters: the ones it adds.
var swiftReservedParams = map[string]bool{"options": true, "instance": true, "items": true, "rpc": true}

type swiftWriter struct {
	registry *typeRegistry
	taken    map[string]bool // root and definition type names
}

// swiftType maps a JSON Schema to a Swift type; `nested` collects the
// structs inline objects need, declared inside the type that uses them.
func (s *swiftWriter) swiftType(schema *contract.Object, defNames map[string]string, hint string, nested *[]string, indent string) (string, bool) {
	if schema == nil {
		return "JSONValue", false
	}
	if ref := schema.Str("$ref"); strings.HasPrefix(ref, "#/$defs/") {
		key := ref[strings.LastIndexByte(ref, '/')+1:]
		if name, ok := defNames[key]; ok {
			return name, false
		}
		return swiftTypeName(key, "Value"), false
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
				t, _ := s.swiftType(others[0], defNames, hint, nested, indent)
				return t, nullable
			}
			return "JSONValue", false
		}
	}
	if allOf, ok := schema.Values["allOf"].([]any); ok && len(allOf) == 1 {
		o, _ := allOf[0].(*contract.Object)
		return s.swiftType(o, defNames, hint, nested, indent)
	}
	if enum, ok := schema.Values["enum"].([]any); ok && schema.Str("type") == "" {
		if allStrings(enum) {
			return "String", false
		}
		return "JSONValue", false
	}
	if value, ok := schema.Values["const"]; ok && schema.Str("type") == "" {
		if _, isString := value.(string); isString {
			return "String", false
		}
		return "JSONValue", false
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
			base, _ := s.swiftType(schema.With("type", kinds[0]), defNames, hint, nested, indent)
			return base, nullable
		}
		return "JSONValue", false
	case string:
		switch t {
		case "integer":
			return "Int", false
		case "number":
			return "Double", false
		case "string":
			return "String", false
		case "boolean":
			return "Bool", false
		case "null":
			return "JSONValue", false
		case "array":
			item, _ := s.swiftType(schema.Obj("items"), defNames, singular(hint), nested, indent)
			return "[" + item + "]", false
		case "object":
			if additional := schema.Obj("additionalProperties"); additional != nil {
				value, _ := s.swiftType(additional, defNames, hint+"Value", nested, indent)
				return "[String: " + value + "]", false
			}
			if props := schema.Obj("properties"); props != nil && len(props.Keys) > 0 {
				name := swiftTypeName(hint, "Value")
				if s.taken[name] {
					name += "Value"
				}
				*nested = append(*nested, s.structDecl(name, schema, defNames, indent))
				return name, false
			}
			return "[String: JSONValue]", false
		}
	}
	return "JSONValue", false
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

type swiftField struct {
	key, name, typ string
	optional       bool // the Swift type is Optional
	requiredNull   bool // required but nullable: encoded as null, never omitted
	description    string
}

func (s *swiftWriter) fields(schema *contract.Object, defNames map[string]string, nested *[]string, indent string) []swiftField {
	props := schema.Obj("properties")
	if props == nil {
		return nil
	}
	required := stringSet(schema.Strings("required"))
	used := map[string]int{}
	var out []swiftField
	for _, key := range props.Keys {
		prop := props.Obj(key)
		name := swiftMemberName(key, "field")
		if used[name]++; used[name] > 1 {
			name += itoa(used[name])
		}
		typ, nullable := s.swiftType(prop, defNames, key, nested, indent+"    ")
		optional := nullable || !required[key]
		out = append(out, swiftField{
			key: key, name: name, typ: typ, optional: optional, requiredNull: nullable && required[key],
			description: prop.Str("description"),
		})
	}
	return out
}

func swiftDocComment(text, indent string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	var b strings.Builder
	for _, line := range strings.Split(text, "\n") {
		b.WriteString(strings.TrimRight(indent+"/// "+line, " \t") + "\n")
	}
	return b.String()
}

// structDecl renders a Codable struct for an object schema.
func (s *swiftWriter) structDecl(name string, schema *contract.Object, defNames map[string]string, indent string) string {
	var nested []string
	fields := s.fields(schema, defNames, &nested, indent)
	var b strings.Builder
	b.WriteString(swiftDocComment(schema.Str("description"), indent))
	b.WriteString(indent + "public struct " + name + ": Codable, Sendable, Hashable {\n")
	inner := indent + "    "
	for _, f := range fields {
		b.WriteString(swiftDocComment(f.description, inner))
		typ := f.typ
		if f.optional {
			typ += "?"
		}
		b.WriteString(inner + "public var " + swiftEscape(f.name) + ": " + typ + "\n")
	}
	if len(fields) > 0 {
		b.WriteString("\n")
	}
	var params, assigns []string
	for _, f := range fields {
		param := swiftEscape(f.name) + ": " + f.typ
		if f.optional {
			param += "? = nil"
		}
		params = append(params, param)
		assigns = append(assigns, inner+"    self."+f.name+" = "+swiftEscape(f.name))
	}
	if len(params) == 0 {
		b.WriteString(inner + "public init() {}\n")
	} else {
		b.WriteString(inner + "public init(" + strings.Join(params, ", ") + ") {\n" + strings.Join(assigns, "\n") + "\n" + inner + "}\n")
	}
	renamed, requiredNull := false, false
	for _, f := range fields {
		renamed = renamed || f.name != f.key
		requiredNull = requiredNull || f.requiredNull
	}
	if renamed || requiredNull {
		b.WriteString("\n" + inner + "enum CodingKeys: String, CodingKey {\n")
		for _, f := range fields {
			if f.name == f.key {
				b.WriteString(inner + "    case " + swiftEscape(f.name) + "\n")
			} else {
				b.WriteString(inner + "    case " + swiftEscape(f.name) + " = " + swiftString(f.key) + "\n")
			}
		}
		b.WriteString(inner + "}\n")
	}
	if requiredNull {
		// Required but nullable: the key must be present, as null.
		b.WriteString("\n" + inner + "public func encode(to encoder: Encoder) throws {\n")
		b.WriteString(inner + "    var container = encoder.container(keyedBy: CodingKeys.self)\n")
		for _, f := range fields {
			method := "encode"
			if f.optional && !f.requiredNull {
				method = "encodeIfPresent"
			}
			b.WriteString(inner + "    try container." + method + "(" + swiftEscape(f.name) + ", forKey: ." + swiftEscape(f.name) + ")\n")
		}
		b.WriteString(inner + "}\n")
	}
	for _, decl := range nested {
		b.WriteString("\n" + decl)
	}
	b.WriteString(indent + "}\n")
	return b.String()
}

func (s *swiftWriter) typeDecl(name string, schema *contract.Object, defNames map[string]string) string {
	if enum, ok := schema.Values["enum"].([]any); ok && allStrings(enum) {
		// An open set: values this client does not know yet still decode.
		var b strings.Builder
		b.WriteString(swiftDocComment(schema.Str("description"), ""))
		b.WriteString("public struct " + name + ": RawRepresentable, Codable, Sendable, Hashable, ExpressibleByStringLiteral {\n")
		b.WriteString("    public var rawValue: String\n\n")
		b.WriteString("    public init(rawValue: String) { self.rawValue = rawValue }\n")
		b.WriteString("    public init(stringLiteral value: String) { self.rawValue = value }\n")
		b.WriteString("    public init(from decoder: Decoder) throws { rawValue = try decoder.singleValueContainer().decode(String.self) }\n")
		b.WriteString("    public func encode(to encoder: Encoder) throws {\n")
		b.WriteString("        var container = encoder.singleValueContainer()\n        try container.encode(rawValue)\n    }\n\n")
		values := make([]string, 0, len(enum))
		for _, v := range enum {
			values = append(values, v.(string))
		}
		used := map[string]bool{}
		for _, v := range values {
			member := swiftMemberName(v, "value")
			if used[member] {
				continue
			}
			used[member] = true
			b.WriteString("    public static let " + swiftEscape(member) + ": " + name + " = " + swiftString(v) + "\n")
		}
		b.WriteString("}\n")
		return b.String()
	}
	if props := schema.Obj("properties"); props != nil && len(props.Keys) > 0 {
		return s.structDecl(name, schema, defNames, "")
	}
	plain := !schema.Has("additionalProperties") && !schema.Has("anyOf") && !schema.Has("oneOf") &&
		!schema.Has("allOf") && !schema.Has("$ref") && !schema.Has("enum") && !schema.Has("const")
	if t := schema.Str("type"); plain && (t == "object" || t == "") {
		return s.structDecl(name, schema, defNames, "")
	}
	var nested []string
	base, nullable := s.swiftType(schema, defNames, name+"Value", &nested, "")
	if nullable {
		base += "?"
	}
	out := swiftDocComment(schema.Str("description"), "") + "public typealias " + name + " = " + base + "\n"
	for _, decl := range nested {
		out += "\n" + decl
	}
	return out
}

// RenderSwiftClient renders one Swift file: every payload type and a typed client.
func RenderSwiftClient(service *contract.Service, opts SwiftOptions) string {
	opts = opts.withDefaults(service)
	registry := newTypeRegistry(service, func(value, fallback string) string { return swiftTypeName(value, fallback) })
	s := &swiftWriter{registry: registry, taken: map[string]bool{opts.ClassName: true}}
	for _, name := range registry.names {
		s.taken[name] = true
	}
	for _, def := range registry.definitions {
		s.taken[def.name] = true
	}
	var types []string
	rendered := map[string]bool{}
	for _, ref := range registry.refs {
		names := registry.DefinitionNames(ref)
		if defs := ref.Schema.Obj("$defs"); defs != nil {
			for _, key := range defs.Keys {
				if rendered[names[key]] {
					continue
				}
				rendered[names[key]] = true
				types = append(types, s.typeDecl(names[key], defs.Obj(key), names))
			}
		}
		name := registry.Name(ref)
		if !rendered[name] {
			rendered[name] = true
			types = append(types, s.typeDecl(name, ref.Schema, names))
		}
	}

	var b strings.Builder
	b.WriteString("// Generated by MeshCall. Do not edit manually.\n")
	b.WriteString("import " + opts.RuntimeImport + "\n\n")
	for _, t := range types {
		b.WriteString(t + "\n")
	}
	b.WriteString(swiftDocComment(derefOr(service.Description), ""))
	b.WriteString("public struct " + opts.ClassName + ": Sendable {\n")
	b.WriteString("    /// The service these calls address.\n")
	b.WriteString("    public static let serviceName = " + swiftString(service.Name) + "\n\n")
	b.WriteString("    private let rpc: any MeshCallRPC\n\n")
	b.WriteString("    /// Wraps a `Mesh` (or any `MeshCallRPC`).\n")
	b.WriteString("    public init(_ rpc: any MeshCallRPC) {\n        self.rpc = rpc\n    }\n\n")
	b.WriteString("    /// A copy of the client with default `CallOptions` applied to every call.\n")
	b.WriteString("    public func withOptions(_ options: CallOptions) -> " + opts.ClassName + " {\n")
	b.WriteString("        " + opts.ClassName + "(rpc.withCallOptions(options))\n    }\n")
	for _, m := range service.Methods {
		b.WriteString("\n")
		s.writeMethod(&b, service, m)
	}
	b.WriteString("}\n")
	return b.String()
}

// swiftString is a Swift string literal.
func swiftString(value string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 || r == 0x7f {
				b.WriteString(`\u{` + strconv.FormatInt(int64(r), 16) + `}`)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func derefOr(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}

// swiftLiteral renders a schema default as a Swift literal, when it is one.
func swiftLiteral(value any, typ string) (string, bool) {
	switch v := value.(type) {
	case nil:
		return "nil", true
	case bool:
		if typ == "Bool" {
			return strconv.FormatBool(v), true
		}
	case string:
		if typ == "String" {
			return swiftString(v), true
		}
	case json.Number:
		if typ == "Int" {
			if _, err := v.Int64(); err == nil {
				return v.String(), true
			}
		}
		if typ == "Double" {
			return v.String(), true
		}
	}
	return "", false
}

func (s *swiftWriter) writeMethod(b *strings.Builder, service *contract.Service, m *contract.Method) {
	reg := s.registry
	requestType := reg.Name(m.Request)
	name := swiftEscape(swiftMemberName(m.Name, "call"))
	b.WriteString(swiftDocComment(derefOr(m.Description), "    "))
	if m.Deprecated != nil {
		message := "use " + *m.Deprecated
		if *m.Deprecated == "" {
			message = "no replacement"
		}
		b.WriteString("    @available(*, deprecated, message: " + swiftString(message) + ")\n")
	}
	var params []string
	if service.Targeted {
		// Required on tunnel connections too: the instance is implied there.
		params = append(params, "instance: String")
	}
	request := s.requestExpression(m, requestType, &params)
	var setup []string
	optionsExpr := "options"
	if m.Stream == "duplex" {
		params = append(params, "options: DuplexCallOptions = DuplexCallOptions()")
		if service.Targeted {
			setup = append(setup, "var options = options", "options.instance = instance")
		}
	} else {
		params = append(params, "options: CallOptions = CallOptions()")
		var defaults []string
		if m.Annotations.Idempotent && m.Stream == "unary" {
			defaults = append(defaults, "idempotent: true")
		}
		if service.Targeted {
			defaults = append(defaults, "instance: instance")
		}
		if len(defaults) > 0 {
			optionsExpr = "CallOptions(" + strings.Join(defaults, ", ") + ").merged(with: options)"
		}
	}
	method := swiftString(m.Name)
	var signature, call string
	switch m.Stream {
	case "unary":
		response := reg.Name(requiredRef(m.Response, m.Name+" response"))
		signature = "public func " + name + "(" + strings.Join(params, ", ") + ") async throws -> " + response
		call = "try await rpc.unary(Self.serviceName, " + method + ", " + request + ", as: " + response + ".self, options: " + optionsExpr + ")"
	case "server_stream":
		item := reg.Name(requiredRef(m.OutputItem, m.Name+" output item"))
		signature = "public func " + name + "(" + strings.Join(params, ", ") + ") async throws -> ServerStream<" + item + ">"
		call = "try await rpc.serverStream(Self.serviceName, " + method + ", " + request + ", of: " + item + ".self, options: " + optionsExpr + ")"
	case "client_stream":
		input := reg.Name(requiredRef(m.InputItem, m.Name+" input item"))
		response := reg.Name(requiredRef(m.Response, m.Name+" response"))
		withItems := append(params[:len(params)-1:len(params)-1], "items: Items", params[len(params)-1])
		signature = "public func " + name + "<Items: AsyncSequence & Sendable>(" + strings.Join(withItems, ", ") + ") async throws -> " + response + " where Items.Element == " + input
		call = "try await rpc.clientStream(Self.serviceName, " + method + ", " + request + ", items: items, as: " + response + ".self, options: " + optionsExpr + ")"
	case "duplex":
		input := reg.Name(requiredRef(m.InputItem, m.Name+" input item"))
		output := reg.Name(requiredRef(m.OutputItem, m.Name+" output item"))
		signature = "public func " + name + "(" + strings.Join(params, ", ") + ") async throws -> DuplexCall<" + input + ", " + output + ">"
		call = "try await rpc.duplex(Self.serviceName, " + method + ", " + request + ", input: " + input + ".self, output: " + output + ".self, options: options)"
	}
	b.WriteString("    " + signature + " {\n")
	for _, line := range setup {
		b.WriteString("        " + line + "\n")
	}
	b.WriteString("        " + call + "\n    }\n")
}

// requestExpression appends the method's parameters and returns the request value.
func (s *swiftWriter) requestExpression(m *contract.Method, requestType string, params *[]string) string {
	schema := m.Request.Schema
	defNames := s.registry.DefinitionNames(m.Request)
	var nested []string
	fields := s.fields(schema, defNames, &nested, "")
	for _, decl := range nested {
		if match := swiftStructName.FindStringSubmatch(decl); match != nil {
			qualify := regexp.MustCompile(`\b` + match[1] + `\b`)
			for i := range fields {
				fields[i].typ = qualify.ReplaceAllString(fields[i].typ, requestType+"."+match[1])
			}
		}
	}
	if len(m.Parameters) == 0 {
		for _, f := range fields {
			if !f.optional {
				// Nothing to expand into, and the request needs values: take it whole.
				*params = append(*params, "request: "+requestType)
				return "request"
			}
		}
		return requestType + "()"
	}
	byKey := map[string]swiftField{}
	for _, f := range fields {
		byKey[f.key] = f
	}
	props := schema.Obj("properties")
	used := map[string]int{}
	var args []string
	for _, key := range m.Parameters {
		f, ok := byKey[key]
		if !ok {
			panic(genError("Request parameter " + pyStringRepr(key) + " is missing from " + m.Request.Qualname))
		}
		label := f.name
		if swiftReservedParams[label] {
			label += "Value"
		}
		if used[label]++; used[label] > 1 {
			label += itoa(used[label])
		}
		param := swiftEscape(label) + ": " + f.typ
		if f.optional {
			literal, ok := "", false
			if value, has := props.Obj(key).Get("default"); has && !f.requiredNull {
				literal, ok = swiftLiteral(value, f.typ)
			}
			if ok && literal != "nil" {
				param += " = " + literal
			} else {
				param += "? = nil"
			}
		}
		*params = append(*params, param)
		args = append(args, swiftEscape(f.name)+": "+swiftEscape(label))
	}
	// The memberwise init lists every field in order; parameters may skip or reorder them.
	ordered := make([]string, 0, len(args))
	given := map[string]string{}
	for i, key := range m.Parameters {
		given[key] = args[i]
	}
	keys := make([]string, 0, len(fields))
	for _, f := range fields {
		keys = append(keys, f.key)
	}
	for _, key := range keys {
		if arg, ok := given[key]; ok {
			ordered = append(ordered, arg)
		}
	}
	return requestType + "(" + strings.Join(ordered, ", ") + ")"
}
