package codegen

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// The Swift target: the client model (clientmodel.go) spelled for the
// meshcall-swift runtime. One file: Codable types and an async client.

func init() {
	registerTarget(Target{
		Language: "swift", Extension: ".swift", Golden: "client.swift",
		FileName: func(service *contract.Service, opts TargetOptions) string {
			if opts.ClassName != "" {
				return opts.ClassName + ".swift"
			}
			return SwiftClientName(service) + ".swift"
		},
		Render: func(service *contract.Service, opts TargetOptions) string {
			return RenderSwiftClient(service, SwiftOptions{ClassName: opts.ClassName, RuntimeImport: opts.RuntimeImport})
		},
	})
}

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
	return swiftTypeName(serviceBaseName(service.Name), "Service") + "Client"
}

var swiftInitialisms = map[string]string{"id": "ID", "url": "URL", "http": "HTTP", "json": "JSON", "api": "API", "ip": "IP", "uri": "URI", "uuid": "UUID"}

func swiftTypeName(value, fallback string) string {
	return camelCase(value, fallback, true, swiftInitialisms)
}

func swiftMemberName(value, fallback string) string {
	return camelCase(value, fallback, false, swiftInitialisms)
}

var swiftNaming = Naming{
	TypeName: swiftTypeName, MemberName: swiftMemberName,
	// Names a generated method may not take as parameters: the ones it adds.
	ReservedParams: map[string]bool{"options": true, "instance": true, "items": true, "rpc": true},
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

// swiftLabel spells an argument label at a call site, where Swift accepts
// every keyword but inout, var and let as it is (and warns about backquotes).
func swiftLabel(name string) string {
	switch name {
	case "inout", "var", "let":
		return "`" + name + "`"
	}
	return name
}

// swiftType spells a type: local inside the declaration enclosing a nested
// type, qualified (Request.Diff) anywhere else.
func swiftType(e TypeExpr, qualified bool) string {
	switch e.Kind {
	case ExprInt:
		return "Int"
	case ExprDouble:
		return "Double"
	case ExprString:
		return "String"
	case ExprBool:
		return "Bool"
	case ExprArray:
		return "[" + swiftType(*e.Elem, qualified) + "]"
	case ExprMap:
		return "[String: " + swiftType(*e.Elem, qualified) + "]"
	case ExprJSONObject:
		return "[String: JSONValue]"
	case ExprNamed:
		if qualified {
			return e.Qualified(".")
		}
		return e.Local()
	}
	return "JSONValue"
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

// swiftStruct renders a Codable struct.
func swiftStruct(decl *TypeDecl, indent string) string {
	fields := decl.Fields
	var b strings.Builder
	b.WriteString(swiftDocComment(decl.Description, indent))
	b.WriteString(indent + "public struct " + decl.Name + ": Codable, Sendable, Hashable {\n")
	inner := indent + "    "
	for _, f := range fields {
		b.WriteString(swiftDocComment(f.Description, inner))
		typ := swiftType(f.Type, false)
		if f.Optional {
			typ += "?"
		}
		b.WriteString(inner + "public var " + swiftEscape(f.Name) + ": " + typ + "\n")
	}
	if len(fields) > 0 {
		b.WriteString("\n")
	}
	var params, assigns []string
	for _, f := range fields {
		param := swiftEscape(f.Name) + ": " + swiftType(f.Type, false)
		if f.Optional {
			param += "? = nil"
		}
		params = append(params, param)
		assigns = append(assigns, inner+"    self."+f.Name+" = "+swiftEscape(f.Name))
	}
	if len(params) == 0 {
		b.WriteString(inner + "public init() {}\n")
	} else {
		b.WriteString(inner + "public init(" + strings.Join(params, ", ") + ") {\n" + strings.Join(assigns, "\n") + "\n" + inner + "}\n")
	}
	renamed, requiredNull := false, false
	for _, f := range fields {
		renamed = renamed || f.Name != f.Key
		requiredNull = requiredNull || f.RequiredNull
	}
	if renamed || requiredNull {
		b.WriteString("\n" + inner + "enum CodingKeys: String, CodingKey {\n")
		for _, f := range fields {
			if f.Name == f.Key {
				b.WriteString(inner + "    case " + swiftEscape(f.Name) + "\n")
			} else {
				b.WriteString(inner + "    case " + swiftEscape(f.Name) + " = " + swiftString(f.Key) + "\n")
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
			if f.Optional && !f.RequiredNull {
				method = "encodeIfPresent"
			}
			b.WriteString(inner + "    try container." + method + "(" + swiftEscape(f.Name) + ", forKey: ." + swiftEscape(f.Name) + ")\n")
		}
		b.WriteString(inner + "}\n")
	}
	for _, nested := range decl.Nested {
		b.WriteString("\n" + swiftStruct(nested, inner))
	}
	b.WriteString(indent + "}\n")
	return b.String()
}

func swiftDecl(decl *TypeDecl) string {
	switch decl.Kind {
	case DeclEnum:
		// An open set: values this client does not know yet still decode.
		var b strings.Builder
		b.WriteString(swiftDocComment(decl.Description, ""))
		b.WriteString("public struct " + decl.Name + ": RawRepresentable, Codable, Sendable, Hashable, ExpressibleByStringLiteral {\n")
		b.WriteString("    public var rawValue: String\n\n")
		b.WriteString("    public init(rawValue: String) { self.rawValue = rawValue }\n")
		b.WriteString("    public init(stringLiteral value: String) { self.rawValue = value }\n")
		b.WriteString("    public init(from decoder: Decoder) throws { rawValue = try decoder.singleValueContainer().decode(String.self) }\n")
		b.WriteString("    public func encode(to encoder: Encoder) throws {\n")
		b.WriteString("        var container = encoder.singleValueContainer()\n        try container.encode(rawValue)\n    }\n\n")
		for _, v := range decl.Enum {
			b.WriteString("    public static let " + swiftEscape(v.Name) + ": " + decl.Name + " = " + swiftString(v.Value) + "\n")
		}
		b.WriteString("}\n")
		return b.String()
	case DeclAlias:
		base := swiftType(decl.Alias, false)
		if decl.AliasNullable {
			base += "?"
		}
		out := swiftDocComment(decl.Description, "") + "public typealias " + decl.Name + " = " + base + "\n"
		for _, nested := range decl.Nested {
			out += "\n" + swiftStruct(nested, "")
		}
		return out
	}
	return swiftStruct(decl, "")
}

// RenderSwiftClient renders one Swift file: every payload type and a typed client.
func RenderSwiftClient(service *contract.Service, opts SwiftOptions) string {
	opts = opts.withDefaults(service)
	model := BuildClientModel(service, swiftNaming, opts.ClassName)
	var b strings.Builder
	b.WriteString("// Generated by MeshCall. Do not edit manually.\n")
	b.WriteString("import " + opts.RuntimeImport + "\n\n")
	for _, decl := range model.Types {
		b.WriteString(swiftDecl(decl) + "\n")
	}
	b.WriteString(swiftDocComment(model.Description, ""))
	b.WriteString("public struct " + opts.ClassName + ": Sendable {\n")
	b.WriteString("    /// The service these calls address.\n")
	b.WriteString("    public static let serviceName = " + swiftString(service.Name) + "\n\n")
	b.WriteString("    private let rpc: any MeshCallRPC\n\n")
	b.WriteString("    /// Wraps a `Mesh` (or any `MeshCallRPC`).\n")
	b.WriteString("    public init(_ rpc: any MeshCallRPC) {\n        self.rpc = rpc\n    }\n\n")
	b.WriteString("    /// A copy of the client with default `CallOptions` applied to every call.\n")
	b.WriteString("    public func withOptions(_ options: CallOptions) -> " + opts.ClassName + " {\n")
	b.WriteString("        " + opts.ClassName + "(rpc.withCallOptions(options))\n    }\n")
	for _, m := range model.Methods {
		b.WriteString("\n")
		writeSwiftMethod(&b, model, m)
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

// swiftLiteral renders a schema default as a Swift literal, when it is one.
func swiftLiteral(value any, kind ExprKind) (string, bool) {
	switch v := value.(type) {
	case nil:
		return "nil", true
	case bool:
		if kind == ExprBool {
			return strconv.FormatBool(v), true
		}
	case string:
		if kind == ExprString {
			return swiftString(v), true
		}
	case json.Number:
		if kind == ExprInt {
			if _, err := v.Int64(); err == nil {
				return v.String(), true
			}
		}
		if kind == ExprDouble {
			return v.String(), true
		}
	}
	return "", false
}

func writeSwiftMethod(b *strings.Builder, model *ClientModel, m ClientMethod) {
	name := swiftEscape(m.Name)
	b.WriteString(swiftDocComment(m.Description, "    "))
	if m.Deprecated != nil {
		message := "use " + *m.Deprecated
		if *m.Deprecated == "" {
			message = "no replacement"
		}
		b.WriteString("    @available(*, deprecated, message: " + swiftString(message) + ")\n")
	}
	var params []string
	if model.Targeted {
		// Required on tunnel connections too: the instance is implied there.
		params = append(params, "instance: String")
	}
	request := m.Request + "()"
	if m.WholeRequest {
		params = append(params, "request: "+m.Request)
		request = "request"
	} else if len(m.Params) > 0 {
		for _, p := range m.Params {
			param := swiftEscape(p.Label) + ": " + swiftType(p.Field.Type, true)
			if p.Field.Optional {
				literal, ok := "", false
				if p.Field.HasDefault && !p.Field.RequiredNull {
					literal, ok = swiftLiteral(p.Field.Default, p.Field.Type.Kind)
				}
				if ok && literal != "nil" {
					param += " = " + literal
				} else {
					param += "? = nil"
				}
			}
			params = append(params, param)
		}
		var args []string
		for _, p := range m.RequestArgs {
			args = append(args, swiftLabel(p.Field.Name)+": "+swiftEscape(p.Label))
		}
		request = m.Request + "(" + strings.Join(args, ", ") + ")"
	}
	var setup []string
	optionsExpr := "options"
	if m.Shape == "duplex" {
		params = append(params, "options: DuplexCallOptions = DuplexCallOptions()")
		if model.Targeted {
			setup = append(setup, "var options = options", "options.instance = instance")
		}
	} else {
		params = append(params, "options: CallOptions = CallOptions()")
		var defaults []string
		if m.Idempotent {
			defaults = append(defaults, "idempotent: true")
		}
		if model.Targeted {
			defaults = append(defaults, "instance: instance")
		}
		if len(defaults) > 0 {
			optionsExpr = "CallOptions(" + strings.Join(defaults, ", ") + ").merged(with: options)"
		}
	}
	method := swiftString(m.Wire)
	var signature, call string
	switch m.Shape {
	case "unary":
		signature = "public func " + name + "(" + strings.Join(params, ", ") + ") async throws -> " + m.Response
		call = "try await rpc.unary(Self.serviceName, " + method + ", " + request + ", as: " + m.Response + ".self, options: " + optionsExpr + ")"
	case "server_stream":
		signature = "public func " + name + "(" + strings.Join(params, ", ") + ") async throws -> ServerStream<" + m.Output + ">"
		call = "try await rpc.serverStream(Self.serviceName, " + method + ", " + request + ", of: " + m.Output + ".self, options: " + optionsExpr + ")"
	case "client_stream":
		withItems := append(params[:len(params)-1:len(params)-1], "items: Items", params[len(params)-1])
		signature = "public func " + name + "<Items: AsyncSequence & Sendable>(" + strings.Join(withItems, ", ") + ") async throws -> " + m.Response + " where Items.Element == " + m.Input
		call = "try await rpc.clientStream(Self.serviceName, " + method + ", " + request + ", items: items, as: " + m.Response + ".self, options: " + optionsExpr + ")"
	case "duplex":
		signature = "public func " + name + "(" + strings.Join(params, ", ") + ") async throws -> DuplexCall<" + m.Input + ", " + m.Output + ">"
		call = "try await rpc.duplex(Self.serviceName, " + method + ", " + request + ", input: " + m.Input + ".self, output: " + m.Output + ".self, options: options)"
	}
	b.WriteString("    " + signature + " {\n")
	for _, line := range setup {
		b.WriteString("        " + line + "\n")
	}
	b.WriteString("        " + call + "\n    }\n")
}
