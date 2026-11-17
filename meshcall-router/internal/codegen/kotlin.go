package codegen

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// The Kotlin target: the client model (clientmodel.go) spelled for the
// meshcall-jvm runtime. One file: kotlinx.serialization classes and a client
// with suspend / Flow methods, plus `…Async` methods for Java callers.

func init() {
	registerTarget(Target{
		Language: "kotlin", Extension: ".kt", Golden: "client.kt",
		FileName: func(service *contract.Service, opts TargetOptions) string {
			if opts.ClassName != "" {
				return opts.ClassName + ".kt"
			}
			return KotlinClientName(service) + ".kt"
		},
		Render: func(service *contract.Service, opts TargetOptions) string {
			return RenderKotlinClient(service, KotlinOptions{PackageName: opts.PackageName, ClassName: opts.ClassName})
		},
	})
}

// KotlinOptions configures the Kotlin client generator.
type KotlinOptions struct {
	// PackageName is the file's package (default: GoPackageName,
	// "demo.v1.Greeting" → "greetingclient").
	PackageName string
	// ClassName is the client class (default: the service's last name
	// segment plus "Client").
	ClassName string
}

// KotlinClientName is the default client class: "demo.v1.Greeting" → "GreetingClient".
func KotlinClientName(service *contract.Service) string {
	return kotlinTypeName(serviceBaseName(service.Name), "Service") + "Client"
}

// Kotlin spells initialisms as words ("sessionId", "HttpUrl").
func kotlinTypeName(value, fallback string) string {
	return camelCase(value, fallback, true, nil)
}

func kotlinMemberName(value, fallback string) string {
	return camelCase(value, fallback, false, nil)
}

var kotlinNaming = Naming{
	TypeName: kotlinTypeName, MemberName: kotlinMemberName,
	// Names a generated method may not take as parameters: the ones it adds.
	ReservedParams: map[string]bool{"options": true, "instance": true, "items": true, "rpc": true, "observer": true},
}

var kotlinKeywords = map[string]bool{
	"as": true, "break": true, "class": true, "continue": true, "do": true, "else": true, "false": true,
	"for": true, "fun": true, "if": true, "in": true, "interface": true, "is": true, "null": true,
	"object": true, "package": true, "return": true, "super": true, "this": true, "throw": true,
	"true": true, "try": true, "typealias": true, "typeof": true, "val": true, "var": true,
	"when": true, "while": true,
}

// kotlinEscape backquotes a keyword used as a name.
func kotlinEscape(name string) string {
	if kotlinKeywords[name] {
		return "`" + name + "`"
	}
	return name
}

// kotlinType spells a type; nested classes are always named qualified
// (Request.Diff), which is valid inside and outside their enclosing class.
func kotlinType(e TypeExpr, uses map[string]bool) string {
	switch e.Kind {
	case ExprInt:
		return "Long"
	case ExprDouble:
		return "Double"
	case ExprString:
		return "String"
	case ExprBool:
		return "Boolean"
	case ExprArray:
		return "List<" + kotlinType(*e.Elem, uses) + ">"
	case ExprMap:
		return "Map<String, " + kotlinType(*e.Elem, uses) + ">"
	case ExprJSONObject:
		uses["JsonObject"] = true
		return "JsonObject"
	case ExprNamed:
		return e.Qualified(".")
	}
	uses["JsonElement"] = true
	return "JsonElement"
}

func kotlinDoc(text, indent string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	text = strings.ReplaceAll(text, "*/", "*&#47;")
	lines := strings.Split(text, "\n")
	if len(lines) == 1 {
		return indent + "/** " + strings.TrimRight(lines[0], " \t") + " */\n"
	}
	var b strings.Builder
	b.WriteString(indent + "/**\n")
	for _, line := range lines {
		b.WriteString(strings.TrimRight(indent+" * "+line, " \t") + "\n")
	}
	b.WriteString(indent + " */\n")
	return b.String()
}

// kotlinString is a Kotlin string literal ("$" would start a template).
func kotlinString(value string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '$':
			b.WriteString(`\$`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 || r == 0x7f {
				b.WriteString(`\u` + leftPad(strconv.FormatInt(int64(r), 16), 4))
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func leftPad(s string, n int) string {
	for len(s) < n {
		s = "0" + s
	}
	return s
}

// kotlinLiteral renders a schema default as a Kotlin literal, when it is one.
func kotlinLiteral(value any, kind ExprKind) (string, bool) {
	switch v := value.(type) {
	case bool:
		if kind == ExprBool {
			return strconv.FormatBool(v), true
		}
	case string:
		if kind == ExprString {
			return kotlinString(v), true
		}
	case json.Number:
		if kind == ExprInt {
			if _, err := v.Int64(); err == nil {
				return v.String(), true
			}
		}
		if kind == ExprDouble {
			text := v.String()
			if !strings.ContainsAny(text, ".eE") {
				text += ".0"
			}
			return text, true
		}
	}
	return "", false
}

// kotlinClass renders a @Serializable class; nested classes go inside it.
func kotlinClass(decl *TypeDecl, indent string, uses map[string]bool) string {
	var b strings.Builder
	b.WriteString(kotlinDoc(decl.Description, indent))
	b.WriteString(indent + "@Serializable\n")
	if len(decl.Fields) == 0 {
		b.WriteString(indent + "public class " + decl.Name + "()")
	} else {
		b.WriteString(indent + "public data class " + decl.Name + "(\n")
		inner := indent + "    "
		for _, f := range decl.Fields {
			b.WriteString(kotlinDoc(f.Description, inner))
			line := inner
			if f.Name != f.Key {
				uses["SerialName"] = true
				line += "@SerialName(" + kotlinString(f.Key) + ") "
			}
			line += "val " + kotlinEscape(f.Name) + ": " + kotlinType(f.Type, uses)
			switch {
			case f.RequiredNull:
				// Required but nullable: always sent, as null when unset.
				line += "?"
			case f.Optional:
				// Omitted when null, so the server's default applies.
				line += "? = null"
			}
			b.WriteString(line + ",\n")
		}
		b.WriteString(indent + ")")
	}
	if len(decl.Nested) > 0 {
		b.WriteString(" {\n")
		for i, nested := range decl.Nested {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString(kotlinClass(nested, indent+"    ", uses))
		}
		b.WriteString(indent + "}")
	}
	b.WriteString("\n")
	return b.String()
}

func kotlinDecl(decl *TypeDecl, uses map[string]bool) string {
	switch decl.Kind {
	case DeclEnum:
		// An open set: values this client does not know yet still decode.
		var b strings.Builder
		b.WriteString(kotlinDoc(decl.Description, ""))
		b.WriteString("public typealias " + decl.Name + " = String\n\n")
		b.WriteString("/** Known values of [" + decl.Name + "]; a newer server may send others. */\n")
		b.WriteString("public object " + decl.Name + "Values {\n")
		for _, v := range decl.Enum {
			b.WriteString("    public const val " + kotlinEscape(v.Name) + ": " + decl.Name + " = " + kotlinString(v.Value) + "\n")
		}
		b.WriteString("}\n")
		return b.String()
	case DeclAlias:
		base := kotlinType(decl.Alias, uses)
		if decl.AliasNullable {
			base += "?"
		}
		out := kotlinDoc(decl.Description, "") + "public typealias " + decl.Name + " = " + base + "\n"
		for _, nested := range decl.Nested {
			out += "\n" + kotlinClass(nested, "", uses)
		}
		return out
	}
	return kotlinClass(decl, "", uses)
}

// RenderKotlinClient renders one Kotlin file: every payload type and a typed client.
func RenderKotlinClient(service *contract.Service, opts KotlinOptions) string {
	if opts.PackageName == "" {
		opts.PackageName = GoPackageName(service)
	}
	if opts.ClassName == "" {
		opts.ClassName = KotlinClientName(service)
	}
	model := BuildClientModel(service, kotlinNaming, opts.ClassName)
	uses := map[string]bool{}
	var body strings.Builder
	for _, decl := range model.Types {
		body.WriteString(kotlinDecl(decl, uses) + "\n")
	}
	writeKotlinClient(&body, model, opts.ClassName, uses)

	imports := []string{
		"io.github.t4wefan.meshcall.CallOptions",
		"io.github.t4wefan.meshcall.MeshCallRpc",
		"io.github.t4wefan.meshcall.future",
		"java.util.concurrent.CompletableFuture",
		"kotlinx.serialization.Serializable",
		"kotlinx.serialization.serializer",
	}
	shapes := map[string]bool{}
	for _, m := range model.Methods {
		shapes[m.Shape] = true
	}
	if shapes["unary"] {
		imports = append(imports, "io.github.t4wefan.meshcall.unary")
	}
	if shapes["server_stream"] || shapes["duplex"] {
		imports = append(imports, "io.github.t4wefan.meshcall.StreamObserver")
	}
	if shapes["server_stream"] {
		imports = append(imports, "io.github.t4wefan.meshcall.CallHandle", "io.github.t4wefan.meshcall.observe",
			"io.github.t4wefan.meshcall.serverStream", "kotlinx.coroutines.flow.Flow")
	}
	if shapes["client_stream"] {
		imports = append(imports, "io.github.t4wefan.meshcall.clientStream", "io.github.t4wefan.meshcall.javaItems", "kotlinx.coroutines.flow.Flow")
	}
	if shapes["duplex"] {
		imports = append(imports, "io.github.t4wefan.meshcall.DuplexCall", "io.github.t4wefan.meshcall.JavaDuplexCall",
			"io.github.t4wefan.meshcall.duplex", "io.github.t4wefan.meshcall.duplexAsync")
	}
	if uses["SerialName"] {
		imports = append(imports, "kotlinx.serialization.SerialName")
	}
	for _, name := range []string{"JsonElement", "JsonObject"} {
		if uses[name] {
			imports = append(imports, "kotlinx.serialization.json."+name)
		}
	}
	imports = uniqueSorted(imports)

	var file strings.Builder
	file.WriteString("// Generated by MeshCall. Do not edit manually.\n")
	file.WriteString("// Runtime: io.github.t4wefan:meshcall:" + RuntimeRelease + " (meshcall-jvm)\n")
	file.WriteString("@file:Suppress(\"unused\", \"RedundantVisibilityModifier\")\n\n")
	file.WriteString("package " + opts.PackageName + "\n\n")
	for _, imp := range imports {
		file.WriteString("import " + imp + "\n")
	}
	file.WriteString("\n" + body.String())
	return file.String()
}

func uniqueSorted(list []string) []string {
	out := append([]string{}, list...)
	sort.Strings(out)
	unique := out[:0]
	for i, v := range out {
		if i == 0 || v != out[i-1] {
			unique = append(unique, v)
		}
	}
	return unique
}

func writeKotlinClient(b *strings.Builder, model *ClientModel, className string, uses map[string]bool) {
	b.WriteString(kotlinDoc(model.Description, ""))
	b.WriteString("public class " + className + "(private val rpc: MeshCallRpc) {\n")
	b.WriteString("    /** A view of this client with [options] applied to every call. */\n")
	b.WriteString("    public fun withOptions(options: CallOptions): " + className + " = " + className + "(rpc.withOptions(options))\n")
	for _, m := range model.Methods {
		writeKotlinMethod(b, model, m, uses)
	}
	b.WriteString("\n    public companion object {\n")
	b.WriteString("        /** The service these calls address. */\n")
	b.WriteString("        public const val SERVICE_NAME: String = " + kotlinString(model.Service.Name) + "\n")
	b.WriteString("    }\n}\n")
}

func writeKotlinMethod(b *strings.Builder, model *ClientModel, m ClientMethod, uses map[string]bool) {
	name := kotlinEscape(m.Name)
	asyncName := m.Name + "Async"
	var params, args []string
	if model.Targeted {
		// Required on tunnel connections too, so the compiler enforces it.
		params = append(params, "instance: String")
		args = append(args, "instance")
	}
	request := m.Request + "()"
	if m.WholeRequest {
		params = append(params, "request: "+m.Request)
		args = append(args, "request")
		request = "request"
	} else if len(m.Params) > 0 {
		for _, p := range m.Params {
			label := kotlinEscape(p.Label)
			param := label + ": " + kotlinType(p.Field.Type, uses)
			if p.Field.Optional {
				literal, ok := "", false
				if p.Field.HasDefault && !p.Field.RequiredNull {
					literal, ok = kotlinLiteral(p.Field.Default, p.Field.Type.Kind)
				}
				if ok {
					param += " = " + literal
				} else {
					param += "? = null"
				}
			}
			params = append(params, param)
			args = append(args, label)
		}
		var fields []string
		for _, p := range m.RequestArgs {
			fields = append(fields, kotlinEscape(p.Field.Name)+" = "+kotlinEscape(p.Label))
		}
		request = m.Request + "(" + strings.Join(fields, ", ") + ")"
	}
	options := "options"
	if model.Targeted {
		options += ".withInstance(instance)"
	}
	if m.Idempotent {
		options += ".withIdempotent(true)"
	}
	deprecated := ""
	if m.Deprecated != nil {
		note := "Use " + *m.Deprecated + " instead"
		if *m.Deprecated == "" {
			note = "No replacement"
		}
		deprecated = "    @Deprecated(" + kotlinString(note) + ")\n"
	}
	optionsParam := "options: CallOptions = CallOptions.DEFAULT"
	ser := func(t string) string { return "serializer<" + t + ">()" }
	call := func(fn string, rest ...string) string {
		all := append([]string{"SERVICE_NAME", kotlinString(m.Wire), request}, rest...)
		return "rpc." + fn + "(" + strings.Join(all, ", ") + ", " + options + ")"
	}
	write := func(signature []string, result, expr string) {
		b.WriteString("\n" + kotlinDoc(m.Description, "    ") + deprecated)
		b.WriteString("    public " + signature[0] + "(" + strings.Join(signature[1:], ", ") + "): " + result + " =\n        " + expr + "\n")
	}
	writeJava := func(signature []string, result, expr, what string) {
		b.WriteString("\n    /** [" + m.Name + "] for Java" + what + ". */\n" + deprecated)
		b.WriteString("    @JvmOverloads\n")
		b.WriteString("    public " + signature[0] + "(" + strings.Join(signature[1:], ", ") + "): " + result + " =\n        " + expr + "\n")
	}
	with := func(head string, extra ...string) []string {
		return append(append([]string{head}, params...), extra...)
	}
	forward := strings.Join(append(append([]string{}, args...), "options"), ", ")
	switch m.Shape {
	case "unary":
		write(with("suspend fun "+name, optionsParam), m.Response, call("unary", ser(m.Request), ser(m.Response)))
		writeJava(with("fun "+asyncName, optionsParam), "CompletableFuture<"+m.Response+">",
			"rpc.future { "+name+"("+forward+") }", "")
	case "server_stream":
		write(with("fun "+name, optionsParam), "Flow<"+m.Output+">", call("serverStream", ser(m.Request), ser(m.Output)))
		writeJava(with("fun "+asyncName, "observer: StreamObserver<"+m.Output+">", optionsParam), "CallHandle",
			"rpc.observe("+name+"("+forward+"), observer)", ": items go to [observer]")
	case "client_stream":
		write(with("suspend fun "+name, "items: Flow<"+m.Input+">", optionsParam), m.Response,
			call("clientStream", "items", ser(m.Request), ser(m.Input), ser(m.Response)))
		javaForward := strings.Join(append(append([]string{}, args...), "javaItems(items)", "options"), ", ")
		writeJava(with("fun "+asyncName, "items: Iterable<"+m.Input+">", optionsParam), "CompletableFuture<"+m.Response+">",
			"rpc.future { "+name+"("+javaForward+") }", "")
	case "duplex":
		write(with("suspend fun "+name, optionsParam), "DuplexCall<"+m.Input+", "+m.Output+">",
			call("duplex", ser(m.Request), ser(m.Input), ser(m.Output)))
		writeJava(with("fun "+asyncName, "observer: StreamObserver<"+m.Output+">", optionsParam),
			"CompletableFuture<JavaDuplexCall<"+m.Input+", "+m.Output+">>",
			"rpc.duplexAsync({ "+name+"("+forward+") }, observer)", ": output goes to [observer]")
	}
}
