package router

import (
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// GET /v1/openapi.json: an OpenAPI 3.1 document generated live from the
// contracts of the services the caller may call, so HTTP callers can browse
// and generate clients with standard tooling. It is filtered exactly like
// the Registry and needs the same permission (Registry/list).
//
// Contract schemas reference nested types as "#/$defs/X", which in an
// OpenAPI document would resolve against the document root. Every $defs
// entry is therefore moved to components/schemas under a unique name and
// the references rewritten.

func (r *Router) serveOpenAPI(w http.ResponseWriter, user *User) {
	if !user.allowsCall(RegistryService, "list") {
		writeHTTPError(w, &errorPayload{Code: "permission_denied", Message: "Call is not permitted"})
		return
	}
	r.mu.Lock()
	doc := r.openAPI(user)
	r.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(contract.Encode(doc))
}

type openAPIBuilder struct {
	schemas *contract.Object
	taken   map[string]bool
}

// component stores schema under a unique name and returns a $ref to it.
func (b *openAPIBuilder) component(base string, schema any) *contract.Object {
	clean := strings.NewReplacer(".", "_", "/", "_").Replace(base)
	name := clean
	for i := 2; b.taken[name]; i++ {
		name = clean + "_" + strconv.Itoa(i)
	}
	b.taken[name] = true
	b.schemas.Set(name, schema)
	ref := &contract.Object{}
	ref.Set("$ref", "#/components/schemas/"+name)
	return ref
}

// lift moves a payload schema's $defs into components and rewrites its refs.
func (b *openAPIBuilder) lift(prefix string, schema *contract.Object) any {
	if schema == nil {
		return &contract.Object{}
	}
	renamed := map[string]string{}
	if defs := schema.Obj("$defs"); defs != nil {
		for _, key := range defs.Keys {
			renamed[key] = "" // placeholder so nested refs resolve while lifting
		}
		for _, key := range defs.Keys {
			ref := b.component(prefix+"_"+key, nil)
			renamed[key] = ref.Str("$ref")
		}
		for _, key := range defs.Keys {
			name := strings.TrimPrefix(renamed[key], "#/components/schemas/")
			b.schemas.Set(name, rewriteRefs(defs.Values[key], renamed))
		}
	}
	out := rewriteRefs(schema, renamed).(*contract.Object)
	stripped := &contract.Object{}
	for _, k := range out.Keys {
		if k != "$defs" {
			stripped.Set(k, out.Values[k])
		}
	}
	return stripped
}

func rewriteRefs(v any, renamed map[string]string) any {
	switch x := v.(type) {
	case *contract.Object:
		out := &contract.Object{}
		for _, k := range x.Keys {
			val := x.Values[k]
			if s, ok := val.(string); ok && k == "$ref" && strings.HasPrefix(s, "#/$defs/") {
				if target, ok := renamed[strings.TrimPrefix(s, "#/$defs/")]; ok && target != "" {
					val = target
				}
			}
			out.Set(k, rewriteRefs(val, renamed))
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = rewriteRefs(x[i], renamed)
		}
		return out
	default:
		return v
	}
}

func obj(pairs ...any) *contract.Object {
	o := &contract.Object{}
	for i := 0; i+1 < len(pairs); i += 2 {
		o.Set(pairs[i].(string), pairs[i+1])
	}
	return o
}

func content(mediaType string, schema any) *contract.Object {
	return obj(mediaType, obj("schema", schema))
}

// openAPI builds the document. Called under r.mu.
func (r *Router) openAPI(user *User) *contract.Object {
	b := &openAPIBuilder{schemas: &contract.Object{}, taken: map[string]bool{}}
	errorRef := b.component("MeshCallError", obj(
		"type", "object", "required", []any{"error"},
		"properties", obj("error", obj(
			"type", "object", "required", []any{"code", "message"},
			"properties", obj(
				"code", obj("type", "string"),
				"message", obj("type", "string"),
				"retryable", obj("type", "boolean"),
				"details", obj("type", []any{"object", "null"}),
			),
		)),
	))
	errorResponse := obj("description", "MeshCall error envelope; see the status table in docs/protocol.md", "content", content("application/json", errorRef))

	names := make([]string, 0, len(r.services))
	for name, pool := range r.services {
		if user.allowsAnyCall(name, pool) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	paths := &contract.Object{}
	tags := []any{}
	for _, name := range names {
		pool := r.services[name]
		var service *contract.Service
		if raw := visibleContract(pool.contract, name, user); raw != nil {
			if doc, err := contract.Parse([]byte(`{"services":[` + string(raw) + `]}`)); err == nil && len(doc.Services) == 1 {
				service = doc.Services[0]
			}
		}
		tag := obj("name", name)
		if service != nil && service.Description != nil {
			tag.Set("description", *service.Description)
		}
		tags = append(tags, tag)
		for _, m := range sortedMethods(pool.methods) {
			if m.Stream == "duplex" || !user.allowsCall(name, m.Name) {
				continue // duplex is WebSocket-only
			}
			var cm *contract.Method
			if service != nil {
				for _, candidate := range service.Methods {
					if candidate.Name == m.Name {
						cm = candidate
					}
				}
			}
			prefix := name + "_" + m.Name
			op := obj("operationId", prefix, "tags", []any{name})
			if cm != nil && cm.Description != nil {
				op.Set("summary", strings.SplitN(*cm.Description, "\n", 2)[0])
				op.Set("description", *cm.Description)
			}
			if cm != nil && cm.Deprecated != nil {
				op.Set("deprecated", true)
			}
			if cm != nil {
				op.Set("x-meshcall-annotations", obj("read_only", cm.Annotations.ReadOnly, "idempotent", cm.Annotations.Idempotent, "destructive", cm.Annotations.Destructive))
			}
			op.Set("x-meshcall-stream", m.Stream)
			var request, response any = &contract.Object{}, &contract.Object{}
			if cm != nil {
				request = b.lift(prefix+"_request", cm.Request.Schema)
				switch {
				case cm.Response != nil:
					response = b.lift(prefix+"_response", cm.Response.Schema)
				case cm.OutputItem != nil:
					response = b.lift(prefix+"_item", cm.OutputItem.Schema)
				}
			}
			ok := obj("description", "result")
			switch m.Stream {
			case "server_stream":
				line := obj("oneOf", []any{
					obj("type", "object", "required", []any{"item"}, "properties", obj("item", response)),
					obj("type", "object", "required", []any{"end"}, "properties", obj("end", obj("type", "object", "properties", obj("trailers", obj("type", "object", "additionalProperties", obj("type", "string")))))),
					errorRef,
				})
				ok.Set("description", "one JSON envelope per line: {\"item\"} for each item, then {\"end\"} or {\"error\"}")
				ok.Set("content", content(ndjson, line))
				op.Set("requestBody", obj("required", true, "content", content("application/json", request)))
			case "client_stream":
				ok.Set("content", content("application/json", response))
				op.Set("requestBody", obj("required", true, "description",
					"NDJSON: the first line is the request, then one input item per line", "content", content(ndjson, request)))
			default:
				ok.Set("content", content("application/json", response))
				op.Set("requestBody", obj("required", true, "content", content("application/json", request)))
			}
			op.Set("responses", obj("200", ok, "default", errorResponse))
			paths.Set("/v1/call/"+name+"/"+m.Name, obj("post", op))
		}
	}
	components := obj("schemas", b.schemas)
	doc := obj(
		"openapi", "3.1.0",
		"info", obj("title", "MeshCall services", "version", "1",
			"description", "Generated by the MeshCall Router from the contracts of the services this caller may call."),
		"tags", tags,
		"paths", paths,
		"components", components,
	)
	if r.config.Auth != nil {
		components.Set("securitySchemes", obj(
			"bearer", obj("type", "http", "scheme", "bearer", "description", "API key (mk_…) or temporary token (mt_…)"),
			"basic", obj("type", "http", "scheme", "basic"),
		))
		doc.Set("security", []any{obj("bearer", []any{}), obj("basic", []any{}), &contract.Object{}})
	}
	return doc
}
