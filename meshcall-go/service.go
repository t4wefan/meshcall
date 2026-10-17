package meshcall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
)

// Balance policies (docs/router.md → Balancing).
type BalancePolicy struct {
	Kind string  `json:"kind"`
	Key  *string `json:"key"`
}

var (
	RoundRobin    = BalancePolicy{Kind: "round_robin"}
	LeastInflight = BalancePolicy{Kind: "least_inflight"}
	Random        = BalancePolicy{Kind: "random"}
	NoBalancing   = BalancePolicy{Kind: "disabled"}
)

// Sticky routes calls with the same value of a request field to the same
// instance: Sticky("session_id").
func Sticky(field string) BalancePolicy {
	key := "request." + field
	return BalancePolicy{Kind: "sticky", Key: &key}
}

// Service is a named set of methods served by a Server. Build one with
// NewService and add methods with HandleUnary, HandleServerStream, … .
type Service struct {
	name         string
	version      string
	description  *string
	targeted     bool
	balance      BalancePolicy
	module       string
	methods      []*method
	byName       map[string]*method
	dependencies []DependencyProvider
	methodOpts   map[string][]MethodOption
}

// ServiceOption configures a Service.
type ServiceOption interface {
	applyService(*Service)
}

type serviceOptFunc func(*Service)

func (f serviceOptFunc) applyService(s *Service) { f(s) }

// Describe sets the service's description (shown to humans and LLM callers).
// An empty string declares "no description" on purpose (silences W001).
func Describe(text string) ServiceOption {
	return serviceOptFunc(func(s *Service) { s.description = &text })
}

// Targeted declares that every call must name the instance it is for: the
// instances hold distinct state (a session, a loaded model, an actor).
func Targeted() ServiceOption {
	return serviceOptFunc(func(s *Service) { s.targeted = true })
}

// DefaultBalance sets the balance policy of methods that do not set one.
func DefaultBalance(p BalancePolicy) ServiceOption {
	return serviceOptFunc(func(s *Service) { s.balance = p })
}

// SourceModule names the module recorded in the contract (default: the Go
// package of the first request type).
func SourceModule(module string) ServiceOption {
	return serviceOptFunc(func(s *Service) { s.module = module })
}

// WithMethodOptions attaches MethodOption values (e.g. ReadOnly(), Idempotent(),
// Doc("...")) to a named method on a reflected service (using either the Go
// method name or the snake_case wire name).
func WithMethodOptions(methodName string, opts ...MethodOption) ServiceOption {
	return serviceOptFunc(func(s *Service) {
		if s.methodOpts == nil {
			s.methodOpts = map[string][]MethodOption{}
		}
		s.methodOpts[methodName] = append(s.methodOpts[methodName], opts...)
	})
}

var (
	nameSegment    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
	majorSegment   = regexp.MustCompile(`^v(0|[1-9][0-9]*)$`)
	numericSegment = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
	patchSegment   = regexp.MustCompile(`^(0|[1-9][0-9]*)(-[0-9A-Za-z-]+)?$`)
)

// NewService creates a service. The name carries one version segment
// ("chat.v1", "demo.v1.Greeting", "chat.v1.2.0"); without one, ".v0" is
// appended. It panics on an invalid name, like other declaration errors.
func NewService(name string, opts ...ServiceOption) *Service {
	full, version, err := normalizeServiceName(name)
	if err != nil {
		panic("meshcall: " + err.Error())
	}
	s := &Service{
		name:       full,
		version:    version,
		balance:    RoundRobin,
		byName:     map[string]*method{},
		methodOpts: map[string][]MethodOption{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyService(s)
		}
	}
	return s
}

// Name is the service's normalised name.
func (s *Service) Name() string { return s.name }

// normalizeServiceName applies docs/protocol.md → Service names.
func normalizeServiceName(name string) (string, string, error) {
	segments := strings.Split(name, ".")
	var normalized []string
	version, names := "", 0
	for i := 0; i < len(segments); {
		segment := segments[i]
		if majorSegment.MatchString(segment) {
			if version != "" {
				return "", "", fmt.Errorf("service name %q has more than one version segment", name)
			}
			if i+2 < len(segments) && numericSegment.MatchString(segments[i+1]) && patchSegment.MatchString(segments[i+2]) {
				version = segment[1:] + "." + segments[i+1] + "." + segments[i+2]
				normalized = append(normalized, segments[i:i+3]...)
				i += 3
			} else {
				version = segment[1:]
				normalized = append(normalized, segment)
				i++
			}
			continue
		}
		if !nameSegment.MatchString(segment) {
			return "", "", fmt.Errorf("invalid service name %q: segment %q must start with a letter and contain only letters, digits and underscores (a version looks like 'v1' or 'v1.2.0')", name, segment)
		}
		normalized = append(normalized, segment)
		names++
		i++
	}
	if names == 0 {
		return "", "", fmt.Errorf("service name %q needs a package or service segment", name)
	}
	if version == "" {
		version = "0"
		normalized = append(normalized, "v0")
	}
	return strings.Join(normalized, "."), version, nil
}

// --- methods ---------------------------------------------------------------

type streamKind string

const (
	unaryShape        streamKind = "unary"
	serverStreamShape streamKind = "server_stream"
	clientStreamShape streamKind = "client_stream"
	duplexShape       streamKind = "duplex"
)

type method struct {
	name            string
	stream          streamKind
	request         reflect.Type
	response        reflect.Type // nil for streams
	input           reflect.Type // client stream / duplex
	output          reflect.Type // server stream / duplex
	parameters      []string
	requestQualname string
	reflected       bool
	opts            methodOptions
	duplexReason    string
	schemas         struct{ request, response, input, output *orderedObject }
	invoke          func(ctx *Context, call *serverCall, request json.RawMessage) (any, error)
}

type methodOptions struct {
	description *string
	idempotent  bool
	readOnly    bool
	destructive bool
	deprecated  *string
	balance     *BalancePolicy
}

// MethodOption configures a method.
type MethodOption func(*methodOptions)

// Doc sets the method's description; "" declares none on purpose.
func Doc(text string) MethodOption { return func(o *methodOptions) { o.description = &text } }

// Idempotent declares that repeating the call is safe (enables retries).
func Idempotent() MethodOption { return func(o *methodOptions) { o.idempotent = true } }

// ReadOnly declares that the call changes nothing (implies Idempotent).
func ReadOnly() MethodOption { return func(o *methodOptions) { o.readOnly, o.idempotent = true, true } }

// Destructive declares irreversible side effects; tools ask before calling.
func Destructive() MethodOption { return func(o *methodOptions) { o.destructive = true } }

// Deprecated marks the method, naming its replacement.
func Deprecated(replacement string) MethodOption {
	return func(o *methodOptions) { o.deprecated = &replacement }
}

// Balance sets this method's balance policy.
func Balance(p BalancePolicy) MethodOption { return func(o *methodOptions) { o.balance = &p } }

func (s *Service) add(m *method, opts []MethodOption) {
	for _, opt := range opts {
		opt(&m.opts)
	}
	if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(m.name) {
		panic(fmt.Sprintf("meshcall: invalid method name %q", m.name))
	}
	if s.byName[m.name] != nil {
		panic(fmt.Sprintf("meshcall: %s declares method %q twice", s.name, m.name))
	}
	if m.opts.readOnly && m.opts.destructive {
		panic(fmt.Sprintf("meshcall: %s.%s cannot be both read-only and destructive", s.name, m.name))
	}
	if m.stream == duplexShape && (m.opts.idempotent || m.opts.readOnly) {
		panic(fmt.Sprintf("meshcall: duplex method %s.%s cannot be idempotent or read-only: a duplex call is never replayed", s.name, m.name))
	}
	if !m.reflected {
		for _, t := range []reflect.Type{m.request, m.response, m.input, m.output} {
			if t != nil && structType(t) == nil {
				panic(fmt.Sprintf("meshcall: %s.%s: request, response and item types must be structs, got %s", s.name, m.name, t))
			}
		}
		m.schemas.request = schemaOf(m.request)
		if m.response != nil {
			m.schemas.response = schemaOf(m.response)
		}
		if m.input != nil {
			m.schemas.input = schemaOf(m.input)
		}
		if m.output != nil {
			m.schemas.output = schemaOf(m.output)
		}
		if s.module == "" && m.request != nil && structType(m.request) != nil {
			s.module = structType(m.request).PkgPath()
		}
	}
	s.methods = append(s.methods, m)
	s.byName[m.name] = m
}

func structType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	return t
}

func typeOf[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

// decodeRequest validates and decodes a request or input item.
func decodeInto[T any](schema *orderedObject, raw json.RawMessage, reason, message string) (T, error) {
	var value T
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	if vs := validate(schema, raw); len(vs) > 0 {
		e := detailed(CodeInvalidArgument, message, "server", reason, false)
		e.Details["violations"] = violationList(vs)
		return value, e
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		e := detailed(CodeInvalidArgument, message, "server", reason, false)
		e.Details["violations"] = []any{map[string]any{"path": "", "code": "type", "message": "Input does not decode"}}
		return value, e
	}
	return value, nil
}

// encodeOutput serialises a response or output item and checks it against
// the method's own contract: a mismatch is the service's bug.
func encodeOutput(schema *orderedObject, value any, reason string, log func(string, ...any)) (json.RawMessage, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		log("cannot encode %s: %v", reason, err)
		return nil, detailed(CodeInternal, "Internal service error", "server", reason, false)
	}
	if vs := validate(schema, raw); len(vs) > 0 {
		log("produced an invalid %s: %v", strings.ReplaceAll(strings.TrimPrefix(reason, "invalid_"), "_", " "), vs)
		return nil, detailed(CodeInternal, "Internal service error", "server", reason, false)
	}
	return raw, nil
}

// HandleUnary adds a request–response method.
func HandleUnary[Req, Resp any](s *Service, name string, handler func(ctx *Context, req Req) (Resp, error), opts ...MethodOption) {
	m := &method{name: name, stream: unaryShape, request: typeOf[Req](), response: typeOf[Resp]()}
	m.invoke = func(ctx *Context, call *serverCall, raw json.RawMessage) (any, error) {
		req, err := decodeInto[Req](m.schemas.request, raw, "request", "Invalid request")
		if err != nil {
			return nil, err
		}
		return call.intercept(ctx, req, func() (any, error) { return handler(ctx, req) })
	}
	s.add(m, opts)
}

// HandleServerStream adds a method that streams items to the caller. The
// handler sends items with send, which waits for the caller's credit, and
// returns when the stream is complete.
func HandleServerStream[Req, Item any](s *Service, name string, handler func(ctx *Context, req Req, send func(Item) error) error, opts ...MethodOption) {
	m := &method{name: name, stream: serverStreamShape, request: typeOf[Req](), output: typeOf[Item]()}
	m.invoke = func(ctx *Context, call *serverCall, raw json.RawMessage) (any, error) {
		req, err := decodeInto[Req](m.schemas.request, raw, "request", "Invalid request")
		if err != nil {
			return nil, err
		}
		return call.intercept(ctx, req, func() (any, error) {
			return nil, handler(ctx, req, func(item Item) error { return call.sendOutput(item) })
		})
	}
	s.add(m, opts)
}

// Input is the stream of items a client-stream or duplex handler reads.
type Input[Item any] struct {
	call   *serverCall
	schema *orderedObject
}

// Recv returns the next item, or io.EOF once the caller has finished sending.
func (in *Input[Item]) Recv() (Item, error) {
	var zero Item
	raw, err := in.call.nextInput()
	if err != nil {
		return zero, err
	}
	return decodeInto[Item](in.schema, raw, "input_item", "Invalid input stream item")
}

// All ranges over the input: for item, err := range items.All() { … }.
func (in *Input[Item]) All() func(yield func(Item, error) bool) {
	return func(yield func(Item, error) bool) {
		for {
			item, err := in.Recv()
			if errors.Is(err, errEndOfInput) {
				return
			}
			if !yield(item, err) || err != nil {
				return
			}
		}
	}
}

// HandleClientStream adds a method that reads a stream of items and answers once.
func HandleClientStream[Req, Item, Resp any](s *Service, name string, handler func(ctx *Context, req Req, items *Input[Item]) (Resp, error), opts ...MethodOption) {
	m := &method{name: name, stream: clientStreamShape, request: typeOf[Req](), input: typeOf[Item](), response: typeOf[Resp]()}
	m.invoke = func(ctx *Context, call *serverCall, raw json.RawMessage) (any, error) {
		req, err := decodeInto[Req](m.schemas.request, raw, "request", "Invalid request")
		if err != nil {
			return nil, err
		}
		return call.intercept(ctx, req, func() (any, error) {
			return handler(ctx, req, &Input[Item]{call: call, schema: m.schemas.input})
		})
	}
	s.add(m, opts)
}

// Channel is what a duplex handler receives: Recv input items while Send-ing
// output items. Returning from the handler closes the output and ends the call.
type Channel[In, Out any] struct {
	Input[In]
}

// Send sends one output item, waiting for the caller's credit.
func (c *Channel[In, Out]) Send(item Out) error { return c.call.sendOutput(item) }

// CloseSend closes the output while input is still being read.
func (c *Channel[In, Out]) CloseSend() error { return c.call.closeOutput() }

// HandleDuplex adds a duplex method — the shape of last resort (docs/protocol.md
// → Duplex calls). reason says why a server stream plus unary calls, or a
// client stream, will not do; it goes into the contract.
func HandleDuplex[Req, In, Out any](s *Service, name, reason string, handler func(ctx *Context, req Req, ch *Channel[In, Out]) error, opts ...MethodOption) {
	if strings.TrimSpace(reason) == "" {
		panic("meshcall: HandleDuplex needs a reason: say why a server stream plus unary calls, or a client stream, will not do")
	}
	m := &method{name: name, stream: duplexShape, request: typeOf[Req](), input: typeOf[In](), output: typeOf[Out](), duplexReason: strings.TrimSpace(reason)}
	m.invoke = func(ctx *Context, call *serverCall, raw json.RawMessage) (any, error) {
		req, err := decodeInto[Req](m.schemas.request, raw, "request", "Invalid request")
		if err != nil {
			return nil, err
		}
		return call.intercept(ctx, req, func() (any, error) {
			return nil, handler(ctx, req, &Channel[In, Out]{Input: Input[In]{call: call, schema: m.schemas.input}})
		})
	}
	s.add(m, opts)
}

// --- contract --------------------------------------------------------------

func typeRef(t reflect.Type, schema *orderedObject, module, qualnameOverride string) *orderedObject {
	pkg := module
	qname := qualnameOverride
	if t != nil {
		if st := structType(t); st != nil {
			if st.PkgPath() != "" {
				pkg = st.PkgPath()
			}
			if qname == "" {
				qname = st.Name()
			}
		} else {
			if t.PkgPath() != "" {
				pkg = t.PkgPath()
			}
			if qname == "" {
				if t.Name() != "" {
					qname = t.Name()
				} else {
					qname = t.String()
				}
			}
		}
	}
	return newObject().set("module", pkg).set("qualname", qname).set("schema", schema)
}

// Contract returns the service's portable contract entry, the object a
// *.meshcall.json document holds (docs/protocol.md → Frames).
func (s *Service) Contract() json.RawMessage {
	methods := []any{}
	for _, m := range s.methods {
		var params []any
		if m.reflected {
			for _, p := range m.parameters {
				params = append(params, p)
			}
		} else if m.request != nil && structType(m.request) != nil {
			for _, f := range fieldsOf(structType(m.request)) {
				params = append(params, f.name)
			}
		}
		if params == nil {
			params = []any{}
		}
		ref := func(t reflect.Type, schema *orderedObject, qname string) any {
			if t == nil && schema == nil {
				return nil
			}
			return typeRef(t, schema, s.module, qname)
		}
		balance := s.balance
		if m.opts.balance != nil {
			balance = *m.opts.balance
		}
		entry := newObject().
			set("name", m.name).
			set("stream", string(m.stream)).
			set("request", ref(m.request, m.schemas.request, m.requestQualname)).
			set("parameters", params).
			set("response", ref(m.response, m.schemas.response, "")).
			set("input_item", ref(m.input, m.schemas.input, "")).
			set("output_item", ref(m.output, m.schemas.output, "")).
			set("balance", balance).
			set("binding", "instance").
			set("description", m.opts.description).
			set("annotations", newObject().
				set("idempotent", m.opts.idempotent).
				set("read_only", m.opts.readOnly).
				set("destructive", m.opts.destructive)).
			set("deprecated", m.opts.deprecated)
		if m.duplexReason != "" {
			entry.set("duplex_reason", m.duplexReason)
		} else {
			entry.set("duplex_reason", nil)
		}
		methods = append(methods, entry)
	}
	qualname := s.name
	if i := strings.LastIndexByte(qualname, '.'); i >= 0 {
		qualname = qualname[i+1:]
	}
	doc := newObject().
		set("name", s.name).
		set("version", s.version).
		set("source_module", s.module).
		set("source_qualname", qualname).
		set("balance", s.balance).
		set("description", s.description).
		set("targeted", s.targeted).
		set("methods", methods)
	raw, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return raw
}

// WriteContract writes a contract document for services to path, for
// `meshcall generate path --language …`.
func WriteContract(path string, services ...*Service) error {
	entries := make([]json.RawMessage, len(services))
	for i, s := range services {
		entries[i] = s.Contract()
	}
	doc, err := json.MarshalIndent(map[string]any{"protocol": Protocol, "services": entries}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(doc, '\n'), 0o644)
}

func (s *Service) registration() registration {
	r := registration{Name: s.name, Targeted: s.targeted, Contract: s.Contract()}
	for _, m := range s.methods {
		balance := s.balance
		if m.opts.balance != nil {
			balance = *m.opts.balance
		}
		r.Methods = append(r.Methods, methodShape{Name: m.name, Stream: string(m.stream), Balance: balance})
	}
	return r
}

// ServerCallNext runs the rest of the server chain: later interceptors, then
// the handler. It returns the handler's result (nil for streams).
type ServerCallNext func() (any, error)

// ServerInterceptor wraps every call on a server; the first one given is
// outermost. request is the validated request.
type ServerInterceptor func(ctx *Context, request any, next ServerCallNext) (any, error)

var _ = context.Background
