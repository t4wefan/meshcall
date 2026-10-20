package meshcall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"iter"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// ServiceMeta is embedded in a struct to declare a MeshCall service via struct
// tags (`name:"workspace" description:"..." targeted:"true" balance:"round_robin"`).
type ServiceMeta struct{}

type duplexChannelParam interface {
	duplexInOutTypes() (inType, outType reflect.Type)
	newDuplexChannelVal(call *serverCall, inSchema *orderedObject) reflect.Value
}

// DuplexChannel is the bidirectional stream passed to reflected Go duplex handlers:
// ordinary business parameters come first, and *DuplexChannel[In, Out] comes last.
type DuplexChannel[In, Out any] struct {
	call   *serverCall
	schema *orderedObject
}

func (*DuplexChannel[In, Out]) duplexInOutTypes() (reflect.Type, reflect.Type) {
	return typeOf[In](), typeOf[Out]()
}

func (*DuplexChannel[In, Out]) newDuplexChannelVal(call *serverCall, inSchema *orderedObject) reflect.Value {
	return reflect.ValueOf(&DuplexChannel[In, Out]{call: call, schema: inSchema})
}

func (*Channel[In, Out]) duplexInOutTypes() (reflect.Type, reflect.Type) {
	return typeOf[In](), typeOf[Out]()
}

func (*Channel[In, Out]) newDuplexChannelVal(call *serverCall, inSchema *orderedObject) reflect.Value {
	return reflect.ValueOf(&Channel[In, Out]{Input: Input[In]{call: call, schema: inSchema}})
}

// Send sends one output item to the caller, waiting for stream credit.
func (c *DuplexChannel[In, Out]) Send(ctx context.Context, item Out) error {
	if ctx != nil && ctx.Err() != nil {
		return contextError(ctx)
	}
	return c.call.sendOutput(item)
}

// Recv returns an iter.Seq2[In, error] over incoming items from the caller.
func (c *DuplexChannel[In, Out]) Recv(ctx context.Context) iter.Seq2[In, error] {
	return func(yield func(In, error) bool) {
		var zero In
		for {
			if ctx != nil && ctx.Err() != nil {
				yield(zero, contextError(ctx))
				return
			}
			raw, err := c.call.nextInput()
			if err != nil {
				if err == errEndOfInput {
					return
				}
				yield(zero, err)
				return
			}
			item, decErr := decodeInto[In](c.schema, raw, "input_item", "Invalid input stream item")
			if !yield(item, decErr) || decErr != nil {
				return
			}
		}
	}
}

// CloseSend half-closes the server output stream while input may still be read.
func (c *DuplexChannel[In, Out]) CloseSend() error {
	return c.call.closeOutput()
}

// SliceSeq converts a slice into an iter.Seq2[T, error] suitable for client-stream
// or server-stream helpers.
func SliceSeq[T any](items []T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, item := range items {
			if !yield(item, nil) {
				return
			}
		}
	}
}

// SliceItems converts a slice into a func(yield func(T) bool) suitable for ClientStream.
func SliceItems[T any](items []T) func(yield func(T) bool) {
	return func(yield func(T) bool) {
		for _, item := range items {
			if !yield(item) {
				return
			}
		}
	}
}

// --- Dependency Injection (Dependency & Meta) ------------------------------

// DependencyProvider is implemented by values returned from Dependency(...) and Meta[T](...).
type DependencyProvider interface {
	outputType() reflect.Type
	isMeta() bool
}

type depSpec struct {
	outType     reflect.Type
	isMetaDep   bool
	metaKey     string
	metaReq     bool
	hasDefault  bool
	defaultVal  reflect.Value
	fnVal       reflect.Value
	depTypes    []reflect.Type
	takesStdCtx bool
	takesRpcCtx bool
}

func (d *depSpec) outputType() reflect.Type { return d.outType }
func (d *depSpec) isMeta() bool             { return d.isMetaDep }

var (
	stdCtxType = reflect.TypeOf((*context.Context)(nil)).Elem()
	rpcCtxType = reflect.TypeOf((*Context)(nil))
	errType    = reflect.TypeOf((*error)(nil)).Elem()
)

// Dependency registers a per-call dependency provider function.
//
// Supported provider signatures:
//   - func([ctx], ...deps) T
//   - func([ctx], ...deps) (T, error)
//   - func([ctx], ...deps) (T, func(), error)
//   - func([ctx], ...deps) (T, func() error, error)
//
// where the optional first parameter is context.Context or *meshcall.Context,
// and any subsequent parameters are other injected dependency types.
func Dependency(provider any) DependencyProvider {
	if provider == nil {
		panic("meshcall: Dependency provider cannot be nil")
	}
	fnVal := reflect.ValueOf(provider)
	fnType := fnVal.Type()
	if fnType.Kind() != reflect.Func {
		panic(fmt.Sprintf("meshcall: Dependency expects a function, got %s", fnType))
	}
	if fnType.NumOut() < 1 || fnType.NumOut() > 3 {
		panic(fmt.Sprintf("meshcall: Dependency provider %s must return 1, 2, or 3 values", fnType))
	}
	outType := fnType.Out(0)
	if fnType.NumOut() == 2 && !fnType.Out(1).Implements(errType) {
		panic(fmt.Sprintf("meshcall: second return value of Dependency provider %s must be error", fnType))
	}
	if fnType.NumOut() == 3 {
		cleanupType := fnType.Out(1)
		if cleanupType.Kind() != reflect.Func || cleanupType.NumIn() != 0 || cleanupType.NumOut() > 1 || (cleanupType.NumOut() == 1 && !cleanupType.Out(0).Implements(errType)) {
			panic(fmt.Sprintf("meshcall: second return value of 3-return Dependency provider %s must be func() or func() error", fnType))
		}
		if !fnType.Out(2).Implements(errType) {
			panic(fmt.Sprintf("meshcall: third return value of Dependency provider %s must be error", fnType))
		}
	}

	spec := &depSpec{
		outType: outType,
		fnVal:   fnVal,
	}
	startIdx := 0
	if fnType.NumIn() > 0 {
		first := fnType.In(0)
		if first == rpcCtxType {
			spec.takesRpcCtx = true
			startIdx = 1
		} else if first == stdCtxType {
			spec.takesStdCtx = true
			startIdx = 1
		}
	}
	for i := startIdx; i < fnType.NumIn(); i++ {
		spec.depTypes = append(spec.depTypes, fnType.In(i))
	}
	return spec
}

// MetaOption configures a Meta[T] header dependency.
type MetaOption func(*metaConfig)

type metaConfig struct {
	required   bool
	hasDefault bool
	defaultVal any
}

// WithMetaRequired marks a metadata header as required; calls missing the
// header fail fast with invalid_argument before acquiring other dependencies.
func WithMetaRequired() MetaOption {
	return func(c *metaConfig) { c.required = true }
}

// WithMetaDefault sets a default value when the metadata header is absent.
func WithMetaDefault[T any](value T) MetaOption {
	return func(c *metaConfig) {
		c.hasDefault = true
		c.defaultVal = value
	}
}

// Meta declares a per-call metadata header extraction keyed by type T.
// Define a distinct type (e.g. `type CallerID string`) so multiple metadata
// headers can be injected by type.
func Meta[T any](key string, opts ...MetaOption) DependencyProvider {
	trimmed := strings.ToLower(strings.TrimSpace(key))
	if trimmed == "" {
		panic("meshcall: Meta header key must be a non-empty string")
	}
	cfg := metaConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	outType := typeOf[T]()
	spec := &depSpec{
		outType:    outType,
		isMetaDep:  true,
		metaKey:    trimmed,
		metaReq:    cfg.required && !cfg.hasDefault,
		hasDefault: cfg.hasDefault,
	}
	if cfg.hasDefault {
		spec.defaultVal = reflect.ValueOf(cfg.defaultVal)
	}
	return spec
}

type dependencyRegistry struct {
	byType map[reflect.Type]*depSpec
}

func newDependencyRegistry(providers []DependencyProvider) (*dependencyRegistry, error) {
	reg := &dependencyRegistry{
		byType: make(map[reflect.Type]*depSpec, len(providers)),
	}
	for _, p := range providers {
		if p == nil {
			continue
		}
		spec, ok := p.(*depSpec)
		if !ok {
			return nil, fmt.Errorf("meshcall: unsupported DependencyProvider %T", p)
		}
		reg.byType[spec.outType] = spec
	}
	// Detect circular dependencies at registration time.
	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)
	state := make(map[reflect.Type]int, len(reg.byType))
	var stack []reflect.Type
	var dfs func(t reflect.Type) error
	dfs = func(t reflect.Type) error {
		state[t] = visiting
		stack = append(stack, t)
		spec := reg.byType[t]
		if spec != nil && !spec.isMetaDep {
			for _, sub := range spec.depTypes {
				switch state[sub] {
				case visiting:
					var names []string
					for _, st := range stack {
						names = append(names, st.String())
					}
					names = append(names, sub.String())
					return fmt.Errorf("meshcall: circular dependency detected: %s", strings.Join(names, " -> "))
				case unvisited:
					if err := dfs(sub); err != nil {
						return err
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[t] = visited
		return nil
	}
	for t := range reg.byType {
		if state[t] == unvisited {
			if err := dfs(t); err != nil {
				return nil, err
			}
		}
	}
	return reg, nil
}

func (r *dependencyRegistry) has(t reflect.Type) bool {
	if r == nil {
		return false
	}
	_, ok := r.byType[t]
	return ok
}

type depScope struct {
	ctx      *Context
	reg      *dependencyRegistry
	cache    map[reflect.Type]reflect.Value
	cleanups []func()
}

func newDepScope(ctx *Context, reg *dependencyRegistry) *depScope {
	return &depScope{
		ctx:   ctx,
		reg:   reg,
		cache: make(map[reflect.Type]reflect.Value),
	}
}

func (s *depScope) close() {
	for i := len(s.cleanups) - 1; i >= 0; i-- {
		s.cleanups[i]()
	}
	s.cleanups = nil
}

func (s *depScope) resolveAll(types []reflect.Type) ([]reflect.Value, error) {
	// Fast-fail metadata headers first before acquiring any regular dependencies.
	for _, t := range types {
		spec := s.reg.byType[t]
		if spec != nil && spec.isMetaDep {
			if _, err := s.resolve(t); err != nil {
				return nil, err
			}
		}
	}
	out := make([]reflect.Value, len(types))
	for i, t := range types {
		val, err := s.resolve(t)
		if err != nil {
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}

func (s *depScope) resolve(t reflect.Type) (reflect.Value, error) {
	if cached, ok := s.cache[t]; ok {
		return cached, nil
	}
	spec := s.reg.byType[t]
	if spec == nil {
		return reflect.Value{}, detailed(CodeInternal, fmt.Sprintf("Unregistered dependency type: %s", t), "server", "dependency", false)
	}
	if spec.isMetaDep {
		val, err := s.resolveMeta(spec)
		if err != nil {
			return reflect.Value{}, err
		}
		s.cache[t] = val
		return val, nil
	}
	// Resolve sub-dependencies first (meta sub-deps first)
	for _, sub := range spec.depTypes {
		if subSpec := s.reg.byType[sub]; subSpec != nil && subSpec.isMetaDep {
			if _, err := s.resolve(sub); err != nil {
				return reflect.Value{}, err
			}
		}
	}
	var args []reflect.Value
	if spec.takesRpcCtx {
		args = append(args, reflect.ValueOf(s.ctx))
	} else if spec.takesStdCtx {
		args = append(args, reflect.ValueOf(context.Context(s.ctx)))
	}
	for _, sub := range spec.depTypes {
		subVal, err := s.resolve(sub)
		if err != nil {
			return reflect.Value{}, err
		}
		args = append(args, subVal)
	}
	results := spec.fnVal.Call(args)
	switch len(results) {
	case 1:
		val := results[0]
		s.cache[t] = val
		return val, nil
	case 2:
		if !results[1].IsNil() {
			return reflect.Value{}, results[1].Interface().(error)
		}
		val := results[0]
		s.cache[t] = val
		return val, nil
	case 3:
		if !results[2].IsNil() {
			return reflect.Value{}, results[2].Interface().(error)
		}
		cleanupVal := results[1]
		if !cleanupVal.IsNil() {
			s.cleanups = append(s.cleanups, func() {
				cleanupVal.Call(nil)
			})
		}
		val := results[0]
		s.cache[t] = val
		return val, nil
	default:
		return reflect.Value{}, detailed(CodeInternal, "Invalid dependency return arity", "server", "dependency", false)
	}
}

func (s *depScope) resolveMeta(spec *depSpec) (reflect.Value, error) {
	var raw string
	var found bool
	if s.ctx != nil && s.ctx.Metadata != nil {
		if v, ok := s.ctx.Metadata[spec.metaKey]; ok {
			raw, found = v, true
		} else {
			for k, val := range s.ctx.Metadata {
				if strings.EqualFold(k, spec.metaKey) {
					raw, found = val, true
					break
				}
			}
		}
	}
	if !found {
		if spec.hasDefault {
			return spec.defaultVal, nil
		}
		if spec.metaReq {
			e := detailed(CodeInvalidArgument, fmt.Sprintf("Missing required metadata header: %s", spec.metaKey), "server", "metadata", false)
			e.Details["header"] = spec.metaKey
			return reflect.Value{}, e
		}
		return reflect.Zero(spec.outType), nil
	}
	return convertMetaValue(raw, spec.outType, spec.metaKey)
}

func convertMetaValue(raw string, targetType reflect.Type, headerKey string) (reflect.Value, error) {
	if targetType.Kind() == reflect.Pointer {
		elemVal, err := convertMetaValue(raw, targetType.Elem(), headerKey)
		if err != nil {
			return reflect.Value{}, err
		}
		ptr := reflect.New(targetType.Elem())
		ptr.Elem().Set(elemVal)
		return ptr, nil
	}
	switch targetType.Kind() {
	case reflect.String:
		v := reflect.New(targetType).Elem()
		v.SetString(raw)
		return v, nil
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			e := detailed(CodeInvalidArgument, fmt.Sprintf("Invalid boolean metadata header %q", headerKey), "server", "metadata", false)
			e.Details["header"] = headerKey
			return reflect.Value{}, e
		}
		v := reflect.New(targetType).Elem()
		v.SetBool(b)
		return v, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, targetType.Bits())
		if err != nil {
			e := detailed(CodeInvalidArgument, fmt.Sprintf("Invalid integer metadata header %q", headerKey), "server", "metadata", false)
			e.Details["header"] = headerKey
			return reflect.Value{}, e
		}
		v := reflect.New(targetType).Elem()
		v.SetInt(n)
		return v, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, targetType.Bits())
		if err != nil {
			e := detailed(CodeInvalidArgument, fmt.Sprintf("Invalid unsigned integer metadata header %q", headerKey), "server", "metadata", false)
			e.Details["header"] = headerKey
			return reflect.Value{}, e
		}
		v := reflect.New(targetType).Elem()
		v.SetUint(n)
		return v, nil
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, targetType.Bits())
		if err != nil {
			e := detailed(CodeInvalidArgument, fmt.Sprintf("Invalid float metadata header %q", headerKey), "server", "metadata", false)
			e.Details["header"] = headerKey
			return reflect.Value{}, e
		}
		v := reflect.New(targetType).Elem()
		v.SetFloat(f)
		return v, nil
	default:
		ptr := reflect.New(targetType)
		if err := json.Unmarshal([]byte(raw), ptr.Interface()); err != nil {
			e := detailed(CodeInvalidArgument, fmt.Sprintf("Invalid JSON metadata header %q", headerKey), "server", "metadata", false)
			e.Details["header"] = headerKey
			return reflect.Value{}, e
		}
		return ptr.Elem(), nil
	}
}

// WithDependencies registers Dependency(...) and Meta[T](...) providers on a
// Server (via Serve/Run) or a reflected Service.
func WithDependencies(deps ...DependencyProvider) interface {
	ServeOption
	ServiceOption
} {
	return depOption(deps)
}

type depOption []DependencyProvider

func (d depOption) applyServe(s *serveConfig) {
	s.dependencies = append(s.dependencies, d...)
}

func (d depOption) applyService(s *Service) {
	s.dependencies = append(s.dependencies, d...)
}

// --- AST source inspection for Go method parameter names & docstrings ------

type parsedFuncInfo struct {
	paramNames []string
	doc        string
}

var (
	astCacheMu sync.Mutex
	astCache   = map[string]map[string]parsedFuncInfo{}
)

func inspectMethodSource(fnPtr uintptr, recvTypeName, methodName string) parsedFuncInfo {
	if fnPtr == 0 {
		return parsedFuncInfo{}
	}
	fn := runtime.FuncForPC(fnPtr)
	if fn == nil {
		return parsedFuncInfo{}
	}
	file, _ := fn.FileLine(fnPtr)
	if file == "" {
		return parsedFuncInfo{}
	}
	astCacheMu.Lock()
	defer astCacheMu.Unlock()
	fileMethods, ok := astCache[file]
	if !ok {
		fileMethods = parseGoFileMethods(file)
		astCache[file] = fileMethods
	}
	if info, ok := fileMethods[recvTypeName+"."+methodName]; ok {
		return info
	}
	if info, ok := fileMethods[methodName]; ok {
		return info
	}
	return parsedFuncInfo{}
}

func parseGoFileMethods(file string) map[string]parsedFuncInfo {
	out := map[string]parsedFuncInfo{}
	src, err := os.ReadFile(file)
	if err != nil {
		return out
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, src, parser.ParseComments)
	if err != nil {
		return out
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil {
			continue
		}
		var names []string
		if fn.Type != nil && fn.Type.Params != nil {
			for _, field := range fn.Type.Params.List {
				if len(field.Names) == 0 {
					names = append(names, "")
				} else {
					for _, ident := range field.Names {
						names = append(names, ident.Name)
					}
				}
			}
		}
		var doc string
		if fn.Doc != nil {
			doc = strings.TrimSpace(fn.Doc.Text())
		}
		info := parsedFuncInfo{paramNames: names, doc: doc}
		out[fn.Name.Name] = info
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			recvName := exprTypeName(fn.Recv.List[0].Type)
			if recvName != "" {
				out[recvName+"."+fn.Name.Name] = info
			}
		}
	}
	return out
}

func exprTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.StarExpr:
		return exprTypeName(e.X)
	case *ast.IndexExpr:
		return exprTypeName(e.X)
	case *ast.IndexListExpr:
		return exprTypeName(e.X)
	default:
		return ""
	}
}

// --- Struct Reflection Service Registration --------------------------------

var (
	serviceMetaType        = reflect.TypeOf(ServiceMeta{})
	duplexChannelParamType = reflect.TypeOf((*duplexChannelParam)(nil)).Elem()
)

func hasServiceMeta(v any) bool {
	if v == nil {
		return false
	}
	st := structType(reflect.TypeOf(v))
	if st == nil {
		return false
	}
	_, ok := findServiceMetaTag(st)
	return ok
}

func findServiceMetaTag(st reflect.Type) (reflect.StructTag, bool) {
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		if f.Type == serviceMetaType {
			return f.Tag, true
		}
	}
	return "", false
}

// seq2ElemType detects iter.Seq2[Item, error] (func(yield func(Item, error) bool)).
func seq2ElemType(t reflect.Type) (reflect.Type, bool) {
	if t == nil || t.Kind() != reflect.Func || t.NumIn() != 1 || t.NumOut() != 0 {
		return nil, false
	}
	y := t.In(0)
	if y.Kind() != reflect.Func || y.NumIn() != 2 || y.NumOut() != 1 || y.Out(0).Kind() != reflect.Bool {
		return nil, false
	}
	if y.In(1) != errType {
		return nil, false
	}
	return y.In(0), true
}

func camelToSnake(s string) string {
	if s == "" {
		return ""
	}
	runes := []rune(s)
	var out []rune
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if prev != '_' && (unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower)) {
					out = append(out, '_')
				}
			}
			out = append(out, unicode.ToLower(r))
		} else {
			out = append(out, r)
		}
	}
	return string(out)
}

func parseBalanceTag(tag string) (BalancePolicy, error) {
	tag = strings.TrimSpace(tag)
	switch tag {
	case "", "round_robin":
		return RoundRobin, nil
	case "least_inflight":
		return LeastInflight, nil
	case "random":
		return Random, nil
	case "disabled", "none":
		return NoBalancing, nil
	default:
		if field, ok := strings.CutPrefix(tag, "sticky:"); ok && strings.TrimSpace(field) != "" {
			return Sticky(strings.TrimSpace(field)), nil
		}
		return BalancePolicy{}, fmt.Errorf("unknown balance policy %q", tag)
	}
}

// MustReflectService reflects a struct embedding ServiceMeta into a *Service
// and panics on invalid declarations.
func MustReflectService(instance any, opts ...ServiceOption) *Service {
	svc, err := ReflectService(instance, opts...)
	if err != nil {
		panic(err)
	}
	return svc
}

// ReflectService reflects a struct (or struct pointer) embedding ServiceMeta
// into a *Service, classifying exported methods into Unary, Server Stream
// (returning iter.Seq2[T, error]), Client Stream (trailing iter.Seq2[T, error]),
// and Duplex (trailing *DuplexChannel[In, Out]), and wiring per-call DI & Meta.
func ReflectService(instance any, opts ...ServiceOption) (*Service, error) {
	if instance == nil {
		return nil, errors.New("meshcall: ReflectService instance cannot be nil")
	}
	val := reflect.ValueOf(instance)
	st := structType(val.Type())
	if st == nil {
		return nil, fmt.Errorf("meshcall: ReflectService expects a struct or struct pointer, got %T", instance)
	}
	metaTag, ok := findServiceMetaTag(st)
	if !ok {
		return nil, fmt.Errorf("meshcall: struct %s must embed meshcall.ServiceMeta", st.Name())
	}

	rawName := strings.TrimSpace(metaTag.Get("name"))
	if rawName == "" {
		rawName = camelToSnake(st.Name())
	}
	full, version, err := normalizeServiceName(rawName)
	if err != nil {
		return nil, fmt.Errorf("meshcall: %w", err)
	}
	svc := &Service{
		name:       full,
		qualname:   st.Name(),
		version:    version,
		balance:    RoundRobin,
		module:     st.PkgPath(),
		byName:     map[string]*method{},
		methodOpts: map[string][]MethodOption{},
	}
	if desc, ok := metaTag.Lookup("description"); ok {
		svc.description = &desc
	} else if doc, ok := metaTag.Lookup("doc"); ok {
		svc.description = &doc
	}
	if targetedTag := strings.ToLower(strings.TrimSpace(metaTag.Get("targeted"))); targetedTag == "true" || targetedTag == "1" {
		svc.targeted = true
	}
	if balTag := metaTag.Get("balance"); balTag != "" {
		bal, err := parseBalanceTag(balTag)
		if err != nil {
			return nil, fmt.Errorf("meshcall: %s: %w", st.Name(), err)
		}
		svc.balance = bal
	}
	if modTag := strings.TrimSpace(metaTag.Get("module")); modTag != "" {
		svc.module = modTag
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyService(svc)
		}
	}

	depReg, err := newDependencyRegistry(svc.dependencies)
	if err != nil {
		return nil, err
	}

	// Ensure we have a pointer value so pointer-receiver methods are included.
	recvVal := val
	if recvVal.Kind() != reflect.Pointer {
		ptr := reflect.New(st)
		ptr.Elem().Set(val)
		recvVal = ptr
	}
	recvType := recvVal.Type()

	type injectedParam struct {
		argIndex int
		typ      reflect.Type
	}

	for i := 0; i < recvType.NumMethod(); i++ {
		m := recvType.Method(i)
		if !m.IsExported() {
			continue
		}
		mType := m.Type
		if mType.NumIn() < 2 {
			continue
		}
		ctxArgType := mType.In(1)
		takesRpcCtx := ctxArgType == rpcCtxType
		takesStdCtx := ctxArgType == stdCtxType
		if !takesRpcCtx && !takesStdCtx {
			continue
		}

		rpcName := camelToSnake(m.Name)
		srcInfo := inspectMethodSource(m.Func.Pointer(), st.Name(), m.Name)

		// Identify DI parameters vs non-DI parameters.
		var nonDIIndices []int
		var diParams []injectedParam
		for argIdx := 2; argIdx < mType.NumIn(); argIdx++ {
			pType := mType.In(argIdx)
			if depReg.has(pType) {
				diParams = append(diParams, injectedParam{argIndex: argIdx, typ: pType})
			} else {
				nonDIIndices = append(nonDIIndices, argIdx)
			}
		}

		var (
			streamArgIdx   = -1
			duplexHelper   duplexChannelParam
			clientSeqType  reflect.Type
			clientItemType reflect.Type
			businessParams []reflectedBusinessParam
		)

		for pos, argIdx := range nonDIIndices {
			pType := mType.In(argIdx)
			isLast := pos == len(nonDIIndices)-1
			if pType.Implements(duplexChannelParamType) {
				if !isLast {
					return nil, fmt.Errorf("meshcall: %s.%s: duplex channel parameter must come after all ordinary business parameters", st.Name(), m.Name)
				}
				streamArgIdx = argIdx
				duplexHelper = reflect.Zero(pType).Interface().(duplexChannelParam)
				continue
			}
			if elemType, isSeq := seq2ElemType(pType); isSeq {
				if !isLast {
					return nil, fmt.Errorf("meshcall: %s.%s: client stream parameter must come after all ordinary business parameters", st.Name(), m.Name)
				}
				streamArgIdx = argIdx
				clientSeqType = pType
				clientItemType = elemType
				continue
			}
			pName := ""
			// srcInfo.paramNames[0] is ctx; argIdx-1 corresponds to argIdx.
			if paramPos := argIdx - 1; paramPos >= 0 && paramPos < len(srcInfo.paramNames) {
				pName = srcInfo.paramNames[paramPos]
			}
			if pName == "" || pName == "_" {
				pName = fmt.Sprintf("arg%d", len(businessParams))
			}
			businessParams = append(businessParams, reflectedBusinessParam{
				argIndex: argIdx,
				name:     pName,
				typ:      pType,
			})
		}

		// Classify method shape and return signature.
		var (
			shape          streamKind
			respType       reflect.Type
			inType         reflect.Type
			outType        reflect.Type
			serverSeqOut   bool
			returnsErrOnly bool
			returnsErr2nd  bool
		)

		switch {
		case duplexHelper != nil:
			shape = duplexShape
			inType, outType = duplexHelper.duplexInOutTypes()
			if mType.NumOut() == 1 && mType.Out(0) == errType {
				returnsErrOnly = true
			} else if mType.NumOut() != 0 {
				return nil, fmt.Errorf("meshcall: duplex method %s.%s must return error, got %d return values", st.Name(), m.Name, mType.NumOut())
			}
		case clientSeqType != nil:
			shape = clientStreamShape
			inType = clientItemType
			if mType.NumOut() == 2 && mType.Out(1) == errType {
				respType = mType.Out(0)
				returnsErr2nd = true
			} else if mType.NumOut() == 1 && mType.Out(0) != errType {
				respType = mType.Out(0)
			} else {
				return nil, fmt.Errorf("meshcall: client-stream method %s.%s must return (Resp, error) or Resp", st.Name(), m.Name)
			}
		default:
			if mType.NumOut() >= 1 {
				if elemType, isSeq := seq2ElemType(mType.Out(0)); isSeq {
					if mType.NumOut() == 1 {
						shape = serverStreamShape
						outType = elemType
						serverSeqOut = true
					} else if mType.NumOut() == 2 && mType.Out(1) == errType {
						shape = serverStreamShape
						outType = elemType
						serverSeqOut = true
						returnsErr2nd = true
					} else {
						return nil, fmt.Errorf("meshcall: server-stream method %s.%s must return iter.Seq2[T, error] or (iter.Seq2[T, error], error)", st.Name(), m.Name)
					}
				}
			}
			if !serverSeqOut {
				shape = unaryShape
				if mType.NumOut() == 2 && mType.Out(1) == errType {
					respType = mType.Out(0)
					returnsErr2nd = true
				} else if mType.NumOut() == 1 && mType.Out(0) != errType {
					respType = mType.Out(0)
				} else {
					return nil, fmt.Errorf("meshcall: unary method %s.%s must return (Resp, error) or Resp", st.Name(), m.Name)
				}
			}
		}

		// Build synthetic request schema from businessParams.
		reqTitle := st.Name() + m.Name + "Request"
		b := &schemaBuilder{defs: newObject(), names: map[reflect.Type]string{}, taken: map[string]reflect.Type{}}
		props := newObject()
		var required []any
		paramNames := make([]string, 0, len(businessParams))
		for _, bp := range businessParams {
			paramNames = append(paramNames, bp.name)
			props.set(bp.name, b.typeSchema(bp.typ))
			if bp.typ.Kind() != reflect.Pointer {
				required = append(required, bp.name)
			}
		}
		reqRoot := newObject().set("properties", props)
		if len(required) > 0 {
			reqRoot.set("required", required)
		}
		reqRoot.set("title", reqTitle).set("type", "object")
		if len(b.defs.keys) > 0 {
			withDefs := newObject().set("$defs", b.defs)
			for _, k := range reqRoot.keys {
				withDefs.set(k, reqRoot.values[k])
			}
			reqRoot = withDefs
		}

		mEntry := &method{
			name:            rpcName,
			stream:          shape,
			response:        respType,
			input:           inType,
			output:          outType,
			parameters:      paramNames,
			requestQualname: reqTitle,
			reflected:       true,
		}
		mEntry.schemas.request = reqRoot
		if respType != nil {
			mEntry.schemas.response = schemaOf(respType)
		}
		if inType != nil {
			mEntry.schemas.input = schemaOf(inType)
		}
		if outType != nil {
			mEntry.schemas.output = schemaOf(outType)
		}

		var mOpts []MethodOption
		if srcInfo.doc != "" {
			cleanDoc, docOpts := parseDocAnnotations(srcInfo.doc)
			mOpts = append(mOpts, Doc(cleanDoc))
			mOpts = append(mOpts, docOpts...)
		}
		if extra, ok := svc.methodOpts[rpcName]; ok {
			mOpts = append(mOpts, extra...)
		}
		if shape == duplexShape && mEntry.duplexReason == "" {
			reason := srcInfo.doc
			if reason == "" {
				reason = "interactive duplex channel"
			}
			mEntry.duplexReason = reason
		}

		// Capture variables for closure.
		fnVal := m.Func
		totalIn := mType.NumIn()
		diTypes := make([]reflect.Type, len(diParams))
		for idx, dp := range diParams {
			diTypes[idx] = dp.typ
		}
		reqSchema := mEntry.schemas.request
		inSchema := mEntry.schemas.input

		mEntry.invoke = func(ctx *Context, call *serverCall, raw json.RawMessage) (any, error) {
			paramVals, decodedMap, err := decodeReflectedParams(reqSchema, businessParams, raw)
			if err != nil {
				return nil, err
			}
			scope := newDepScope(ctx, depReg)
			defer scope.close()

			diVals, err := scope.resolveAll(diTypes)
			if err != nil {
				return nil, err
			}

			return call.intercept(ctx, decodedMap, func() (any, error) {
				callArgs := make([]reflect.Value, totalIn)
				callArgs[0] = recvVal
				if takesRpcCtx {
					callArgs[1] = reflect.ValueOf(ctx)
				} else {
					callArgs[1] = reflect.ValueOf(context.Context(ctx))
				}
				for idx, bp := range businessParams {
					callArgs[bp.argIndex] = paramVals[idx]
				}
				for idx, dp := range diParams {
					callArgs[dp.argIndex] = diVals[idx]
				}
				if streamArgIdx >= 0 {
					if duplexHelper != nil {
						callArgs[streamArgIdx] = duplexHelper.newDuplexChannelVal(call, inSchema)
					} else if clientSeqType != nil {
						callArgs[streamArgIdx] = makeClientStreamSeq(ctx, call, clientSeqType, clientItemType, inSchema)
					}
				}

				results := fnVal.Call(callArgs)
				switch shape {
				case duplexShape:
					if returnsErrOnly && !results[0].IsNil() {
						return nil, results[0].Interface().(error)
					}
					return nil, nil
				case serverStreamShape:
					if returnsErr2nd && !results[1].IsNil() {
						return nil, results[1].Interface().(error)
					}
					seqVal := results[0]
					if seqVal.IsNil() {
						return nil, nil
					}
					return nil, drainServerStreamSeq(ctx, call, seqVal)
				default:
					if returnsErr2nd && !results[1].IsNil() {
						return nil, results[1].Interface().(error)
					}
					return results[0].Interface(), nil
				}
			})
		}

		svc.add(mEntry, mOpts)
	}

	if len(svc.methods) == 0 {
		return nil, fmt.Errorf("meshcall: service %q (%s) declares no exported RPC methods taking context.Context or *meshcall.Context", svc.name, st.Name())
	}
	return svc, nil
}

type reflectedBusinessParam struct {
	argIndex int
	name     string
	typ      reflect.Type
}

func parseDocAnnotations(doc string) (string, []MethodOption) {
	var opts []MethodOption
	var kept []string
	for _, line := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(line)
		switch strings.ToLower(trimmed) {
		case "@idempotent":
			opts = append(opts, Idempotent())
		case "@readonly", "@read_only", "@read-only":
			opts = append(opts, ReadOnly())
		case "@destructive":
			opts = append(opts, Destructive())
		default:
			kept = append(kept, line)
		}
	}
	return strings.TrimSpace(strings.Join(kept, "\n")), opts
}

func decodeReflectedParams(
	schema *orderedObject,
	params []reflectedBusinessParam,
	raw json.RawMessage,
) ([]reflect.Value, map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage("{}")
	}
	if vs := validate(schema, raw); len(vs) > 0 {
		e := detailed(CodeInvalidArgument, "Invalid request", "server", "request", false)
		e.Details["violations"] = violationList(vs)
		return nil, nil, e
	}
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawFields); err != nil {
		e := detailed(CodeInvalidArgument, "Invalid request", "server", "request", false)
		e.Details["violations"] = []any{map[string]any{"path": "", "code": "type", "message": "Input does not decode"}}
		return nil, nil, e
	}
	vals := make([]reflect.Value, len(params))
	decodedMap := make(map[string]any, len(params))
	for i, bp := range params {
		fieldRaw, ok := rawFields[bp.name]
		if !ok || len(fieldRaw) == 0 {
			vals[i] = reflect.Zero(bp.typ)
			decodedMap[bp.name] = vals[i].Interface()
			continue
		}
		ptr := reflect.New(bp.typ)
		if err := json.Unmarshal(fieldRaw, ptr.Interface()); err != nil {
			e := detailed(CodeInvalidArgument, "Invalid request", "server", "request", false)
			e.Details["violations"] = []any{map[string]any{"path": bp.name, "code": "type", "message": "Input does not decode"}}
			return nil, nil, e
		}
		vals[i] = ptr.Elem()
		decodedMap[bp.name] = vals[i].Interface()
	}
	return vals, decodedMap, nil
}

func decodeIntoReflect(t reflect.Type, schema *orderedObject, raw json.RawMessage, reason, message string) (reflect.Value, error) {
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	if vs := validate(schema, raw); len(vs) > 0 {
		e := detailed(CodeInvalidArgument, message, "server", reason, false)
		e.Details["violations"] = violationList(vs)
		return reflect.Zero(t), e
	}
	ptr := reflect.New(t)
	if err := json.Unmarshal(raw, ptr.Interface()); err != nil {
		e := detailed(CodeInvalidArgument, message, "server", reason, false)
		e.Details["violations"] = []any{map[string]any{"path": "", "code": "type", "message": "Input does not decode"}}
		return reflect.Zero(t), e
	}
	return ptr.Elem(), nil
}

func makeClientStreamSeq(ctx *Context, call *serverCall, seqType, itemType reflect.Type, itemSchema *orderedObject) reflect.Value {
	return reflect.MakeFunc(seqType, func(args []reflect.Value) []reflect.Value {
		yieldFn := args[0]
		zeroItem := reflect.Zero(itemType)
		zeroErr := reflect.Zero(errType)
		for {
			if ctx.Err() != nil {
				yieldFn.Call([]reflect.Value{zeroItem, reflect.ValueOf(contextError(ctx))})
				return nil
			}
			raw, err := call.nextInput()
			if err != nil {
				if errors.Is(err, errEndOfInput) {
					return nil
				}
				yieldFn.Call([]reflect.Value{zeroItem, reflect.ValueOf(err)})
				return nil
			}
			itemVal, decErr := decodeIntoReflect(itemType, itemSchema, raw, "input_item", "Invalid input stream item")
			errVal := zeroErr
			if decErr != nil {
				errVal = reflect.ValueOf(decErr)
			}
			cont := yieldFn.Call([]reflect.Value{itemVal, errVal})[0].Bool()
			if !cont || decErr != nil {
				return nil
			}
		}
	})
}

func drainServerStreamSeq(ctx *Context, call *serverCall, seqVal reflect.Value) error {
	yieldType := seqVal.Type().In(0)
	var streamErr error
	yieldVal := reflect.MakeFunc(yieldType, func(args []reflect.Value) []reflect.Value {
		if ctx.Err() != nil {
			streamErr = contextError(ctx)
			return []reflect.Value{reflect.ValueOf(false)}
		}
		if !args[1].IsNil() {
			streamErr = args[1].Interface().(error)
			return []reflect.Value{reflect.ValueOf(false)}
		}
		if err := call.sendOutput(args[0].Interface()); err != nil {
			streamErr = err
			return []reflect.Value{reflect.ValueOf(false)}
		}
		return []reflect.Value{reflect.ValueOf(true)}
	})
	seqVal.Call([]reflect.Value{yieldVal})
	return streamErr
}
