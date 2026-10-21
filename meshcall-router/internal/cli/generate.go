package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/codegen"
	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
	"github.com/t4wefan/meshcall/meshcall-router/internal/router"
)

// GenerateOptions drives `meshcall generate`.
type GenerateOptions struct {
	// Source is a single source specification (used when Sources is empty).
	Source string
	// Sources holds one or more contract documents (*.json) or language source
	// specs (module[:Class], file.py[:Class], file.ts[:Class], file.go[:Type], ./pkg[:Type]).
	Sources  []string
	Service  string // selects one service of a multi-service document
	Language string // python | typescript | go
	Output   string // file path (default single-file mode) or directory (with Package)

	// Package writes a full installable package directory instead of a single file.
	Package bool
	// SingleFile is kept for backward compatibility; single-file output is the
	// default whenever Package is false.
	SingleFile          bool
	ClassName           string
	PackageName         string
	PackageVersion      string
	RuntimeImport       string
	RuntimeVersion      string
	MeshcallRequirement string
}

// Generate writes a client for one or more services. It returns the paths written.
func Generate(ctx context.Context, opts GenerateOptions) (written []string, err error) {
	if opts.Output == "" {
		return nil, errors.New("--output is required")
	}
	sources := opts.Sources
	if len(sources) == 0 && opts.Source != "" {
		sources = []string{opts.Source}
	}
	if len(sources) == 0 {
		return nil, errors.New("at least one source is required")
	}
	switch opts.Language {
	case "", "python":
		opts.Language = "python"
	case "typescript", "go":
	default:
		return nil, fmt.Errorf("unknown language %q (python | typescript | go)", opts.Language)
	}
	services, err := loadServices(ctx, sources, opts.Service)
	if err != nil {
		return nil, err
	}
	if len(services) > 1 && opts.ClassName != "" {
		return nil, errors.New("--class-name cannot be used when generating multiple services; pass --service to select one")
	}
	defer func() {
		if r := recover(); r != nil {
			if ge, ok := r.(interface{ Error() string }); ok {
				err = errors.New(ge.Error())
				written = nil
				return
			}
			panic(r)
		}
	}()
	if opts.Language == "go" {
		// One file: <output> if it names a .go file, else <output>/client.go.
		path := opts.Output
		if !strings.HasSuffix(path, ".go") {
			path = filepath.Join(path, "client.go")
		}
		content := codegen.RenderGoClients(services, codegen.GoOptions{PackageName: opts.PackageName, RuntimeImport: opts.RuntimeImport})
		if err := codegen.WriteFile(path, content); err != nil {
			return nil, err
		}
		return []string{path}, nil
	}
	singleFile := !opts.Package || opts.SingleFile
	if singleFile {
		var content string
		if opts.Language == "python" {
			content = codegen.RenderPythonClients(services, codegen.PythonOptions{
				ClassName: opts.ClassName, MeshcallRequirement: opts.MeshcallRequirement,
			})
		} else if content, err = codegen.RenderTypeScriptClients(services, codegen.TypeScriptOptions{
			ClassName: opts.ClassName, RuntimeImport: opts.RuntimeImport, RuntimeVersion: opts.RuntimeVersion,
		}); err != nil {
			return nil, err
		}
		if err := codegen.WriteFile(opts.Output, content); err != nil {
			return nil, err
		}
		return []string{opts.Output}, nil
	}
	pkg := codegen.PackageOptions{
		PackageName: opts.PackageName, PackageVersion: opts.PackageVersion, ClassName: opts.ClassName,
		MeshcallRequirement: opts.MeshcallRequirement, RuntimeImport: opts.RuntimeImport, RuntimeVersion: opts.RuntimeVersion,
	}
	var files map[string]string
	if opts.Language == "python" {
		files = codegen.PythonPackageForServices(services, pkg)
	} else if files, err = codegen.TypeScriptPackageForServices(services, pkg); err != nil {
		return nil, err
	}
	if err := codegen.WriteFiles(opts.Output, files); err != nil {
		return nil, err
	}
	for rel := range files {
		written = append(written, filepath.Join(opts.Output, filepath.FromSlash(rel)))
	}
	return written, err
}

type contractEnvelope struct {
	Protocol string            `json:"protocol"`
	Services []json.RawMessage `json:"services"`
}

// Export spins up an ephemeral inspect-only Router, registers all specified
// language sources (Python, TypeScript, Go) against it, and writes the
// resulting portable contract document (*.meshcall.json) to output.
func Export(ctx context.Context, sources []string, output string) error {
	if len(sources) == 0 {
		return errors.New("export requires at least one source")
	}
	for _, src := range sources {
		modPart, _ := splitSpec(src)
		if strings.HasSuffix(strings.ToLower(modPart), ".json") {
			return errors.New("export takes source files or modules (Python, TypeScript, Go); the contract document (*.json) is the output")
		}
	}
	contracts, err := collectContracts(ctx, sources)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(contractEnvelope{
		Protocol: router.Protocol,
		Services: contracts,
	}, "", "  ")
	if err != nil {
		return err
	}
	return codegen.WriteFile(output, string(data)+"\n")
}

func loadServices(ctx context.Context, sources []string, serviceName string) ([]*contract.Service, error) {
	// Fast path when a single pre-exported *.json contract file is given.
	if len(sources) == 1 {
		modPart, _ := splitSpec(sources[0])
		if strings.HasSuffix(strings.ToLower(modPart), ".json") {
			doc, err := contract.Load(modPart)
			if err != nil {
				return nil, err
			}
			if serviceName != "" {
				svc, err := doc.Select(serviceName)
				if err != nil {
					return nil, err
				}
				return []*contract.Service{svc}, nil
			}
			if len(doc.Services) == 0 {
				return nil, errors.New("contract document contains no services")
			}
			return doc.Services, nil
		}
	}

	rawContracts, err := collectContracts(ctx, sources)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(contractEnvelope{
		Protocol: router.Protocol,
		Services: rawContracts,
	})
	if err != nil {
		return nil, err
	}
	doc, err := contract.Parse(data)
	if err != nil {
		return nil, err
	}
	if serviceName != "" {
		svc, err := doc.Select(serviceName)
		if err != nil {
			return nil, err
		}
		return []*contract.Service{svc}, nil
	}
	if len(doc.Services) == 0 {
		return nil, errors.New("contract document contains no services")
	}
	return doc.Services, nil
}

type sourceKind int

const (
	sourceKindJSON sourceKind = iota
	sourceKindPython
	sourceKindTS
	sourceKindGo
)

func splitSpec(spec string) (string, string) {
	idx := strings.LastIndexByte(spec, ':')
	if idx < 0 {
		return spec, ""
	}
	if idx == 1 && ((spec[0] >= 'A' && spec[0] <= 'Z') || (spec[0] >= 'a' && spec[0] <= 'z')) &&
		len(spec) > 2 && (spec[2] == '\\' || spec[2] == '/') {
		return spec, ""
	}
	return spec[:idx], spec[idx+1:]
}

func classifySource(spec string) sourceKind {
	modPart, _ := splitSpec(spec)
	lower := strings.ToLower(modPart)
	switch {
	case strings.HasSuffix(lower, ".json"):
		return sourceKindJSON
	case strings.HasSuffix(lower, ".ts"), strings.HasSuffix(lower, ".mts"),
		strings.HasSuffix(lower, ".js"), strings.HasSuffix(lower, ".mjs"):
		return sourceKindTS
	case strings.HasSuffix(lower, ".py"):
		return sourceKindPython
	case strings.HasSuffix(lower, ".go"):
		return sourceKindGo
	}
	if info, err := os.Stat(modPart); err == nil && info.IsDir() {
		if dirHasGoFiles(modPart) {
			return sourceKindGo
		}
	}
	return sourceKindPython
}

func dirHasGoFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
			return true
		}
	}
	return false
}

func collectContracts(ctx context.Context, sources []string) ([]json.RawMessage, error) {
	r, err := router.Start(router.Config{
		Host:        "127.0.0.1",
		Port:        0,
		InspectOnly: true,
	})
	if err != nil {
		return nil, fmt.Errorf("starting inspect-only router: %w", err)
	}
	defer r.Close()
	routerURL := fmt.Sprintf("ws://%s:%d", r.Ready().Host, r.Ready().Port)

	var out []json.RawMessage
	seenNames := map[string]bool{}
	appendContract := func(raw json.RawMessage) {
		var header struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &header); err == nil && header.Name != "" {
			if seenNames[header.Name] {
				return
			}
			seenNames[header.Name] = true
		}
		out = append(out, raw)
	}

	routerCursor := 0
	syncFromRouter := func() {
		ordered := r.OrderedContracts()
		for ; routerCursor < len(ordered); routerCursor++ {
			appendContract(ordered[routerCursor])
		}
	}

	for _, src := range sources {
		kind := classifySource(src)
		switch kind {
		case sourceKindJSON:
			modPart, sym := splitSpec(src)
			rawBytes, err := os.ReadFile(modPart)
			if err != nil {
				return nil, err
			}
			var env contractEnvelope
			if err := json.Unmarshal(rawBytes, &env); err != nil {
				return nil, fmt.Errorf("reading %s: %w", modPart, err)
			}
			for _, rawSvc := range env.Services {
				if sym != "" {
					var h struct {
						Name           string `json:"name"`
						SourceQualname string `json:"source_qualname"`
					}
					_ = json.Unmarshal(rawSvc, &h)
					if h.Name != sym && strings.TrimSuffix(h.Name, ".v0") != sym && h.SourceQualname != sym {
						continue
					}
				}
				appendContract(rawSvc)
			}
		case sourceKindPython:
			if err := exportPython(ctx, []string{src}, routerURL); err != nil {
				return nil, err
			}
			syncFromRouter()
		case sourceKindTS:
			if err := exportTypeScript(ctx, []string{src}, routerURL); err != nil {
				return nil, err
			}
			syncFromRouter()
		case sourceKindGo:
			if err := exportGo(ctx, src, routerURL); err != nil {
				return nil, err
			}
			syncFromRouter()
		}
	}

	if len(out) == 0 {
		return nil, errors.New("no services discovered from the provided sources")
	}
	return out, nil
}

// exportPython runs the Python SDK's exporter in the caller's interpreter and
// registers the discovered services with the ephemeral inspect-only Router.
func exportPython(ctx context.Context, specs []string, routerURL string) error {
	python := os.Getenv("MESHCALL_PYTHON")
	if python == "" {
		for _, candidate := range []string{"python3", "python"} {
			if p, err := exec.LookPath(candidate); err == nil {
				python = p
				break
			}
		}
	}
	if python == "" {
		return errors.New("no Python interpreter found; run through `uv run meshcall …` or set MESHCALL_PYTHON")
	}
	args := append([]string{"-m", "meshcall.export"}, specs...)
	args = append(args, "--router", routerURL)
	cmd := exec.CommandContext(ctx, python, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("exporting %s with %s failed: %w", strings.Join(specs, ", "), python, err)
	}
	return nil
}

// exportTypeScript runs the TypeScript SDK's exporter in Node and registers
// the discovered services with the ephemeral inspect-only Router.
func exportTypeScript(ctx context.Context, specs []string, routerURL string) error {
	node := os.Getenv("MESHCALL_NODE")
	if node == "" {
		if p, err := exec.LookPath("node"); err == nil {
			node = p
		}
	}
	if node == "" {
		return errors.New("no Node.js runtime found; install node or set MESHCALL_NODE")
	}
	helper, err := findTSExportHelper(specs)
	if err != nil {
		return err
	}
	args := append([]string{helper}, specs...)
	args = append(args, "--router", routerURL)
	cmd := exec.CommandContext(ctx, node, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("exporting %s with node failed: %w", strings.Join(specs, ", "), err)
	}
	return nil
}

func findTSExportHelper(specs []string) (string, error) {
	if env := os.Getenv("MESHCALL_TS_EXPORT"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env, nil
		}
	}
	var searchRoots []string
	for _, spec := range specs {
		modPart, _ := splitSpec(spec)
		if abs, err := filepath.Abs(modPart); err == nil {
			searchRoots = append(searchRoots, filepath.Dir(abs))
		}
	}
	if cwd, err := os.Getwd(); err == nil {
		searchRoots = append(searchRoots, cwd)
	}
	if exe, err := os.Executable(); err == nil {
		searchRoots = append(searchRoots, filepath.Dir(exe))
	}
	for _, start := range searchRoots {
		dir := start
		for {
			candidates := []string{
				filepath.Join(dir, "node_modules", "meshcall", "dist", "src", "export.js"),
				filepath.Join(dir, "meshcall-ts", "dist", "src", "export.js"),
				filepath.Join(dir, "dist", "src", "export.js"),
			}
			for _, c := range candidates {
				if _, err := os.Stat(c); err == nil {
					return c, nil
				}
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	return "", errors.New("could not locate meshcall TypeScript exporter (dist/src/export.js); install meshcall in node_modules or set MESHCALL_TS_EXPORT")
}

type goServiceFunc struct {
	Name       string
	ReturnsErr bool
}

func exportGo(ctx context.Context, spec string, routerURL string) error {
	modPart, symbolName := splitSpec(spec)
	info, err := os.Stat(modPart)
	if err != nil {
		return fmt.Errorf("Go source path does not exist: %s", modPart)
	}
	var targetDir, onlyFile string
	if info.IsDir() {
		targetDir, err = filepath.Abs(modPart)
		if err != nil {
			return err
		}
	} else {
		absFile, err := filepath.Abs(modPart)
		if err != nil {
			return err
		}
		targetDir = filepath.Dir(absFile)
		onlyFile = filepath.Base(absFile)
	}

	pkgName, structs, funcs, err := discoverGoServices(targetDir, onlyFile, symbolName)
	if err != nil {
		return err
	}
	filterServiceName := ""
	if symbolName != "" {
		matchedSymbol := false
		for _, s := range structs {
			if s == symbolName {
				structs = []string{s}
				funcs = nil
				matchedSymbol = true
				break
			}
		}
		if !matchedSymbol {
			for _, fn := range funcs {
				if fn.Name == symbolName {
					funcs = []goServiceFunc{fn}
					structs = nil
					matchedSymbol = true
					break
				}
			}
		}
		if !matchedSymbol {
			filterServiceName = symbolName
		}
	}
	if len(structs) == 0 && len(funcs) == 0 {
		return fmt.Errorf("no MeshCall services found in %s", spec)
	}

	goBin, err := exec.LookPath("go")
	if err != nil {
		return errors.New("no `go` toolchain found in PATH")
	}

	goModDir := findAncestorWithFile(targetDir, "go.mod")
	if goModDir == "" {
		return runStandaloneGoExport(ctx, goBin, targetDir, pkgName, structs, funcs, filterServiceName, symbolName != "", routerURL)
	}

	allExported := pkgName != "main"
	if allExported {
		for _, s := range structs {
			if !ast.IsExported(s) {
				allExported = false
				break
			}
		}
		for _, fn := range funcs {
			if !ast.IsExported(fn.Name) {
				allExported = false
				break
			}
		}
	}

	if allExported {
		modName, err := readGoModuleName(filepath.Join(goModDir, "go.mod"))
		if err == nil && modName != "" {
			rel, relErr := filepath.Rel(goModDir, targetDir)
			if relErr == nil {
				importPath := modName
				if rel != "." {
					importPath = modName + "/" + filepath.ToSlash(rel)
				}
				return runSubdirGoExport(ctx, goBin, goModDir, importPath, structs, funcs, filterServiceName, symbolName != "", routerURL)
			}
		}
	}

	return runInPackageGoTestExport(ctx, goBin, targetDir, pkgName, structs, funcs, filterServiceName, symbolName != "", routerURL)
}

func discoverGoServices(targetDir, onlyFile, symbolName string) (string, []string, []goServiceFunc, error) {
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return "", nil, nil, err
	}
	fset := token.NewFileSet()
	var pkgName string
	var structs []string
	var funcs []goServiceFunc

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || strings.HasPrefix(name, "meshcall_export_") {
			continue
		}
		if onlyFile != "" && symbolName == "" && name != onlyFile {
			continue
		}
		fullPath := filepath.Join(targetDir, name)
		fileAst, err := parser.ParseFile(fset, fullPath, nil, 0)
		if err != nil {
			return "", nil, nil, fmt.Errorf("parsing %s: %w", fullPath, err)
		}
		if pkgName == "" {
			pkgName = fileAst.Name.Name
		}
		for _, decl := range fileAst.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				if d.Tok != token.TYPE {
					continue
				}
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok || st.Fields == nil {
						continue
					}
					if structEmbedsServiceMeta(st) {
						structs = append(structs, ts.Name.Name)
					}
				}
			case *ast.FuncDecl:
				if d.Recv != nil || d.Name == nil || d.Type.Params.NumFields() != 0 || d.Type.Results == nil {
					continue
				}
				res := d.Type.Results.List
				if len(res) == 1 && len(res[0].Names) <= 1 && isMeshcallServicePtr(res[0].Type) {
					funcs = append(funcs, goServiceFunc{Name: d.Name.Name, ReturnsErr: false})
				} else if len(res) == 2 && isMeshcallServicePtr(res[0].Type) && isErrorIdent(res[1].Type) {
					funcs = append(funcs, goServiceFunc{Name: d.Name.Name, ReturnsErr: true})
				}
			}
		}
	}
	return pkgName, structs, funcs, nil
}

func structEmbedsServiceMeta(st *ast.StructType) bool {
	for _, field := range st.Fields.List {
		if len(field.Names) != 0 {
			continue
		}
		switch t := field.Type.(type) {
		case *ast.Ident:
			if t.Name == "ServiceMeta" {
				return true
			}
		case *ast.SelectorExpr:
			if t.Sel.Name == "ServiceMeta" {
				return true
			}
		}
	}
	return false
}

func isMeshcallServicePtr(expr ast.Expr) bool {
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch x := star.X.(type) {
	case *ast.Ident:
		return x.Name == "Service"
	case *ast.SelectorExpr:
		return x.Sel.Name == "Service"
	}
	return false
}

func isErrorIdent(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == "error"
}

func findAncestorWithFile(startDir, fileName string) string {
	dir := startDir
	for {
		if _, err := os.Stat(filepath.Join(dir, fileName)); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func readGoModuleName(goModPath string) (string, error) {
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", errors.New("no module directive in go.mod")
}

func renderGoExportBody(pkgPrefix string, structs []string, funcs []goServiceFunc, filterServiceName string, explicitSymbol bool, routerURL string) string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "\tvar services []*meshcall.Service\n")
	fmt.Fprintf(&b, "\tseen := map[string]bool{}\n")
	fmt.Fprintf(&b, "\tfilterName := %q\n", filterServiceName)
	fmt.Fprintf(&b, "\taddService := func(svc *meshcall.Service) {\n")
	fmt.Fprintf(&b, "\t\tif svc == nil || seen[svc.Name()] {\n\t\t\treturn\n\t\t}\n")
	fmt.Fprintf(&b, "\t\tif filterName != \"\" && svc.Name() != filterName && strings.TrimSuffix(svc.Name(), \".v0\") != filterName {\n\t\t\treturn\n\t\t}\n")
	fmt.Fprintf(&b, "\t\tseen[svc.Name()] = true\n")
	fmt.Fprintf(&b, "\t\tservices = append(services, svc)\n")
	fmt.Fprintf(&b, "\t}\n")

	for _, fn := range funcs {
		if fn.ReturnsErr {
			fmt.Fprintf(&b, "\t{\n\t\tsvc, err := %s%s()\n\t\tif err != nil {\n\t\t\tfatalf(\"%s: %%v\", err)\n\t\t}\n\t\taddService(svc)\n\t}\n", pkgPrefix, fn.Name, fn.Name)
		} else {
			fmt.Fprintf(&b, "\taddService(%s%s())\n", pkgPrefix, fn.Name)
		}
	}
	for _, st := range structs {
		if !explicitSymbol && len(funcs) > 0 {
			fmt.Fprintf(&b, "\tif svc, err := meshcall.ReflectService(&%s%s{}); err == nil {\n\t\taddService(svc)\n\t}\n", pkgPrefix, st)
		} else {
			fmt.Fprintf(&b, "\t{\n\t\tsvc, err := meshcall.ReflectService(&%s%s{})\n\t\tif err != nil {\n\t\t\tfatalf(\"%s: %%v\", err)\n\t\t}\n\t\taddService(svc)\n\t}\n", pkgPrefix, st, st)
		}
	}
	fmt.Fprintf(&b, "\tif len(services) == 0 {\n\t\tfatalf(\"no matching MeshCall services found\")\n\t}\n")
	fmt.Fprintf(&b, "\tsrv, err := meshcall.NewServer(meshcall.ServerOptions{Services: services})\n")
	fmt.Fprintf(&b, "\tif err != nil {\n\t\tfatalf(\"%%v\", err)\n\t}\n")
	fmt.Fprintf(&b, "\tctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)\n")
	fmt.Fprintf(&b, "\tdefer cancel()\n")
	fmt.Fprintf(&b, "\tif err := srv.ConnectRouter(ctx, %q); err != nil {\n\t\tfatalf(\"connecting to inspect router: %%v\", err)\n\t}\n", routerURL)
	fmt.Fprintf(&b, "\tsrv.Close()\n")
	return b.String()
}

func runSubdirGoExport(ctx context.Context, goBin, goModDir, importPath string, structs []string, funcs []goServiceFunc, filterServiceName string, explicitSymbol bool, routerURL string) error {
	tmpDir, err := os.MkdirTemp(goModDir, ".meshcall-export-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	var src bytes.Buffer
	fmt.Fprintf(&src, "package main\n\nimport (\n\t\"context\"\n\t\"fmt\"\n\t\"os\"\n\t\"strings\"\n\t\"time\"\n\n\tmeshcall \"github.com/t4wefan/meshcall/meshcall-go\"\n\ttargetPkg %q\n)\n\n", importPath)
	fmt.Fprintf(&src, "var _ = strings.TrimSuffix\n\n")
	fmt.Fprintf(&src, "func fatalf(format string, args ...any) {\n\tfmt.Fprintf(os.Stderr, \"error: \"+format+\"\\n\", args...)\n\tos.Exit(1)\n}\n\n")
	fmt.Fprintf(&src, "func main() {\n")
	src.WriteString(renderGoExportBody("targetPkg.", structs, funcs, filterServiceName, explicitSymbol, routerURL))
	fmt.Fprintf(&src, "}\n")

	mainFile := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(mainFile, src.Bytes(), 0o600); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, goBin, "run", ".")
	cmd.Dir = tmpDir
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		_, _ = os.Stderr.Write(out.Bytes())
		return fmt.Errorf("exporting Go package %s failed: %w", importPath, err)
	}
	return nil
}

func runInPackageGoTestExport(ctx context.Context, goBin, targetDir, pkgName string, structs []string, funcs []goServiceFunc, filterServiceName string, explicitSymbol bool, routerURL string) error {
	var randBytes [6]byte
	_, _ = rand.Read(randBytes[:])
	suffix := hex.EncodeToString(randBytes[:])
	testFnName := "TestMeshCallExport_" + suffix
	testFile := filepath.Join(targetDir, "meshcall_export_"+suffix+"_test.go")

	var src bytes.Buffer
	fmt.Fprintf(&src, "package %s\n\nimport (\n\t\"context\"\n\t\"fmt\"\n\t\"os\"\n\t\"strings\"\n\t\"testing\"\n\t\"time\"\n\n\tmeshcall \"github.com/t4wefan/meshcall/meshcall-go\"\n)\n\n", pkgName)
	fmt.Fprintf(&src, "var _ = strings.TrimSuffix\n\n")
	fmt.Fprintf(&src, "func %s(t *testing.T) {\n", testFnName)
	fmt.Fprintf(&src, "\tfatalf := func(format string, args ...any) {\n\t\tfmt.Fprintf(os.Stderr, \"error: \"+format+\"\\n\", args...)\n\t\tos.Exit(1)\n\t}\n")
	src.WriteString(renderGoExportBody("", structs, funcs, filterServiceName, explicitSymbol, routerURL))
	fmt.Fprintf(&src, "}\n")

	if err := os.WriteFile(testFile, src.Bytes(), 0o600); err != nil {
		return err
	}
	defer os.Remove(testFile)

	cmd := exec.CommandContext(ctx, goBin, "test", "-run", "^"+testFnName+"$", ".")
	cmd.Dir = targetDir
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		_, _ = os.Stderr.Write(out.Bytes())
		return fmt.Errorf("exporting Go sources in %s failed: %w", targetDir, err)
	}
	return nil
}

func runStandaloneGoExport(ctx context.Context, goBin, targetDir, pkgName string, structs []string, funcs []goServiceFunc, filterServiceName string, explicitSymbol bool, routerURL string) error {
	meshcallGoDir, err := findMeshcallGoRoot()
	if err != nil {
		return err
	}
	tmpModDir, err := os.MkdirTemp("", "meshcall-go-export-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpModDir)

	origGoMod, err := os.ReadFile(filepath.Join(meshcallGoDir, "go.mod"))
	if err != nil {
		return err
	}
	var modBuf bytes.Buffer
	for _, line := range strings.Split(string(origGoMod), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "module ") {
			modBuf.WriteString("module meshcall_export_standalone\n")
		} else {
			modBuf.WriteString(line + "\n")
		}
	}
	fmt.Fprintf(&modBuf, "\nrequire github.com/t4wefan/meshcall/meshcall-go v0.0.0\n")
	fmt.Fprintf(&modBuf, "replace github.com/t4wefan/meshcall/meshcall-go => %s\n", meshcallGoDir)
	if err := os.WriteFile(filepath.Join(tmpModDir, "go.mod"), modBuf.Bytes(), 0o600); err != nil {
		return err
	}
	if origGoSum, err := os.ReadFile(filepath.Join(meshcallGoDir, "go.sum")); err == nil {
		_ = os.WriteFile(filepath.Join(tmpModDir, "go.sum"), origGoSum, 0o600)
	}

	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(targetDir, name))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(tmpModDir, name), data, 0o600); err != nil {
			return err
		}
	}
	return runInPackageGoTestExport(ctx, goBin, tmpModDir, pkgName, structs, funcs, filterServiceName, explicitSymbol, routerURL)
}

func findMeshcallGoRoot() (string, error) {
	if env := os.Getenv("MESHCALL_GO_ROOT"); env != "" {
		if _, err := os.Stat(filepath.Join(env, "go.mod")); err == nil {
			return env, nil
		}
	}
	var roots []string
	if cwd, err := os.Getwd(); err == nil {
		roots = append(roots, cwd)
	}
	if exe, err := os.Executable(); err == nil {
		roots = append(roots, filepath.Dir(exe))
	}
	if _, thisFile, _, ok := runtime.Caller(0); ok {
		roots = append(roots, filepath.Dir(thisFile))
	}
	for _, start := range roots {
		dir := start
		for {
			candidate := filepath.Join(dir, "meshcall-go")
			if _, err := os.Stat(filepath.Join(candidate, "go.mod")); err == nil {
				return candidate, nil
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	return "", errors.New("could not find go.mod in source directory or locate meshcall-go module; run inside a Go module or set MESHCALL_GO_ROOT")
}
