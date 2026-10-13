package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/codegen"
	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// GenerateOptions drives `meshcall generate`.
type GenerateOptions struct {
	// Source is a contract document path (*.json) or a Python import path
	// in module:Class form, which is exported through the Python SDK first.
	Source   string
	Service  string // selects one service of a multi-service document
	Language string // python | typescript
	Output   string // directory, or a file path with SingleFile

	SingleFile          bool
	ClassName           string
	PackageName         string
	PackageVersion      string
	RuntimeImport       string
	RuntimeVersion      string
	MeshcallRequirement string
}

// Generate writes a client for one service. It returns the paths written.
func Generate(ctx context.Context, opts GenerateOptions) ([]string, error) {
	if opts.Output == "" {
		return nil, errors.New("--output is required")
	}
	switch opts.Language {
	case "", "python":
		opts.Language = "python"
	case "typescript":
	default:
		return nil, fmt.Errorf("unknown language %q (python | typescript)", opts.Language)
	}
	service, err := loadService(ctx, opts.Source, opts.Service)
	if err != nil {
		return nil, err
	}
	var written []string
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
	if opts.SingleFile {
		var content string
		if opts.Language == "python" {
			content = codegen.RenderPythonClient(service, codegen.PythonOptions{
				ClassName: opts.ClassName, MeshcallRequirement: opts.MeshcallRequirement,
			})
		} else if content, err = codegen.RenderTypeScriptClient(service, codegen.TypeScriptOptions{
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
		files = codegen.PythonPackage(service, pkg)
	} else if files, err = codegen.TypeScriptPackage(service, pkg); err != nil {
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

// Export writes the contract document for a Python service (module:Class).
func Export(ctx context.Context, source, output string) error {
	if strings.HasSuffix(source, ".json") {
		return errors.New("export takes a Python import path (module:Class); the contract document is the output")
	}
	return exportPython(ctx, source, output)
}

func loadService(ctx context.Context, source, name string) (*contract.Service, error) {
	path := source
	if !strings.HasSuffix(source, ".json") {
		if !strings.Contains(source, ":") {
			return nil, fmt.Errorf("source %q is neither a contract document (*.json) nor a Python import path (module:Class)", source)
		}
		tmp, err := os.MkdirTemp("", "meshcall-export-")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(tmp)
		path = filepath.Join(tmp, "contract.json")
		if err := exportPython(ctx, source, path); err != nil {
			return nil, err
		}
	}
	doc, err := contract.Load(path)
	if err != nil {
		return nil, err
	}
	return doc.Select(name)
}

// exportPython runs the Python SDK's exporter in the caller's interpreter.
// `uv run meshcall` (the Python launcher) sets MESHCALL_PYTHON so the service
// module resolves inside the project's environment.
func exportPython(ctx context.Context, source, output string) error {
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
	cmd := exec.CommandContext(ctx, python, "-m", "meshcall.export", source, "--output", output)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("exporting %s with %s failed: %w", source, python, err)
	}
	return nil
}
