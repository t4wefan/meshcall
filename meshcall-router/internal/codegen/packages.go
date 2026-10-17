package codegen

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// RuntimeRelease is the meshcall release generated clients require by
// default: they use that release's API. scripts/release/bump.sh sets it.
const RuntimeRelease = "0.3.0"

// PackageOptions configures a generated client package.
type PackageOptions struct {
	PackageName    string
	PackageVersion string // default "0.1.0"
	ClassName      string
	// Python
	MeshcallRequirement string // default "meshcall>=" + RuntimeRelease
	// TypeScript
	RuntimeImport  string // default "meshcall"
	RuntimeVersion string // default "^" + RuntimeRelease
}

// PythonPackage renders every file of an installable uv package, keyed by
// path relative to the package root.
func PythonPackage(service *contract.Service, opts PackageOptions) map[string]string {
	if opts.PackageVersion == "" {
		opts.PackageVersion = "0.1.0"
	}
	if opts.MeshcallRequirement == "" {
		opts.MeshcallRequirement = "meshcall>=" + RuntimeRelease
	}
	distribution := opts.PackageName
	if distribution == "" {
		distribution = defaultPackageName(service)
	}
	module := pythonModuleName(distribution)
	className := opts.ClassName
	if className == "" {
		className = PythonClientClassName(service)
	}
	typeNames := PythonTypeNames(service)
	src := filepath.ToSlash(filepath.Join("src", module))
	return map[string]string{
		"pyproject.toml":     pythonPyproject(distribution, module, opts.PackageVersion, opts.MeshcallRequirement),
		"README.md":          pythonReadme(distribution, module, className, typeNames),
		src + "/models.py":   RenderPythonModels(service),
		src + "/client.py":   RenderPythonClientModule(service, PythonOptions{ClassName: className}),
		src + "/__init__.py": pythonInit(className, typeNames),
		src + "/py.typed":    "",
	}
}

// TypeScriptPackage renders every file of a Yarn package.
func TypeScriptPackage(service *contract.Service, opts PackageOptions) (map[string]string, error) {
	if opts.PackageVersion == "" {
		opts.PackageVersion = "0.1.0"
	}
	if opts.RuntimeImport == "" {
		opts.RuntimeImport = "meshcall"
	}
	if opts.RuntimeVersion == "" {
		opts.RuntimeVersion = "^" + RuntimeRelease
	}
	name := opts.PackageName
	if name == "" {
		name = defaultPackageName(service)
	}
	className := opts.ClassName
	if className == "" {
		className = TypeScriptClientClassName(service)
	}
	models, err := RenderTypeScriptModels(service, TypeScriptOptions{RuntimeImport: opts.RuntimeImport})
	if err != nil {
		return nil, err
	}
	client, err := RenderTypeScriptClientModule(service, TypeScriptOptions{ClassName: className, RuntimeImport: opts.RuntimeImport})
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"package.json":  typescriptPackageJSON(name, opts.PackageVersion, opts.RuntimeImport, opts.RuntimeVersion),
		"tsconfig.json": typescriptTsconfig(),
		"README.md":     typescriptReadme(name, className, TypeScriptTypeNames(service)),
		".gitignore":    "node_modules/\ndist/\n",
		"src/models.ts": models,
		"src/client.ts": client,
		"src/index.ts":  "export * from \"./models.js\";\nexport { " + className + " } from \"./client.js\";\n",
	}, nil
}

// WriteFiles writes a rendered package under root, atomically per file.
func WriteFiles(root string, files map[string]string) error {
	if info, err := os.Stat(root); err == nil && !info.IsDir() {
		return fmt.Errorf("package output is not a directory: %s", root)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	for rel, content := range files {
		if err := WriteFile(filepath.Join(root, filepath.FromSlash(rel)), content); err != nil {
			return err
		}
	}
	return nil
}

// WriteFile writes content atomically (temp file + rename), creating parents.
func WriteFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

var nonPackageChars = regexp.MustCompile(`[^a-z0-9]+`)

func defaultPackageName(service *contract.Service) string {
	value := strings.Trim(nonPackageChars.ReplaceAllString(strings.ToLower(service.Name), "-"), "-")
	return "meshcall-" + value + "-client"
}

func pythonModuleName(packageName string) string {
	value := packageName[strings.LastIndexByte(packageName, '/')+1:]
	module := nonIdent.ReplaceAllString(value, "_")
	if module == "" || (module[0] >= '0' && module[0] <= '9') {
		module = "meshcall_" + module
	}
	return strings.ToLower(module)
}

func pythonPyproject(distribution, module, version, requirement string) string {
	return "[project]\n" +
		"name = " + jsonDumps(distribution) + "\n" +
		"version = " + jsonDumps(version) + "\n" +
		"description = \"Generated MeshCall Python client\"\n" +
		"readme = \"README.md\"\n" +
		"requires-python = \">=3.11\"\n" +
		"dependencies = [\n" +
		"    " + jsonDumps(requirement) + ",\n" +
		"    \"pydantic>=2.11\",\n" +
		"]\n\n" +
		"[build-system]\n" +
		"requires = [\"hatchling\"]\n" +
		"build-backend = \"hatchling.build\"\n\n" +
		"[tool.hatch.build.targets.wheel]\n" +
		"packages = [" + jsonDumps("src/"+module) + "]\n\n" +
		"[tool.uv]\n" +
		"package = true\n"
}

func pythonReadme(distribution, module, className string, typeNames []string) string {
	imports := strings.Join(append([]string{className}, typeNames...), ", ")
	return "# " + distribution + "\n\n" +
		"Generated MeshCall Python client package.\n\n" +
		"```bash\nuv sync\nuv build\n```\n\n" +
		"```python\nfrom " + module + " import " + imports + "\n```\n"
}

func pythonInit(className string, typeNames []string) string {
	models := append([]string(nil), typeNames...)
	sort.Strings(models)
	exports := append([]string{className}, typeNames...)
	sort.Strings(exports)
	quoted := make([]string, len(exports))
	for i, e := range exports {
		quoted[i] = pyStringRepr(e)
	}
	return "from .client import " + className + "\n" +
		"from .models import " + strings.Join(models, ", ") + "\n\n" +
		"__all__ = [" + strings.Join(quoted, ", ") + "]\n"
}

func typescriptPackageJSON(name, version, runtimeImport, runtimeVersion string) string {
	return "{\n" +
		"  \"name\": " + jsonDumps(name) + ",\n" +
		"  \"version\": " + jsonDumps(version) + ",\n" +
		"  \"type\": \"module\",\n" +
		"  \"main\": \"dist/index.js\",\n" +
		"  \"types\": \"dist/index.d.ts\",\n" +
		"  \"exports\": {\n" +
		"    \".\": {\n" +
		"      \"types\": \"./dist/index.d.ts\",\n" +
		"      \"import\": \"./dist/index.js\"\n" +
		"    }\n" +
		"  },\n" +
		"  \"files\": [\n" +
		"    \"dist\"\n" +
		"  ],\n" +
		"  \"license\": \"UNLICENSED\",\n" +
		"  \"sideEffects\": false,\n" +
		"  \"packageManager\": \"yarn@1.22.22\",\n" +
		"  \"scripts\": {\n" +
		"    \"build\": \"tsc -p tsconfig.json\",\n" +
		"    \"check\": \"tsc -p tsconfig.json --noEmit\"\n" +
		"  },\n" +
		"  \"dependencies\": {\n" +
		"    " + jsonDumps(runtimeImport) + ": " + jsonDumps(runtimeVersion) + "\n" +
		"  },\n" +
		"  \"devDependencies\": {\n" +
		"    \"typescript\": \"^7.0.2\"\n" +
		"  }\n" +
		"}\n"
}

func typescriptTsconfig() string {
	return "{\n" +
		"  \"compilerOptions\": {\n" +
		"    \"target\": \"ES2022\",\n" +
		"    \"module\": \"NodeNext\",\n" +
		"    \"moduleResolution\": \"NodeNext\",\n" +
		"    \"rootDir\": \"src\",\n" +
		"    \"outDir\": \"dist\",\n" +
		"    \"declaration\": true,\n" +
		"    \"strict\": true,\n" +
		"    \"noUncheckedIndexedAccess\": true,\n" +
		"    \"exactOptionalPropertyTypes\": true,\n" +
		"    \"skipLibCheck\": false\n" +
		"  },\n" +
		"  \"include\": [\n" +
		"    \"src/**/*.ts\"\n" +
		"  ]\n" +
		"}\n"
}

func typescriptReadme(name, className string, typeNames []string) string {
	imports := strings.Join(append([]string{className}, typeNames...), ", ")
	return "# " + name + "\n\n" +
		"Generated MeshCall TypeScript client package.\n\n" +
		"```bash\nyarn install\nyarn build\n```\n\n" +
		"```typescript\nimport { " + imports + " } from " + jsonDumps(name) + ";\n```\n"
}
