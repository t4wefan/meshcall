package codegen

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// update rewrites the expected files that exist from the current generators:
// go test ./internal/codegen -update. Review the diff before committing.
var update = flag.Bool("update", false, "rewrite golden files")

// testdata/<case>/ holds a contract and the exact output the original Python
// generators produced for it (single files, package modules, whole packages).
// The Go generators must reproduce every byte.
func TestGeneratorsMatchGolden(t *testing.T) {
	cases, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range cases {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join("testdata", entry.Name())
		t.Run(entry.Name(), func(t *testing.T) {
			doc, err := contract.Load(filepath.Join(dir, "contract.json"))
			if err != nil {
				t.Fatal(err)
			}
			service, err := doc.Select("")
			if err != nil {
				t.Fatal(err)
			}
			expect := func(name, got string) {
				t.Helper()
				want, err := os.ReadFile(filepath.Join(dir, name))
				if os.IsNotExist(err) {
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if *update && string(want) != got {
					if err := os.WriteFile(filepath.Join(dir, name), []byte(got), 0o644); err != nil {
						t.Fatal(err)
					}
					return
				}
				if string(want) != got {
					t.Errorf("%s differs:\n%s", name, firstDiff(string(want), got))
				}
			}
			expect("single.py", RenderPythonClient(service, PythonOptions{}))
			expect("models.py", RenderPythonModels(service))
			expect("client.py", RenderPythonClientModule(service, PythonOptions{}))
			for rel, content := range PythonPackage(service, PackageOptions{}) {
				expect(filepath.Join("py-package", rel), content)
			}
			if single, err := RenderTypeScriptClient(service, TypeScriptOptions{}); err == nil {
				expect("single.ts", single)
				models, _ := RenderTypeScriptModels(service, TypeScriptOptions{})
				expect("models.ts", models)
				client, _ := RenderTypeScriptClientModule(service, TypeScriptOptions{})
				expect("client.ts", client)
				files, _ := TypeScriptPackage(service, PackageOptions{PackageName: "meshcall-demo-client", RuntimeVersion: "file:../../meshcall-ts"})
				for rel, content := range files {
					expect(filepath.Join("ts-package", rel), content)
				}
			} else if _, statErr := os.Stat(filepath.Join(dir, "single.ts")); statErr == nil {
				t.Errorf("TypeScript generation failed: %v", err)
			}
		})
	}
}

func firstDiff(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")
	for i := 0; i < len(w) || i < len(g); i++ {
		var a, b string
		if i < len(w) {
			a = w[i]
		}
		if i < len(g) {
			b = g[i]
		}
		if a != b {
			return "line " + itoa(i+1) + ":\n  want: " + a + "\n  got:  " + b
		}
	}
	return "lengths differ"
}

func TestPythonLiterals(t *testing.T) {
	cases := map[string]string{
		`"it's"`: `"it's"`, `"say \"hi\""`: `'say "hi"'`, `"a\nb"`: `'a\nb'`, `"héllo"`: `'héllo'`,
		`1`: `1`, `1.0`: `1.0`, `0.1`: `0.1`, `1e-05`: `1e-05`, `1e16`: `1e+16`, `123456.0`: `123456.0`, `-2.5`: `-2.5`,
		`true`: `True`, `null`: `None`, `[1, "a", null]`: `[1, 'a', None]`, `{"b": 1, "a": [2]}`: `{'b': 1, 'a': [2]}`,
	}
	for input, want := range cases {
		value, err := contract.DecodeOrdered([]byte(input))
		if err != nil {
			t.Fatal(err)
		}
		if got := pyRepr(value); got != want {
			t.Errorf("repr(%s) = %s, want %s", input, got, want)
		}
	}
	dumps := map[string]string{
		`"héllo 😀"`: `"h\u00e9llo \ud83d\ude00"`, `{"a": [1, 2.5, true, null]}`: `{"a": [1, 2.5, true, null]}`, `"x\"y"`: `"x\"y"`,
	}
	for input, want := range dumps {
		value, _ := contract.DecodeOrdered([]byte(input))
		if got := jsonDumps(value); got != want {
			t.Errorf("dumps(%s) = %s, want %s", input, got, want)
		}
	}
}

// Every model a TypeScript client module mentions must be imported; the
// golden files would happily pin a module that does not compile.
func TestTypeScriptClientImportsEveryModelItUses(t *testing.T) {
	exported := regexp.MustCompile(`export (?:interface|type|const) (\w+)`)
	imports := regexp.MustCompile(`import (?:type )?\{([^}]*)\}`)
	identifiers := regexp.MustCompile(`\b[A-Z]\w*\b`)
	cases, _ := filepath.Glob("testdata/*/ts-package/src/client.ts")
	for _, path := range cases {
		client, _ := os.ReadFile(path)
		models, _ := os.ReadFile(filepath.Join(filepath.Dir(path), "models.ts"))
		imported := map[string]bool{}
		for _, m := range imports.FindAllStringSubmatch(string(client), -1) {
			for _, name := range strings.Split(m[1], ",") {
				imported[strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(name), "type "))] = true
			}
		}
		modelNames := map[string]bool{}
		for _, m := range exported.FindAllStringSubmatch(string(models), -1) {
			modelNames[m[1]] = true
		}
		for _, name := range identifiers.FindAllString(string(client), -1) {
			if modelNames[name] && !imported[name] {
				t.Errorf("%s uses %s without importing it", path, name)
			}
		}
	}
}
