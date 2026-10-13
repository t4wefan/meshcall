package codegen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

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
