package meshcall_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The Go generator's golden output (meshcall-router/internal/codegen/testdata)
// must compile against this runtime: golden files alone only pin bytes.
func TestGeneratedClientsCompile(t *testing.T) {
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not on PATH")
	}
	clients, _ := filepath.Glob("../meshcall-router/internal/codegen/testdata/*/client.go")
	if len(clients) == 0 {
		t.Skip("generator golden files not found")
	}
	root := filepath.Join("testdata", "generated")
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	var packages []string
	for _, client := range clients {
		name := filepath.Base(filepath.Dir(client))
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(client)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "client.go"), data, 0o644); err != nil {
			t.Fatal(err)
		}
		packages = append(packages, "./"+filepath.ToSlash(dir))
	}
	cmd := exec.Command(goTool, append([]string{"vet"}, packages...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated clients do not compile:\n%s", out)
	}
}
