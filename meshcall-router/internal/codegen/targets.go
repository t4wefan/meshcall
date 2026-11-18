package codegen

import (
	"path/filepath"
	"sort"
	"strings"

	"github.com/t4wefan/meshcall/meshcall-router/internal/contract"
)

// A Target is a client generator that writes one file per service. Each
// language registers itself from its own file (registerTarget in an init),
// so adding one touches neither the CLI nor the golden test.
type Target struct {
	// Language is the --language value.
	Language string
	// Extension is the file suffix; an --output ending in it names the file.
	Extension string
	// Golden is the testdata/<case>/ file that pins the output.
	Golden string
	// FileName is the file written into an --output directory.
	FileName func(service *contract.Service, opts TargetOptions) string
	Render   func(service *contract.Service, opts TargetOptions) string
}

// TargetOptions are the CLI options a single-file target may use.
type TargetOptions struct {
	ClassName     string // the client type
	PackageName   string // Go package, JVM package, …
	RuntimeImport string // the runtime module or package to import
}

var targets = map[string]Target{}

func registerTarget(t Target) {
	if _, dup := targets[t.Language]; dup {
		panic("codegen: target registered twice: " + t.Language)
	}
	targets[t.Language] = t
}

// LookupTarget returns the single-file target for a --language value.
func LookupTarget(language string) (Target, bool) {
	t, ok := targets[language]
	return t, ok
}

// Targets lists the single-file targets by language.
func Targets() []Target {
	out := make([]Target, 0, len(targets))
	for _, t := range targets {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Language < out[j].Language })
	return out
}

// TargetPath is the file a target writes for --output: the path itself when
// it ends in the target's extension, else FileName inside that directory.
func TargetPath(t Target, service *contract.Service, output string, opts TargetOptions) string {
	if strings.HasSuffix(output, t.Extension) {
		return output
	}
	return filepath.Join(output, t.FileName(service, opts))
}
