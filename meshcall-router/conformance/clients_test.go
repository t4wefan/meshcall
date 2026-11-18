package conformance

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

var scenarioMarker = regexp.MustCompile(`conformance-scenario: ([a-z0-9_.]+)`)

// Every client runtime marks every client scenario in its tests, or says
// why it skips it; no marker names a scenario that does not exist.
func TestClientRuntimesCoverEveryScenario(t *testing.T) {
	known := map[string]bool{}
	for _, s := range ClientScenarios {
		if known[s.ID] {
			t.Fatalf("scenario %s listed twice", s.ID)
		}
		known[s.ID] = true
	}
	for _, runtime := range ClientRuntimes {
		t.Run(runtime.Name, func(t *testing.T) {
			marked := map[string]bool{}
			root := filepath.Join("..", "..", runtime.Tests)
			err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				for _, m := range scenarioMarker.FindAllStringSubmatch(string(data), -1) {
					if !known[m[1]] {
						t.Errorf("%s marks unknown scenario %s", path, m[1])
					}
					marked[m[1]] = true
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			var missing []string
			for id := range known {
				_, skipped := runtime.Skipped[id]
				if marked[id] && skipped {
					t.Errorf("%s is both marked and skipped", id)
				}
				if !marked[id] && !skipped {
					missing = append(missing, id)
				}
			}
			for id := range runtime.Skipped {
				if !known[id] {
					t.Errorf("skips unknown scenario %s", id)
				}
			}
			sort.Strings(missing)
			for _, id := range missing {
				t.Errorf("no test marks scenario %s (conformance-scenario: %s), and it is not skipped", id, id)
			}
		})
	}
}
