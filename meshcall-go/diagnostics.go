package meshcall

import (
	"os"
	"regexp"
	"strings"
)

// Advisory contract warnings, the same codes and rules as the Python and
// TypeScript runtimes (docs/architecture.md → Contract warnings). They never
// stop a server unless strict mode is on (ServerOptions.Strict or
// MESHCALL_STRICT=1); MESHCALL_IGNORE_WARNINGS=W001,W002 drops codes.

// ContractWarning is one finding with an instruction for fixing it.
type ContractWarning struct {
	Code    string
	Subject string
	Message string
	Hint    string
	Silence string
}

func (w ContractWarning) String() string {
	return w.Code + " " + w.Subject + ": " + w.Message + "\n  hint: " + w.Hint + "\n  silence: " + w.Silence
}

var (
	mutatingName = regexp.MustCompile(`^(create|add|insert|update|set|put|patch|delete|remove|drop|clear|reset|send|post|write|save|upload|cancel|revoke|issue|register|unregister|start|stop|restart|apply|assign|move|rename|archive|restore|purge)(_|$)`)
	readingName  = regexp.MustCompile(`^(get|list|read|fetch|find|search|query|count|describe|show|check|is|has|exists|lookup|peek|preview|validate|estimate|stream|watch)(_|$)`)
)

// LintService returns the advisory warnings for s.
func LintService(s *Service) []ContractWarning {
	var out []ContractWarning
	if s.description == nil {
		out = append(out, ContractWarning{"W001", s.name, "service has no description",
			"Add meshcall.Describe(\"…\") saying what the service is for. It becomes the contract's description and the text LLM callers and generated clients show.",
			`meshcall.Describe("") to declare none on purpose`})
	}
	for _, m := range s.methods {
		subject := s.name + "." + m.name
		o := m.opts
		annotated := o.idempotent || o.readOnly || o.destructive
		if o.description == nil {
			out = append(out, ContractWarning{"W001", subject, "method has no description",
				"Add meshcall.Doc(\"…\") stating what the call does and when to use it. Mention side effects. This becomes the tool description LLM callers see.",
				`meshcall.Doc("") to declare none on purpose`})
		}
		if mutatingName.MatchString(m.name) && !annotated {
			out = append(out, ContractWarning{"W002", subject, "name suggests side effects but the method has no annotation",
				"Declare its semantics so retry policies and tools can reason about it: meshcall.Idempotent() if repeating it is safe, meshcall.Destructive() if it cannot be undone, or meshcall.ReadOnly() if the name is misleading.",
				"any of Idempotent(), ReadOnly(), Destructive()"})
		}
		if o.readOnly && mutatingName.MatchString(m.name) {
			out = append(out, ContractWarning{"W003", subject, "ReadOnly() on a method whose name suggests mutation",
				"Either the name or the annotation is wrong. Rename the method (get_/list_/…) or drop ReadOnly() and declare Idempotent() / Destructive() instead.",
				"fix the name or the annotation"})
		}
		if o.destructive && readingName.MatchString(m.name) {
			out = append(out, ContractWarning{"W003", subject, "Destructive() on a method whose name suggests a read",
				"Either the name or the annotation is wrong. Rename the method to say what it changes, or drop Destructive().",
				"fix the name or the annotation"})
		}
		if o.deprecated != nil && *o.deprecated == "" {
			out = append(out, ContractWarning{"W005", subject, "deprecated without naming a replacement",
				`Give callers somewhere to go: meshcall.Deprecated("new_name"). Use a short sentence if there is no replacement.`,
				`meshcall.Deprecated("no replacement")`})
		}
		balance := s.balance
		if o.balance != nil {
			balance = *o.balance
		}
		if s.targeted && balance.Kind == "sticky" {
			out = append(out, ContractWarning{"W006", subject, "sticky balancing on a targeted service is never used",
				"Targeted services route by the instance id the caller passes; the Router ignores the sticky key. Drop the sticky policy or drop Targeted().",
				"remove the sticky policy"})
		}
	}
	return out
}

func strictMode() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("MESHCALL_STRICT"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func withoutIgnored(findings []ContractWarning) []ContractWarning {
	ignored := map[string]bool{}
	for _, code := range strings.Split(os.Getenv("MESHCALL_IGNORE_WARNINGS"), ",") {
		if code = strings.ToUpper(strings.TrimSpace(code)); code != "" {
			ignored[code] = true
		}
	}
	var out []ContractWarning
	for _, f := range findings {
		if !ignored[f.Code] {
			out = append(out, f)
		}
	}
	return out
}
