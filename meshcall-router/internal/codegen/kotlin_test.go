package codegen

import "testing"

func TestKotlinNames(t *testing.T) {
	members := map[string]string{"session_id": "sessionId", "max_tokens": "maxTokens", "ID": "id", "2fa": "_2fa"}
	for input, want := range members {
		if got := kotlinMemberName(input, "x"); got != want {
			t.Errorf("kotlinMemberName(%q) = %q, want %q", input, got, want)
		}
	}
	if got := kotlinEscape("in"); got != "`in`" {
		t.Errorf("kotlinEscape(in) = %q", got)
	}
	if got := kotlinString(`a"$b`); got != `"a\"\$b"` {
		t.Errorf("kotlinString = %s", got)
	}
}
