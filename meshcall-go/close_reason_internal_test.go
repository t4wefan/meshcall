package meshcall

import "testing"

func TestA4401CloseNamesTheReasonTheRouterGave(t *testing.T) {
	for _, reason := range []string{"tunnel_revoked", "tunnel_closed", "key_changed", "token_revoked"} {
		if got := closeError(closeUnauthenticated, reason); got == nil || got.Code != CodeUnauthenticated || got.Reason() != reason {
			t.Errorf("close 4401 %q: %+v", reason, got)
		}
	}
	if got := closeError(closeUnauthenticated, "authentication failed"); got == nil || got.Reason() != "" {
		t.Errorf("close 4401 with a free-text reason: %+v", got)
	}
}
