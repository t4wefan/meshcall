package meshcall

import (
	"context"
	"errors"
	"strings"
)

// CredentialSource returns the credential to connect to router with (its URL
// as the Router lists it: wss://…, unix:///…, pipe://…). The runtime calls it
// before every connection, registration and HTTP request; caching is up to
// the source. A Router refusing the old credential asks it again
// (docs/protocol.md → Credential sources). A Mesh over one Router may ignore
// router.
type CredentialSource func(ctx context.Context, router string) (string, error)

// CredentialSourceError reports a credential a source returned that cannot
// be used as configured (a tunnel credential given as a token, a plain one
// where an end-to-end key is pinned): a configuration error, never retried.
type CredentialSourceError struct{ Message string }

func (e *CredentialSourceError) Error() string { return "meshcall: " + e.Message }

// askCredentialSource asks source for a credential and checks it; a failing
// source fails this connection attempt as an unreachable Router would.
func askCredentialSource(ctx context.Context, source CredentialSource, router string, check func(string) error) (string, error) {
	credential, err := source(ctx, router)
	if err != nil {
		var kind *CredentialSourceError
		if errors.As(err, &kind) {
			return "", kind
		}
		if e, ok := AsError(err); ok {
			return "", e
		}
		return "", detailed(CodeUnavailable, "The credential source failed: "+err.Error(), "client", "credential_source", true)
	}
	if credential == "" {
		return "", detailed(CodeUnavailable, "The credential source returned no credential", "client", "credential_source", true)
	}
	if check != nil {
		if err := check(credential); err != nil {
			return "", &CredentialSourceError{Message: strings.TrimPrefix(err.Error(), "meshcall: ")}
		}
	}
	return credential, nil
}

// kindedSource checks that what a WithTokenSource or WithTunnelSource source
// returns is that kind of credential.
func kindedSource(source CredentialSource, tunnel bool) CredentialSource {
	return func(ctx context.Context, router string) (string, error) {
		credential, err := source(ctx, router)
		if err != nil || credential == "" {
			return credential, err
		}
		isTunnel := strings.HasPrefix(credential, tunnelPrefix)
		switch {
		case tunnel && !isTunnel:
			return "", &CredentialSourceError{Message: "the tunnel source returned no tunnel credential (mtun_…)"}
		case !tunnel && isTunnel:
			return "", &CredentialSourceError{Message: "the token source returned a tunnel credential: pass it as WithTunnelSource"}
		}
		return credential, nil
	}
}

// isRouterRefusal reports whether the Router refused the credential itself
// (HTTP 401 or close 4401), not an instance a handshake.
func isRouterRefusal(err error) bool {
	e, ok := AsError(err)
	return ok && e.Code == CodeUnauthenticated && e.Details["source"] == "router"
}

// checkE2EAgreement refuses a credential and a pinned key that disagree,
// before anything is sent: an end-to-end tunnel never sees an unsealed
// payload by mistake.
func checkE2EAgreement(credential, e2eKey string) error {
	switch {
	case isE2ECredential(credential) && e2eKey == "":
		return errors.New("meshcall: this is an end-to-end tunnel credential: pass the instance's e2e key the issuer gave you (WithE2EKey)")
	case e2eKey != "" && !isE2ECredential(credential):
		return errors.New("meshcall: an e2e key needs the credential of an end-to-end tunnel (mtun_e2e.…)")
	}
	return nil
}
