package conformance

// The client scenarios: what every client runtime must show it does, against
// a client environment (env.go, CLIENTS.md). Each runtime implements them in
// its own test framework and marks each test with a line
//
//	conformance-scenario: <id>
//
// TestClientRuntimesCoverEveryScenario checks that every runtime listed in
// ClientRuntimes marks every scenario, or says why it skips it.

// ClientScenario is one behaviour a client runtime must show.
type ClientScenario struct {
	ID   string
	What string
}

var ClientScenarios = []ClientScenario{
	{"unary.round_trip", "echo returns its input; unknown response fields are ignored"},
	{"unary.invalid_request", "a request failing the server's contract is invalid_argument, source server, reason request, with violations; the connection stays"},
	{"unary.invalid_response", "bad_response is internal, reason invalid_response (the server's own check)"},
	{"unary.undecodable_response", "a response this client's types cannot decode is protocol_error, source client, reason invalid_response, with violations"},
	{"unary.application_error", "fail passes code, message, retryable and details through; not retried without an idempotency key"},
	{"unary.retry", "a retryable error with an idempotency key is retried; the final error carries details.attempts"},
	{"targeted.instance", "a targeted service needs instance_id (instance_required), routes by it, and refuses an unknown one (instance_unknown)"},
	{"router.method_not_found", "an unknown method is method_not_found"},
	{"stream.server_credit", "a server stream longer than the initial credit arrives in full and in order"},
	{"stream.concurrent", "concurrent server streams on one connection keep their own credit"},
	{"stream.early_exit", "leaving a stream early cancels the call; the connection stays usable"},
	{"stream.client_stream", "a client stream sends every item, ends its input and returns the result"},
	{"deadline.server", "timeout_ms enforced by the server: deadline_exceeded, not retryable"},
	{"deadline.idle", "idle_timeout_ms on output enforced by the client: deadline_exceeded, source client, reason idle"},
	{"deadline.connect", "the client budget covers connecting: nothing listening is a retryable unavailable"},
	{"deadline.handshake", "the client budget covers a handshake nobody answers: deadline_exceeded, source client, within timeout plus grace"},
	{"cancel.caller", "a caller's cancellation ends the call locally as cancelled and the handler sees call.cancel"},
	{"metadata.round_trip", "request metadata reaches the handler and trailers reach the caller"},
	{"metadata.reserved", "meshcall- keys are refused before sending: invalid_argument, reason reserved_metadata"},
	{"duplex.interleaved", "duplex items flow both ways while the input is open; half-closing the input ends it"},
	{"duplex.server_half_close", "the output may end while the input stays open"},
	{"duplex.no_idempotency_key", "a duplex call cannot carry an idempotency key"},
	{"frame_size.request", "an oversized request fails only its call: resource_exhausted, source client, reason frame_too_large"},
	{"tunnel.scope", "a tunnel reaches only its instance's scoped services (tunnel_scope otherwise) and carries the issuer's identity"},
	{"tunnel.revoked", "revoking ends calls with unauthenticated / tunnel_revoked, and later calls fail the same way without reconnecting"},
	{"e2e.all_shapes", "an end-to-end tunnel seals every shape; sealed error messages and details open on the client"},
	{"e2e.wrong_key", "a pinned key the instance does not hold fails the handshake: unauthenticated / e2e_handshake_failed"},
	{"e2e.required", "an instance requiring end-to-end refuses a plain tunnel at open: tunnel_rejected"},
	{"e2e.agreement", "an end-to-end credential without its key (or a key without one) is refused before connecting"},
	{"e2e.session_lost", "after the instance restarts, e2e_session_unknown makes the client handshake again and retry a unary call once"},
	{"e2e.test_vector", "the Noise handshakes match testdata/noise_{nk,ik,ikpsk2}_25519_aesgcm_sha256.json and the requests, answers and confirmation in testdata/e2e_handshakes.json"},
	{"e2e.ik", "a client with a key the instance accepts reaches conformance.v1.Paired: peer reports Noise_IK_25519_AESGCM_SHA256 and the client's public key"},
	{"e2e.ik_unknown_key", "a client key the instance does not accept fails the handshake: unauthenticated / e2e_handshake_failed, not retried"},
	{"e2e.peer_required", "a client without a key of its own (NK) is refused by conformance.v1.Paired: permission_denied / e2e_peer_required; it still reaches conformance.v1.Fixture"},
	{"e2e.pairing", "a client pairing with an unspent pairing reaches Paired: peer reports Noise_IKpsk2_25519_AESGCM_SHA256 and the pairing id; another key with the same pairing then fails with e2e_handshake_failed, and a new client with the paired key and no pairing reaches Paired with IK"},
	{"e2e.pairing_wrong_psk", "a pairing with a wrong PSK fails with e2e_handshake_failed and does not spend the pairing: the right PSK then pairs"},
	{"e2e.pairing_reconnect", "after a confirmed pairing, a client whose connection was lost (pause_router, the client's connection check, resume_router) handshakes again with IK and its next call reaches Paired"},
	{"reconnect.router_restart", "a lost connection fails its calls with a retryable unavailable; the next call reconnects"},
	{"reconnect.dead_socket", "a connection that stops answering (pause_router) is dropped, failing its calls retryably; the next call reconnects"},
	{"reconnect.failover", "with several Router URLs, an unreachable one is skipped"},
	{"auth.credential_source", "a client given a credential source connects with what it returns; after the Router revokes that tunnel the next call connects with a new credential from the source and succeeds; a source whose new credential is refused too fails the call with unauthenticated"},
	{"routers.several", "a client of several Routers reaches an instance registered on only one of them, in every call shape, moving on from Routers that answer no_instances / instance_unknown or cannot be reached; the Router's own services need a view of one Router; a tunnel opens where its instance is and names that Router"},
}

// ClientRuntime is a client runtime whose tests mark the scenarios.
type ClientRuntime struct {
	Name string
	// Tests is the directory (from the repository root) holding the marked tests.
	Tests string
	// Skipped scenarios, with the reason.
	Skipped map[string]string
}

// ClientRuntimes lists the runtimes under test; each adds itself from its
// own clients_<runtime>.go.
var ClientRuntimes []ClientRuntime
