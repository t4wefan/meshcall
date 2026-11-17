package conformance

func init() {
	ClientRuntimes = append(ClientRuntimes, ClientRuntime{
		Name: "swift", Tests: "meshcall-swift/Tests",
		Skipped: map[string]string{
			"duplex.no_idempotency_key": "refused at compile time: DuplexCallOptions has no idempotency key",
		},
	})
}
