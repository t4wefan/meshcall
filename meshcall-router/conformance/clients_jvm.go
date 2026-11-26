package conformance

func init() {
	ClientRuntimes = append(ClientRuntimes, ClientRuntime{
		Name: "jvm", Tests: "meshcall-jvm/meshcall/src/test",
		Skipped: map[string]string{
			"auth.credential_source": "credential sources are not implemented in the JVM client yet (#83)",
		},
	})
}
