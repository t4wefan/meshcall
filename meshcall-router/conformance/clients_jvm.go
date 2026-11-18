package conformance

func init() {
	ClientRuntimes = append(ClientRuntimes, ClientRuntime{
		Name: "jvm", Tests: "meshcall-jvm/meshcall/src/test",
	})
}
