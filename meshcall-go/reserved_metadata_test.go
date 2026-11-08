package meshcall

import "testing"

// Reserved keys are set lower-case by the runtime and the Router; a caller's
// "MESHCALL-…" key must never stand in for them, in Meta or on the wire.
func TestReservedMetadataKeysIgnoreLetterCase(t *testing.T) {
	type principal string
	forged := &depScope{ctx: &Context{Metadata: map[string]string{"MESHCALL-TUNNEL-PRINCIPAL": "user:mallory", "X-User": "bob"}}}
	reserved := Meta[principal]("MeshCall-Tunnel-Principal").(*depSpec)
	if v, err := forged.resolveMeta(reserved); err != nil || v.String() != "" {
		t.Fatalf("Meta read a mixed-case reserved key: %q %v", v.String(), err)
	}
	if v, err := forged.resolveMeta(Meta[principal]("x-user").(*depSpec)); err != nil || v.String() != "bob" {
		t.Fatalf("ordinary keys still match case-insensitively: %q %v", v.String(), err)
	}
	set := &depScope{ctx: &Context{Metadata: map[string]string{"meshcall-tunnel-principal": "user:alice"}}}
	if v, err := set.resolveMeta(reserved); err != nil || v.String() != "user:alice" {
		t.Fatalf("Meta misses the Router's key: %q %v", v.String(), err)
	}
	for _, key := range []string{"meshcall-x", "MeshCall-Tunnel-Principal"} {
		if e := checkMetadata(map[string]string{key: "v"}); e == nil || e.Code != CodeInvalidArgument || e.Reason() != "reserved_metadata" {
			t.Fatalf("%s: %v", key, e)
		}
	}
	if e := checkMetadata(map[string]string{"traceparent": "00"}); e != nil {
		t.Fatalf("ordinary metadata refused: %v", e)
	}
}
