package meshcall_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	meshcall "github.com/t4wefan/meshcall/meshcall-go"
)

func TestLocalRouterAndMeshDX(t *testing.T) {
	if os.Getenv("MESHCALL_ROUTER_BINARY") == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router DX tests")
	}

	type AddReq struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	type AddResp struct {
		Sum int `json:"sum"`
	}
	type EchoReq struct {
		Msg string `json:"msg"`
	}
	type EchoResp struct {
		Echoed string `json:"echoed"`
	}

	mathSvc := meshcall.NewService("test.v1.Math", meshcall.Describe("Math service."))
	meshcall.HandleUnary(mathSvc, "add", func(_ *meshcall.Context, req AddReq) (AddResp, error) {
		return AddResp{Sum: req.A + req.B}, nil
	}, meshcall.Doc("Add two integers."), meshcall.Idempotent())

	echoSvc := meshcall.NewService("test.v1.Echo", meshcall.Describe("Echo service."))
	meshcall.HandleUnary(echoSvc, "ping", func(callCtx *meshcall.Context, req EchoReq) (EchoResp, error) {
		prefix := callCtx.Metadata["x-prefix"]
		return EchoResp{Echoed: prefix + req.Msg}, nil
	}, meshcall.Doc("Echo message."), meshcall.Idempotent())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	router, err := meshcall.StartLocalRouter(
		ctx,
		meshcall.WithPort(0),
		meshcall.WithHTTPConfig(15*time.Second, "https://app.example.com"),
	)
	if err != nil {
		t.Fatalf("StartLocalRouter failed: %v", err)
	}
	defer func() { _ = router.Stop() }()

	if !router.IsRunning() || router.BoundPort() <= 0 || router.PID() <= 0 {
		t.Fatalf("unexpected LocalRouter state: running=%v port=%d pid=%d", router.IsRunning(), router.BoundPort(), router.PID())
	}

	serverMesh := meshcall.NewMesh(router)
	serverMesh.Mount(mathSvc, echoSvc)
	running, err := serverMesh.StartServing(
		ctx,
		meshcall.WithInstanceID("go-dx-node-1"),
		meshcall.WithStrict(true),
	)
	if err != nil {
		t.Fatalf("StartServing failed: %v", err)
	}
	defer func() { _ = running.Close() }()

	// Verify HTTP and WebSocket share the exact same Router port.
	httpBase := router.HTTPURL()
	healthResp, err := http.Get(httpBase + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	_ = healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /healthz, got %d", healthResp.StatusCode)
	}

	httpCallBody, _ := json.Marshal(AddReq{A: 10, B: 25})
	httpCallResp, err := http.Post(httpBase+"/v1/call/test.v1.Math/add", "application/json", bytes.NewReader(httpCallBody))
	if err != nil {
		t.Fatalf("POST /v1/call failed: %v", err)
	}
	defer func() { _ = httpCallResp.Body.Close() }()
	var httpAddOut AddResp
	if err := json.NewDecoder(httpCallResp.Body).Decode(&httpAddOut); err != nil || httpAddOut.Sum != 35 {
		t.Fatalf("unexpected HTTP call output: %+v, err=%v", httpAddOut, err)
	}

	// Connect client-side Mesh via Router(url) and call built-in Registry + multiplex services.
	clientMesh, err := meshcall.ConnectMesh(ctx, meshcall.NewRouter(router.URL()))
	if err != nil {
		t.Fatalf("ConnectMesh failed: %v", err)
	}
	defer func() { _ = clientMesh.Close() }()

	services, err := clientMesh.Registry().List(ctx)
	if err != nil {
		t.Fatalf("Registry().List failed: %v", err)
	}
	if len(services) < 2 {
		t.Fatalf("expected at least 2 services, got %+v", services)
	}

	detail, err := clientMesh.Registry().Describe(ctx, "test.v1.Math")
	if err != nil {
		t.Fatalf("Registry().Describe failed: %v", err)
	}
	if detail.Name != "test.v1.Math" || len(detail.Instances) != 1 || detail.Instances[0].ID != "go-dx-node-1" {
		t.Fatalf("unexpected service detail: %+v", detail)
	}

	// Call both services over the shared Mesh and verify WithOptions.
	sumResp, err := meshcall.Unary[AddReq, AddResp](ctx, clientMesh, "test.v1.Math", "add", AddReq{A: 19, B: 23})
	if err != nil || sumResp.Sum != 42 {
		t.Fatalf("Math.add failed: %+v, err=%v", sumResp, err)
	}

	scopedMesh := clientMesh.WithOptions(meshcall.CallOptions{
		Timeout:  5 * time.Second,
		Metadata: map[string]string{"x-prefix": "hi:"},
	})
	echoResp, err := meshcall.Unary[EchoReq, EchoResp](ctx, scopedMesh, "test.v1.Echo", "ping", EchoReq{Msg: "world"})
	if err != nil || echoResp.Echoed != "hi:world" {
		t.Fatalf("Echo.ping with WithOptions failed: %+v, err=%v", echoResp, err)
	}
}

func TestMeshAuthBuiltinsDX(t *testing.T) {
	bin := os.Getenv("MESHCALL_ROUTER_BINARY")
	if bin == "" {
		t.Skip("set MESHCALL_ROUTER_BINARY to run Router DX tests")
	}

	hashCmd := exec.Command(bin, "hash-password")
	hashCmd.Stdin = strings.NewReader("secret\n")
	hashOut, err := hashCmd.Output()
	if err != nil {
		t.Fatalf("hash-password failed: %v", err)
	}
	pwHash := strings.TrimSpace(string(hashOut))

	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "auth.json")
	authJSON := fmt.Sprintf(`{
		"users": [
			{
				"username": "admin",
				"password_hash": %q,
				"roles": ["client", "server"],
				"register": ["*"],
				"call": ["*"]
			}
		]
	}`, pwHash)
	if err := os.WriteFile(authFile, []byte(authJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	type PingReq struct{}
	type PingResp struct {
		OK bool `json:"ok"`
	}
	svc := meshcall.NewService("test.v1.Echo", meshcall.Describe("Echo service."))
	meshcall.HandleUnary(svc, "ping", func(_ *meshcall.Context, _ PingReq) (PingResp, error) {
		return PingResp{OK: true}, nil
	}, meshcall.Doc("Ping."), meshcall.Idempotent())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	router, err := meshcall.StartLocalRouter(
		ctx,
		meshcall.WithPort(0),
		meshcall.WithAuthFile(authFile),
	)
	if err != nil {
		t.Fatalf("StartLocalRouter with auth failed: %v", err)
	}
	defer func() { _ = router.Stop() }()

	adminAuth := meshcall.BasicAuth("admin", "secret")
	serverMesh := meshcall.NewMesh(router, meshcall.WithAuth(adminAuth))
	serverMesh.Mount(svc)
	running, err := serverMesh.StartServing(ctx, meshcall.WithStrict(true))
	if err != nil {
		t.Fatalf("StartServing with auth failed: %v", err)
	}
	defer func() { _ = running.Close() }()

	adminMesh, err := meshcall.ConnectMesh(
		ctx,
		meshcall.NewRouter(router.URL()),
		meshcall.WithAuth(adminAuth),
	)
	if err != nil {
		t.Fatalf("ConnectMesh admin failed: %v", err)
	}
	defer func() { _ = adminMesh.Close() }()

	me, err := adminMesh.Auth().Whoami(ctx)
	if err != nil || me.Username != "admin" {
		t.Fatalf("Auth().Whoami admin failed: %+v, err=%v", me, err)
	}

	issued, err := adminMesh.Auth().IssueToken(ctx, []meshcall.RouterScope{
		{Service: "test.v1.Echo", Methods: []string{"ping"}},
	}, 600)
	if err != nil || !strings.HasPrefix(issued.Token, "mt_") || issued.TokenID == "" {
		t.Fatalf("Auth().IssueToken failed: %+v, err=%v", issued, err)
	}

	scopedMesh, err := meshcall.ConnectMesh(
		ctx,
		meshcall.NewRouter(router.URL()),
		meshcall.WithToken(issued.Token),
	)
	if err != nil {
		t.Fatalf("ConnectMesh scoped failed: %v", err)
	}
	defer func() { _ = scopedMesh.Close() }()

	scopedMe, err := scopedMesh.Auth().Whoami(ctx)
	if err != nil || scopedMe.Username != "admin" || len(scopedMe.Call) != 1 || scopedMe.Call[0] != "test.v1.Echo/ping" {
		t.Fatalf("Auth().Whoami scoped failed: %+v, err=%v", scopedMe, err)
	}

	revoked, err := adminMesh.Auth().RevokeToken(ctx, issued.TokenID)
	if err != nil || !revoked.Revoked {
		t.Fatalf("Auth().RevokeToken failed: %+v, err=%v", revoked, err)
	}
}
