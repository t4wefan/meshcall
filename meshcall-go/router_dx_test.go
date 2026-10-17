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

func TestServeAndRouterDX(t *testing.T) {
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

	routerCfg := meshcall.NewRouter(
		meshcall.WithRouterURLs("ws://127.0.0.1:0"),
		meshcall.WithHTTPConfig(15*time.Second, "https://app.example.com"),
	)

	running, err := meshcall.Serve(
		ctx,
		mathSvc,
		echoSvc,
		meshcall.WithRouter(routerCfg),
		meshcall.WithInstanceID("go-dx-node-1"),
		meshcall.WithStrict(true),
	)
	if err != nil {
		t.Fatalf("Serve failed: %v", err)
	}
	defer func() { _ = running.Close() }()

	// Verify HTTP and WebSocket share the exact same Router port.
	httpBase := strings.Replace(running.RouterURL, "ws://", "http://", 1)
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

	// Connect client-side Router and call built-in Registry + multiplex services.
	router, err := meshcall.ConnectRouter(ctx, meshcall.WithRouterURLs(running.RouterURL))
	if err != nil {
		t.Fatalf("ConnectRouter failed: %v", err)
	}
	defer func() { _ = router.Close() }()

	services, err := router.ListServices(ctx)
	if err != nil {
		t.Fatalf("ListServices failed: %v", err)
	}
	if len(services) < 2 {
		t.Fatalf("expected at least 2 services, got %+v", services)
	}

	detail, err := router.DescribeService(ctx, "test.v1.Math")
	if err != nil {
		t.Fatalf("DescribeService failed: %v", err)
	}
	if detail.Name != "test.v1.Math" || len(detail.Instances) != 1 || detail.Instances[0].ID != "go-dx-node-1" {
		t.Fatalf("unexpected service detail: %+v", detail)
	}

	// Call both services over the shared Router client and verify WithOptions.
	sumResp, err := meshcall.Unary[AddReq, AddResp](ctx, router.Client(), "test.v1.Math", "add", AddReq{A: 19, B: 23})
	if err != nil || sumResp.Sum != 42 {
		t.Fatalf("Math.add failed: %+v, err=%v", sumResp, err)
	}

	scopedRouter := router.WithOptions(meshcall.CallOptions{
		Timeout:  5 * time.Second,
		Metadata: map[string]string{"x-prefix": "hi:"},
	})
	echoResp, err := meshcall.Unary[EchoReq, EchoResp](ctx, scopedRouter.Client(), "test.v1.Echo", "ping", EchoReq{Msg: "world"})
	if err != nil || echoResp.Echoed != "hi:world" {
		t.Fatalf("Echo.ping with WithOptions failed: %+v, err=%v", echoResp, err)
	}
}

func TestRouterAuthBuiltinsDX(t *testing.T) {
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

	adminAuth := meshcall.BasicAuth("admin", "secret")
	running, err := meshcall.Serve(
		ctx,
		svc,
		meshcall.WithRouterURLs("ws://127.0.0.1:0"),
		meshcall.WithAuthFile(authFile),
		meshcall.WithAuth(adminAuth),
		meshcall.WithStrict(true),
	)
	if err != nil {
		t.Fatalf("Serve with auth failed: %v", err)
	}
	defer func() { _ = running.Close() }()

	adminRouter, err := meshcall.ConnectRouter(
		ctx,
		meshcall.WithRouterURLs(running.RouterURL),
		meshcall.WithAuth(adminAuth),
	)
	if err != nil {
		t.Fatalf("ConnectRouter admin failed: %v", err)
	}
	defer func() { _ = adminRouter.Close() }()

	me, err := adminRouter.Whoami(ctx)
	if err != nil || me.Username != "admin" {
		t.Fatalf("Whoami admin failed: %+v, err=%v", me, err)
	}

	issued, err := adminRouter.IssueToken(ctx, []meshcall.RouterScope{
		{Service: "test.v1.Echo", Methods: []string{"ping"}},
	}, 600)
	if err != nil || !strings.HasPrefix(issued.Token, "mt_") || issued.TokenID == "" {
		t.Fatalf("IssueToken failed: %+v, err=%v", issued, err)
	}

	scopedRouter, err := meshcall.ConnectRouter(
		ctx,
		meshcall.WithRouterURLs(running.RouterURL),
		meshcall.WithAuth(meshcall.BearerToken(issued.Token)),
	)
	if err != nil {
		t.Fatalf("ConnectRouter scoped failed: %v", err)
	}
	defer func() { _ = scopedRouter.Close() }()

	scopedMe, err := scopedRouter.Whoami(ctx)
	if err != nil || scopedMe.Username != "admin" || len(scopedMe.Call) != 1 || scopedMe.Call[0] != "test.v1.Echo/ping" {
		t.Fatalf("Whoami scoped failed: %+v, err=%v", scopedMe, err)
	}

	revoked, err := adminRouter.RevokeToken(ctx, issued.TokenID)
	if err != nil || !revoked.Revoked {
		t.Fatalf("RevokeToken failed: %+v, err=%v", revoked, err)
	}
}
