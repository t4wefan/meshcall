package meshcall_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4wefan/meshcall/meshcall-go"
)

type testDatabase struct {
	id     int64
	closed atomic.Bool
}

func (db *testDatabase) Save(path, content string) int {
	return len(content)
}

type testAuditRepo struct {
	db *testDatabase
}

type testCallerID string
type testTenantID string

type testFileStat struct {
	Path   string `json:"path"`
	Size   int    `json:"size"`
	Caller string `json:"caller"`
	Tenant string `json:"tenant"`
	SameDB bool   `json:"same_db"`
}

type testTermIn struct {
	Data string `json:"data"`
}

type testTermOut struct {
	Output string `json:"output"`
}

type testWorkspace struct {
	meshcall.ServiceMeta `name:"workspace" description:"Remote workspace service."`
}

func newTestWorkspace() *testWorkspace {
	return &testWorkspace{}
}

// WriteFile writes content to path.
func (w *testWorkspace) WriteFile(
	ctx context.Context,
	path string,
	content string,
	db *testDatabase,
	repo *testAuditRepo,
	caller testCallerID,
	tenant testTenantID,
) (testFileStat, error) {
	size := db.Save(path, content)
	return testFileStat{
		Path:   path,
		Size:   size,
		Caller: string(caller),
		Tenant: string(tenant),
		SameDB: repo.db == db && repo.db.id == db.id,
	}, nil
}

// TailFile streams lines from path.
func (w *testWorkspace) TailFile(ctx context.Context, path string, lines int) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for i := range lines {
			if !yield(fmt.Sprintf("%s:%d", path, i), nil) {
				return
			}
		}
	}
}

// UploadFile uploads chunks to path.
func (w *testWorkspace) UploadFile(
	ctx context.Context,
	path string,
	caller testCallerID,
	chunks iter.Seq2[string, error],
) (testFileStat, error) {
	total := 0
	for chunk, err := range chunks {
		if err != nil {
			return testFileStat{}, err
		}
		total += len(chunk)
	}
	return testFileStat{
		Path:   path,
		Size:   total,
		Caller: string(caller),
	}, nil
}

// OpenTerminal opens an interactive shell session.
func (w *testWorkspace) OpenTerminal(
	ctx context.Context,
	shell string,
	ch *meshcall.DuplexChannel[testTermIn, testTermOut],
) error {
	if err := ch.Send(ctx, testTermOut{Output: "started " + shell}); err != nil {
		return err
	}
	for msg, err := range ch.Recv(ctx) {
		if err != nil {
			return err
		}
		if msg.Data == "exit" {
			return nil
		}
		if err := ch.Send(ctx, testTermOut{Output: "echo: " + msg.Data}); err != nil {
			return err
		}
	}
	return nil
}

func TestGoDIAndReflectService(t *testing.T) {
	var createdCount atomic.Int64
	var closedCount atomic.Int64

	dbDep := meshcall.Dependency(func(ctx context.Context) (*testDatabase, func(), error) {
		id := createdCount.Add(1)
		db := &testDatabase{id: id}
		return db, func() {
			db.closed.Store(true)
			closedCount.Add(1)
		}, nil
	})
	repoDep := meshcall.Dependency(func(ctx context.Context, db *testDatabase) (*testAuditRepo, error) {
		return &testAuditRepo{db: db}, nil
	})
	callerDep := meshcall.Meta[testCallerID]("x-user-id", meshcall.WithMetaRequired())
	tenantDep := meshcall.Meta[testTenantID]("x-tenant-id", meshcall.WithMetaDefault(testTenantID("default-tenant")))

	svc, err := meshcall.ReflectService(
		newTestWorkspace(),
		meshcall.WithDependencies(dbDep, repoDep, callerDep, tenantDep),
	)
	if err != nil {
		t.Fatalf("ReflectService failed: %v", err)
	}

	// Verify contract strips DI & Meta parameters
	var contract map[string]any
	if err := json.Unmarshal(svc.Contract(), &contract); err != nil {
		t.Fatalf("failed to unmarshal contract: %v", err)
	}
	methods, ok := contract["methods"].([]any)
	if !ok || len(methods) != 4 {
		t.Fatalf("expected 4 methods in contract, got %#v", contract["methods"])
	}
	for _, rawM := range methods {
		m, _ := rawM.(map[string]any)
		if m["name"] == "write_file" {
			params, _ := m["parameters"].([]any)
			if len(params) != 2 || params[0] != "path" || params[1] != "content" {
				t.Fatalf("expected write_file to have 2 parameters (path, content), got %#v", params)
			}
		}
		if m["name"] == "upload_file" {
			params, _ := m["parameters"].([]any)
			if len(params) != 1 || params[0] != "path" {
				t.Fatalf("expected upload_file to have 1 parameter (path), got %#v", params)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	router := meshcall.NewRouter(meshcall.WithRouterURLs("ws://127.0.0.1:0"), meshcall.WithSpawnIfLocal(true))

	handle, err := meshcall.Serve(
		ctx,
		newTestWorkspace(),
		meshcall.WithRouter(router),
		meshcall.WithDependencies(dbDep, repoDep, callerDep, tenantDep),
	)
	if err != nil {
		t.Fatalf("Serve failed: %v", err)
	}
	defer handle.Close()

	conn, err := meshcall.ConnectRouter(ctx, meshcall.WithRouterURLs(handle.RouterURL), meshcall.WithSpawnIfLocal(false))
	if err != nil {
		t.Fatalf("ConnectRouter failed: %v", err)
	}
	defer conn.Close()

	// 1. Missing required Meta ("x-user-id") fails BEFORE acquiring dbDep
	_, err = meshcall.Unary[map[string]any, testFileStat](
		ctx,
		conn.Client(),
		svc.Name(),
		"write_file",
		map[string]any{"path": "/a.txt", "content": "hello"},
	)
	if err == nil {
		t.Fatalf("expected INVALID_ARGUMENT when x-user-id header is missing")
	}
	if createdCount.Load() != 0 {
		t.Fatalf("expected dbDep not to be created on missing required Meta, got %d", createdCount.Load())
	}

	// 2. Unary with DI + Meta + per-call caching + cleanup teardown
	stat, err := meshcall.Unary[map[string]any, testFileStat](
		ctx,
		conn.WithOptions(meshcall.CallOptions{
			Metadata: map[string]string{"x-user-id": "u_42"},
		}).Client(),
		svc.Name(),
		"write_file",
		map[string]any{"path": "/a.txt", "content": "hello world"},
	)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if stat.Path != "/a.txt" || stat.Size != 11 || stat.Caller != "u_42" || stat.Tenant != "default-tenant" || !stat.SameDB {
		t.Fatalf("unexpected WriteFile result: %+v", stat)
	}
	if createdCount.Load() != 1 || closedCount.Load() != 1 {
		t.Fatalf("expected 1 db created and 1 closed, got created=%d closed=%d", createdCount.Load(), closedCount.Load())
	}

	// 3. Server Stream via ServerStreamSeq
	var lines []string
	for line, err := range meshcall.ServerStreamSeq[map[string]any, string](
		ctx,
		conn.Client(),
		svc.Name(),
		"tail_file",
		map[string]any{"path": "/a.txt", "lines": 3},
	) {
		if err != nil {
			t.Fatalf("TailFile error: %v", err)
		}
		lines = append(lines, line)
	}
	if len(lines) != 3 || lines[0] != "/a.txt:0" || lines[2] != "/a.txt:2" {
		t.Fatalf("unexpected TailFile lines: %#v", lines)
	}

	// 4. Client Stream via ClientStreamSeq + SliceSeq
	uploaded, err := meshcall.ClientStreamSeq[map[string]any, string, testFileStat](
		ctx,
		conn.WithOptions(meshcall.CallOptions{
			Metadata: map[string]string{"x-user-id": "u_99"},
		}).Client(),
		svc.Name(),
		"upload_file",
		map[string]any{"path": "/blob.bin"},
		meshcall.SliceSeq([]string{"part1-", "part2"}),
	)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}
	if uploaded.Path != "/blob.bin" || uploaded.Size != 11 || uploaded.Caller != "u_99" {
		t.Fatalf("unexpected UploadFile result: %+v", uploaded)
	}

	// 5. Duplex via DuplexChannel
	term, err := meshcall.Duplex[map[string]any, testTermIn, testTermOut](
		ctx,
		conn.Client(),
		svc.Name(),
		"open_terminal",
		map[string]any{"shell": "zsh"},
	)
	if err != nil {
		t.Fatalf("OpenTerminal failed: %v", err)
	}
	defer term.Close()

	first, err := term.Recv()
	if err != nil || first.Output != "started zsh" {
		t.Fatalf("unexpected first duplex output: %+v, err=%v", first, err)
	}
	if err := term.Send(testTermIn{Data: "pwd"}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	second, err := term.Recv()
	if err != nil || second.Output != "echo: pwd" {
		t.Fatalf("unexpected second duplex output: %+v, err=%v", second, err)
	}
	if err := term.Send(testTermIn{Data: "exit"}); err != nil {
		t.Fatalf("Send exit failed: %v", err)
	}
	if _, err := term.Recv(); err != io.EOF {
		t.Fatalf("expected EOF after exit, got %v", err)
	}
}

type depCycleA struct{}
type depCycleB struct{}

type cycleService struct {
	meshcall.ServiceMeta `name:"cycle" description:"Cycle test."`
}

// Check tests circular dependency detection.
func (s *cycleService) Check(ctx context.Context, a *depCycleA) (string, error) {
	return "ok", nil
}

func TestGoCircularDependencyDetectedAtRegistration(t *testing.T) {
	depA := meshcall.Dependency(func(ctx context.Context, b *depCycleB) (*depCycleA, error) {
		return &depCycleA{}, nil
	})
	depB := meshcall.Dependency(func(ctx context.Context, a *depCycleA) (*depCycleB, error) {
		return &depCycleB{}, nil
	})
	_, err := meshcall.ReflectService(&cycleService{}, meshcall.WithDependencies(depA, depB))
	if err == nil {
		t.Fatalf("expected circular dependency error at registration time")
	}
}
