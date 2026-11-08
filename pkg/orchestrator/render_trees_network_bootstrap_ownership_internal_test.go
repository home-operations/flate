package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_NetworkBootstrapOwnership(t *testing.T) {
	t.Parallel()

	for _, injected := range []bool{false, true} {
		for _, failure := range []bool{false, true} {
			t.Run(fmt.Sprintf("injected=%t/bootstrap_failure=%t", injected, failure), func(t *testing.T) {
				t.Parallel()
				testRenderTreesNetworkBootstrapOwnershipCase(
					t,
					testRenderTreesNetworkBootstrapOwnershipCaseInputs{injected: &injected, failure: &failure},
				)
			},

			// A failed bootstrap response can still be draining on its origin.

			)
		}
	}
}

func TestRenderTrees_NetworkOwnership(t *testing.T) {
	t.Parallel()

	for _, injected := range []bool{false, true} {
		t.Run(fmt.Sprintf("injected=%t", injected), func(t *testing.T) {
			t.Parallel()
			testRenderTreesNetworkOwnershipCase(t, &injected)
		},
		)
	}
}

type registryFixture struct {
	url         string
	credentials string
}

func privateRegistry(t *testing.T) registryFixture {
	t.Helper()
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }),
	)
	t.Cleanup(server.Close)
	registry := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, registry, "{}")
	return registryFixture{url: "oci://" + strings.TrimPrefix(server.URL, "https://"), credentials: registry}
}

type testRenderTreesNetworkBootstrapOwnershipCaseInputs struct {
	injected *bool
	failure  *bool
}

func testRenderTreesNetworkBootstrapOwnershipCase(
	t *testing.T,
	testCaseInput testRenderTreesNetworkBootstrapOwnershipCaseInputs,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type connection struct {
		idle   chan struct{}
		closed chan struct{}
	}
	var mu sync.Mutex
	connections := make(map[string]*connection)
	track := func(c net.Conn, state http.ConnState) {
		mu.Lock()
		defer mu.Unlock()
		address := c.LocalAddr().String() + "/" + c.RemoteAddr().String()
		switch state {
		case http.StateActive, http.StateHijacked:
		case http.StateNew:
			connections[address] = &connection{idle: make(chan struct{}), closed: make(chan struct{})}
		case http.StateIdle:
			select {
			case <-connections[address].idle:
			default:
				close(connections[address].idle)
			}
		case http.StateClosed:
			close(connections[address].closed)
		}
	}
	bootstrapRequests := make(chan string, 2)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Connection", r.Host+"/"+r.RemoteAddr)
		if r.URL.Path == "/bad.env" {
			http.NotFound(w, r)
			return
		}
		bootstrapRequests <- r.Host + "/" + r.RemoteAddr
		_, _ = io.WriteString(w, "VALUE=ok\n")
	}))
	srv.Config.ConnState = track
	srv.Start()
	t.Cleanup(srv.Close)
	cfg := Config{Concurrency: 2}
	var probeURL, callerConnection, bootstrapConnection string
	get := func(url string) string {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
		assert.NoError(t, err)
		response, err := (&http.Client{Transport: cfg.Network}).Do(req)
		assert.FatalIf(t, err != nil, "%v", err)
		_, err = io.Copy(io.Discard, response.Body)
		assert.NoError(t, errors.Join(err, response.Body.Close()))
		address := response.Header.Get("X-Connection")
		mu.Lock()
		c := connections[address]
		mu.Unlock()
		select {
		case <-c.idle:
		case <-ctx.Done():
			t.Fatal("caller connection did not become idle")
		}
		return address
	}
	if *testCaseInput.injected {
		cfg.Network = new(source.Network)
		t.Cleanup(cfg.Network.Close)

		probe := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Connection", r.Host+"/"+r.RemoteAddr)
			_, _ = io.WriteString(w, "probe\n")
		}))
		probe.Config.ConnState = track
		probe.Start()
		t.Cleanup(probe.Close)
		probeURL = probe.URL
		callerConnection = get(probeURL)
		bootstrapConnection = get(srv.URL + "/vars.env")
		<-bootstrapRequests
	}
	root := t.TempDir()
	envs := srv.URL + "/vars.env"
	if *testCaseInput.failure {
		envs += ", " + srv.URL + "/bad.env"
	}
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		"configMapGenerator:\n- name: generated\n  envs: ["+envs+"]\n",
	)
	runs, err := renderTreesForTest(ctx, t,
		[]Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: root, RepoRoot: root}},
		cfg,
	)
	assert.FatalIf(
		t,
		(err != nil) != (*testCaseInput.failure) || len(runs) != 1 || runs[0].Orchestrator == nil ||
			runs[0].Result == nil,
		"render result=%+v error=%v",
		runs,
		err,
	)
	select {
	case address := <-bootstrapRequests:
		assert.FatalIf(
			t, (*testCaseInput.injected) &&
				address != bootstrapConnection, "%v",
			"bootstrap did not reuse the supplied network",
		)
	case <-ctx.Done():
		t.Fatal("bootstrap made no request")
	}
	if *testCaseInput.injected {
		assert.FatalIf(
			t,
			get(probeURL) != callerConnection,
			"%v",
			"borrower discarded caller connection",
		)
		cfg.Network.Close()
	}
	mu.Lock()
	observed := maps.Clone(connections)
	mu.Unlock()
	for _, address := range slices.Sorted(maps.Keys(observed)) {
		select {
		case <-observed[address].closed:
		case <-ctx.Done():
			t.Fatalf("owner retained connection %s", address)
		}
	}
}

func testRenderTreesNetworkOwnershipCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	var opened atomic.Int32
	closed := make(chan struct{}, 2)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n")
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	cfg := treeConfig{Concurrency: 2, Tree: Tree{}}
	if *testCaseInput {
		cfg.Network = new(source.Network)
		t.Cleanup(cfg.Network.Close)
	}
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux.yaml", substitutionKS("consumer", "app", ""))
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: ["+srv.URL+"/cm.yaml]\n")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	renderedTrees7, err := renderTreesForTest(ctx, t,
		[]Tree{
			{
				RepoRoot:        root,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				BaseInput:       cfg.Input,
				ExternalChanges: cfg.ExternalChanges,
				Input:           cfg.BaseInput,
			},
			{
				RepoRoot:        root,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           cfg.Input,
				BaseInput:       cfg.BaseInput,
				ExternalChanges: cfg.ExternalChanges,
			},
		},
		cfg.Config,
	)
	base, head := renderedTrees7[0], renderedTrees7[1]
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		base.cfg.Network != head.cfg.Network,
		"%v",
		"diff sides did not borrow the outer Network",
	)
	assert.FatalIf(t, opened.Load() == 0, "%v", "renders made no request")
	if *testCaseInput {
		select {
		case <-closed:
			t.Fatal("diff side closed caller's Network")
		default:
		}
		cfg.Network.Close()
	}
	for range opened.Load() {
		select {
		case <-closed:
		case <-ctx.Done():
			t.Fatal("outer owner retained idle transport")
		}
	}
}
