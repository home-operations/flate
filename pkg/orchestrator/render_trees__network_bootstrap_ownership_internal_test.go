package orchestrator

import (
	"context"
	"encoding/json"
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
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
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

func TestOrchestrator_Render_ResourceSetEffectiveOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		competitor bool
	}{
		{name: "both-producers", competitor: true},
		{name: "sole-producer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testOrchestratorRenderResourceSetEffectiveOwnershipCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestOrchestrator_Render_ResourceSetDocumentBoundary(t *testing.T) {
	t.Parallel()
	source, err := filepath.Abs("../../testdata/resourceset-documents")
	assert.NoError(t, err)
	path := t.TempDir()
	testutil.CopyTree(t, source, path)
	var previous map[manifest.NamedResource][]map[string]any
	testOrchestratorRenderResourceSetDocumentBoundarySegment(t, &path, &previous)
}

func privateRegistry(t *testing.T) (string, string) {
	t.Helper()
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }),
	)
	t.Cleanup(server.Close)
	registry := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, registry, "{}")
	return "oci://" + strings.TrimPrefix(server.URL, "https://"), registry
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
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		assert.NoError(t, err)
		response, err := (&http.Client{Transport: cfg.Network}).Do(req)
		{
			assert.FatalIf(t, err != nil, "%v", err)
		}
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
	runs, err := RenderTrees(
		ctx,
		[]Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: root, RepoRoot: root}},
		cfg,
	)
	{
		assert.FatalIf(
			t,
			(err != nil) != (*testCaseInput.failure) || len(runs) != 1 || runs[0].Orchestrator == nil ||
				runs[0].Result == nil,
			"render result=%+v error=%v",
			runs,
			err,
		)
	}
	select {
	case address := <-bootstrapRequests:
		{
			assert.FatalIf(
				t, (*testCaseInput.injected) &&
					address != bootstrapConnection, "%v",
				"bootstrap did not reuse the supplied network",
			)
		}
	case <-ctx.Done():
		t.Fatal("bootstrap made no request")
	}
	if *testCaseInput.injected {
		{
			assert.FatalIf(
				t,
				get(probeURL) != callerConnection,
				"%v",
				"borrower discarded caller connection",
			)
		}
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
	renderedTrees7, err := RenderTrees(
		ctx,
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
	{
		assert.FatalIf(
			t,
			base.cfg.Network != head.cfg.Network,
			"%v",
			"diff sides did not borrow the outer Network",
		)
	}
	{
		assert.FatalIf(t, opened.Load() == 0, "%v", "renders made no request")
	}
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

func testOrchestratorRenderResourceSetEffectiveOwnershipCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		competitor bool
	},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, root, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system",
		"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [rs.yaml]\n")
	resources := `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: aaa, namespace: flux-system}
spec:
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: foo}
      data: {marker: aaa-omitted}
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: foo, namespace: flux-system}
      data: {marker: aaa-explicit}
`
	if testCaseInput.competitor {
		resources += `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: zzz, namespace: flux-system}
spec:
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: foo, namespace: flux-system}
      data: {marker: zzz-winner}
`
	}
	testutil.WriteFile(t, root, "apps/rs.yaml", resources)
	want, err := testutil.DecodeDocs(strings.NewReader(`apiVersion: v1
kind: ConfigMap
metadata: {name: foo}
data: {marker: aaa-omitted}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: foo, namespace: flux-system}
data: {marker: aaa-explicit}
`))
	assert.NoError(t, err)
	if testCaseInput.competitor {
		want = []map[string]any{
			{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "foo", "namespace": "flux-system"},
				"data":       map[string]any{"marker": "zzz-winner"},
			},
		}
	}
	owner := "aaa"
	if testCaseInput.competitor {
		owner = "zzz"
	}
	for _, doc := range want {
		doc["metadata"].(map[string]any)["labels"] = map[string]any{
			"resourceset.fluxcd.controlplane.io/name":      owner,
			"resourceset.fluxcd.controlplane.io/namespace": "flux-system",
		}
	}
	testOrchestratorRenderResourceSetEffectiveOwnershipCaseTail(
		t,
		&testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs{
			root:          &root,
			testCaseInput: &testCaseInput,
			want:          &want,
		},
	)
}

func testOrchestratorRenderResourceSetEffectiveOwnershipCaseTail(
	t *testing.T,
	tailCaseInput *testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs,
) {
	t.Helper()
	var previous map[manifest.NamedResource][]map[string]any
	testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailSegment(t, &tailCaseInput, &previous)
}

type testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs struct {
	root          *string
	testCaseInput **struct {
		name       string
		competitor bool
	}
	want *[]map[string]any
}

func testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailSegment(t *testing.T, tailCaseInput **testOrchestratorRenderResourceSetEffectiveOwnershipCaseTailInputs, previous *map[manifest.NamedResource][]map[string]any) {
	t.Helper()
	for range 2 {
		renderedSingleton2, err := RenderTrees(
			t.Context(),
			[]Tree{{Path: (*(*tailCaseInput).root), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
			Config{Concurrency: 2},
		)
		{
			assert.FatalIf(t, len(renderedSingleton2) != 1, "missing render: %v", err)
		}
		result := renderedSingleton2[0].Result
		assert.NoError(t, err)
		assert.Equal(t, len(result.Failed), 0)
		var got []map[string]any
		for parent, docs := range outputByParent(result.Manifests) {
			for _, doc := range docs {
				if manifest.DocKind(doc) == manifest.KindConfigMap {
					assert.Equal(t, parent, testutil.KustomizationID("flux-system", "apps"))
					got = append(got, doc)
				}
				if (*(*tailCaseInput).testCaseInput).competitor && manifest.DocKind(doc) == manifest.KindConfigMap {
					encoded, err := json.Marshal(doc)
					assert.NoError(t, err)
					{
						assert.ErrorIf(t, strings.Contains(string(encoded), "aaa-omitted") ||
							strings.Contains(
								string(encoded),
								"aaa-explicit",
							), "losing marker in output: %s", encoded)
					}
				}
			}
		}
		assert.Diff(t, got, (*(*tailCaseInput).want))
		if !(*(*tailCaseInput).testCaseInput).competitor && len(got) == 2 {
			_, present := got[0]["metadata"].(map[string]any)["namespace"]
			assert.Equal(t, present, false)
		}
		if (*previous) != nil {
			assert.Diff(t, outputByParent(result.Manifests), (*previous))
		}
		(*previous) = outputByParent(result.Manifests)
	}
}

func testOrchestratorRenderResourceSetDocumentBoundarySegment(t *testing.T, path *string, previous *map[manifest.NamedResource][]map[string]any) {
	t.Helper()
	for range 2 {
		renderedSingleton3, err := RenderTrees(
			t.Context(),
			[]Tree{{Path: (*path), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
			Config{WipeSecrets: true, Concurrency: 2},
		)
		{
			assert.FatalIf(t, len(renderedSingleton3) != 1, "missing render: %v", err)
		}
		o := renderedSingleton3[0].Orchestrator
		result := renderedSingleton3[0].Result
		if !errors.Is(err, manifest.ErrFlux) || len(result.Failed) != 1 {
			t.Fatalf("required malformed output = %v, failures=%v", err, result.Failed)
		}
		effective := manifest.NamedResource{Group: "apps", Kind: "Deployment", Namespace: "flux-system", Name: "raw"}
		{
			{
				child := o.Store().GetObject(effective)
				assert.FatalIf(t, child == nil, "missing effective child %s", effective)
			}
		}

		counts := map[string]int{}
		for _, docs := range outputByParent(result.Manifests) {
			for _, doc := range docs {
				name, ns := manifest.DocMetadata(doc)
				if name == "raw" || name == "duplicate" {
					assert.Equal(t, ns, "")
				}
				if name == "explicit" {
					assert.Equal(t, ns, "other")
				}
				if name == "sanitized" {
					assert.Diff(t, doc["data"], map[string]any{"encrypted": "..PLACEHOLDER_encrypted.."})
				}
				if name == "kindless-list" {
					assert.Equal(t, manifest.DocKind(doc), "")
					assert.Equal(t, manifest.DocAPIVersion(doc), "")
				}
				counts[name]++
				if data, ok := doc["data"].(map[string]any); ok {
					counts[fmt.Sprint(data["value"])]++
				}
				if spec, ok := doc["spec"].(map[string]any); ok {
					counts[fmt.Sprint(spec["value"])]++
				}
				if manifest.DocKind(doc) == "List" {
					counts["wrapper"]++
				}
				if manifest.DocAPIVersion(doc) == "kustomize.config.k8s.io/v1beta1" {
					counts["directive"]++
				}
			}
		}
		for key, want := range map[string]int{"raw": 1, "duplicate": 2, "nameless-json": 1, "nameless-list": 1, "kindless-list": 1, "wrapper": 2, "directive": 2, "explicit": 1, "sanitized": 1, "nested": 0, "item-directive": 1, "child-result": 1, "rejected-yaml": 0, "rejected-directive": 0, "child": 0} {
			if counts[key] != want {
				t.Errorf("%s count=%d, want %d; all=%v", key, counts[key], want, counts)
			}
		}
		if (*previous) != nil {
			assert.Diff(t, outputByParent(result.Manifests), (*previous))
		}
		(*previous) = outputByParent(result.Manifests)
	}
}
