package kustomize

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_RemoteCacheDependencies(t *testing.T) {
	t.Parallel()
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted sentinel assertions executed=%v", eligible)
	for _, immutable := range []bool{false, true} {
		t.Run(fmt.Sprintf("immutable_%t", immutable), func(t *testing.T) {
			t.Parallel()
			testRenderRemoteCacheDependenciesCase(
				t,
				&testRenderRemoteCacheDependenciesCaseInputs{eligible: &eligible, immutable: &immutable},
			)
		},
		)
	}
}

type testRenderRemoteCacheDependenciesCaseInputs struct {
	eligible  *bool
	immutable *bool
}

func testRenderRemoteCacheDependenciesCase(t *testing.T, testCaseInput *testRenderRemoteCacheDependenciesCaseInputs) {
	t.Helper()
	for _, origin := range []string{"http", "patch", "generator", "plugin", "plugin-directory", "git", "nested-mount"} {
		t.Run(origin, func(t *testing.T) { testRenderRemoteCacheDependenciesCaseOrigin(t, &origin, &testCaseInput) })
	}
}

func testRenderRemoteCacheDependenciesCaseOrigin(
	t *testing.T,
	origin *string,
	testCaseInput **testRenderRemoteCacheDependenciesCaseInputs,
) {
	t.Helper()

	var version atomic.Int64
	var trace remoteHTTPTrace
	const gitURL = "file:///example.invalid/o/r.git"
	newRemoteDependencyFixtureResult1 := newRemoteDependencyFixture(t, &remoteDependencyFixtureInputs{
		origin: &origin, version: &version, trace: &trace, gitURL: gitURL,
	})
	root, remoteFiles := newRemoteDependencyFixtureResult1.root, newRemoteDependencyFixtureResult1.files
	cacheDir := t.TempDir()

	newCache := func(storage string) *TreeCache {
		c := newTestTreeCache(t, storage)
		v := version.Load()
		remoteFiles["remote/cm.yaml"] = cmYAML(fmt.Sprintf("remote-v%d", v))
		remote := memoryTree(t, remoteFiles)
		c.SetGitBaseFetcher(
			func(_ context.Context, req GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				assert.ErrorIf(t, req.URL != gitURL, "unexpected git URL %q", req.URL)
				return &store.SourceArtifact{
					FS:       remote,
					Key:      "family",
					Content:  strconv.FormatInt(v, 10),
					Revision: strconv.FormatInt(v, 10),
				}, nil
			},
		)

		return c
	}
	request := func() RenderRequest {
		source := localSource(t, root)
		if *(*testCaseInput).immutable {
			source.Key, source.Content, source.Revision = "local-family", "local-content", "local-pin"
		}
		return RenderRequest{Source: source, Label: root, SubPath: "."}
	}
	version.Store(1)
	initial := len(trace.snapshot())
	cache := newCache(cacheDir)
	out1, receipt16, err := testutil.RenderMaps(
		Render(t.Context(), cache, request(), sourcecache.RetryConfig{}),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt16},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		err != nil || !hasRenderField(out1, "", "remote-v1"),
		"initial render: %s, %v",
		out1,
		err,
	)
	inputRec := newReadSet(root)
	inputRec.immutable = (*(*testCaseInput).immutable)
	view := newTreeFS(request().Source.FS, root, inputRec)
	{
		preflightRemoteResourcesErr := preflightRemoteResources(t.Context(), cache, view, root)
		assert.NoError(t, preflightRemoteResourcesErr)
	}
	recorded := inputRec.snapshot()
	assert.FatalIf(t, len(recorded.Remotes) == 0 || (*(*testCaseInput).immutable) &&
		(len(recorded.Files) != 0 || len(recorded.Dirs) != 0 || len(recorded.Nodes) != 0), "incomplete remote recorder: %+v", recorded)
	{
		valid, stillValidErr := recorded.stillValid(t.Context(), cache, view, (*(*testCaseInput).immutable))
		assert.FatalIf(
			t,
			stillValidErr != nil || !valid,
			"unchanged remote identity: %t, %v",
			valid,
			stillValidErr,
		)
	}
	if (*origin) == "generator" && len(trace.snapshot())-initial != 1 {
		t.Fatalf(
			"bootstrap, render and validation did not share acquisition: %+v",
			trace.snapshot()[initial:],
		)
	}
	renderRequestSnapshot := request()
	key := renderKey(&renderRequestSnapshot, root, nil)
	snap, stored, hit := readTestRenderFrame(t, cache, key)
	assert.FatalIf(
		t,
		!hit || !reflect.DeepEqual(stored, out1) || len(snap.Remotes) == 0 || (*(*testCaseInput).immutable) &&
			(len(snap.Files) != 0 || len(snap.Dirs) != 0 || len(snap.Nodes) != 0),
		"published=%t eligible=%t snapshot=%+v output=%s",
		hit,
		(*(*testCaseInput).eligible),
		snap,
		stored,
	)
	var before int
	checkRemoteDependencyReplay(t.Context(), t, &remoteDependencyReplayInputs{
		origin: &origin, scenario: &testCaseInput, trace: &trace,
		newCache: &newCache, request: &request, firstOutput: &out1,
		stored: &stored, hit: &hit, snapshot: &snap, cache: &cache,
		cacheDir: &cacheDir, key: &key, recorded: &recorded,
		before: &before, err: &err,
	})
	version.Store(2)
	before = len(trace.snapshot())
	cache = newCache(cacheDir)
	{
		valid, stillValidErr3 := recorded.stillValid(
			t.Context(),
			cache,
			&treeFS{lower: request().Source.FS}, (*(*testCaseInput).immutable),
		)
		assert.FatalIf(t, stillValidErr3 != nil ||
			valid, "changed remote identity: %t, %v", valid, stillValidErr3)
	}
	out3, receipt19, err := testutil.RenderMaps(
		Render(t.Context(), cache, request(), sourcecache.RetryConfig{}),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt19},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		err != nil || !hasRenderField(out3, "", "remote-v2") || reflect.DeepEqual(out3, out1),
		"changed remote render: %s, %v",
		out3,
		err,
	)
	if (*origin) != "git" && (*origin) != "nested-mount" {
		got := trace.snapshot()[before:]
		t.Logf(
			"changed render and preflight: requests=%+v accepted_connections=%d",
			got,
			trace.connections.Load(),
		)
		assert.ErrorIf(
			t,
			len(got) != 1 || got[0].Status != http.StatusOK || got[0].Bytes == 0,
			"changed render must acquire one response including subsequent preflight: %+v",
			got,
		)
	}
	closeTestTreeCache(t, cache)
}

func testRenderRemoteCacheDependenciesCaseOriginLoop(t *testing.T, loopInputs struct {
	origin        **string
	trace         *remoteHTTPTrace
	before        *int
	testCaseInput ***testRenderRemoteCacheDependenciesCaseInputs
	out2          *[]map[string]any
},
) {
	t.Helper()

	if (*(*loopInputs.origin)) != "git" && (*(*loopInputs.origin)) != "nested-mount" {
		got := loopInputs.trace.snapshot()[(*loopInputs.before):]
		if (*(*loopInputs.origin)) == "generator" {
			t.Logf(
				"bootstrap-primed persistence eligible=%t sentinel_reused=%t requests=%d",
				(*(*(*loopInputs.testCaseInput)).eligible),
				hasRenderField((*loopInputs.out2), "witness", "SENTINEL"),
				len(got),
			)
		}
		t.Logf("unchanged render: requests=%+v accepted_connections=%d", got, loopInputs.trace.connections.Load())
		assert.ErrorIf(t, len(got) != 1 || got[0].Method != http.MethodGet || got[0].ETag == "" ||
			got[0].Modified == "" ||
			got[0].Status != http.StatusNotModified ||
			got[0].Bytes != 0, "unchanged render must make one conditional GET with zero body bytes: %+v", got)
	}
}

type remoteDependencyReplayInputs struct {
	origin      **string
	scenario    ***testRenderRemoteCacheDependenciesCaseInputs
	trace       *remoteHTTPTrace
	newCache    *func(string) *TreeCache
	request     *func() RenderRequest
	firstOutput *[]map[string]any
	stored      *[]map[string]any
	hit         *bool
	snapshot    *readSetSnapshot
	cache       **TreeCache
	cacheDir    *string
	key         *string
	recorded    *readSetSnapshot
	before      *int
	err         *error
}

func checkRemoteDependencyReplay(ctx context.Context, t *testing.T, inputs *remoteDependencyReplayInputs) {
	t.Helper()

	fresh := (*inputs.newCache)("")
	var want []map[string]any
	var receipt17 *manifest.RenderReceipt
	want, receipt17, *inputs.err = testutil.RenderMaps(
		Render(ctx, fresh, (*inputs.request)(), sourcecache.RetryConfig{}),
	)
	if *inputs.err == nil {
		{
			recordErr := fresh.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt17},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		*inputs.err != nil || !reflect.DeepEqual(want, *inputs.firstOutput) ||
			*inputs.hit && !reflect.DeepEqual(want, *inputs.stored),
		"fresh/cached bytes differ: %s, %s, %v",
		want,
		*inputs.stored,
		*inputs.err,
	)
	closeTestTreeCache(t, fresh)
	if *(*(*inputs.scenario)).eligible {
		publishTestFrame(
			t,
			(*inputs.cache).owner,
			sourcecache.BlobKey{Content: *inputs.key, Render: sourcecache.RenderPending},
			testRenderFrame(t, new(*inputs.snapshot), testRenderPayload(t, "SENTINEL")),
		)
	}
	closeTestTreeCache(t, (*inputs.cache))
	*inputs.before = len(inputs.trace.snapshot())
	(*inputs.cache) = (*inputs.newCache)(*inputs.cacheDir)
	{
		valid, stillValidErr2 := inputs.recorded.stillValid(
			ctx,
			(*inputs.cache),
			&treeFS{lower: (*inputs.request)().Source.FS}, (*(*(*inputs.scenario)).immutable),
		)
		assert.FatalIf(t, stillValidErr2 != nil ||
			!valid, "reopened unchanged remote identity: %t, %v", valid, stillValidErr2)
	}
	var out2 []map[string]any
	var receipt18 *manifest.RenderReceipt
	out2, receipt18, *inputs.err = testutil.RenderMaps(
		Render(ctx, (*inputs.cache), (*inputs.request)(), sourcecache.RetryConfig{}),
	)
	if *inputs.err == nil {
		{
			recordErr := (*inputs.cache).RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt18},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		*inputs.err != nil || (*(*(*inputs.scenario)).eligible) && !hasRenderField(out2, "witness", "SENTINEL") ||
			!(*(*(*inputs.scenario)).eligible) &&
				!reflect.DeepEqual(out2, *inputs.firstOutput),
		"unchanged fresh-owner render: %s, %v",
		out2,
		*inputs.err,
	)
	testRenderRemoteCacheDependenciesCaseOriginLoop(t, struct {
		origin        **string
		trace         *remoteHTTPTrace
		before        *int
		testCaseInput ***testRenderRemoteCacheDependenciesCaseInputs
		out2          *[]map[string]any
	}{origin: inputs.origin, trace: inputs.trace, before: inputs.before, testCaseInput: inputs.scenario, out2: &out2})
	closeTestTreeCache(t, (*inputs.cache))
}

type remoteDependencyFixtureInputs struct {
	origin  **string
	version *atomic.Int64
	trace   *remoteHTTPTrace
	gitURL  string
}

type remoteDependencyFixture struct {
	root  string
	files map[string]string
}

func newRemoteDependencyFixture(t *testing.T, inputs *remoteDependencyFixtureInputs) remoteDependencyFixture {
	t.Helper()

	remoteBody := func() string {
		name := fmt.Sprintf("remote-v%d", inputs.version.Load())
		switch *(*inputs.origin) {
		case "patch":
			return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: local}\ndata: {marker: " + name + "}\n"
		case "generator", "plugin-directory":
			return name
		case "plugin":
			return "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: " + name + "}\nliterals: [marker=" + name + "]\n"
		default:
			return cmYAML(name)
		}
	}

	srv := inputs.trace.serve(t, remoteBody)

	ref := srv.URL + "/remote.yaml"
	remoteFiles := map[string]string{}
	if (*(*inputs.origin)) == "git" || (*(*inputs.origin)) == "nested-mount" {
		ref = inputs.gitURL + "//remote?ref=main"
		remoteFiles["remote/kustomization.yaml"] = "resources: [cm.yaml]\n"
	}
	kustomization := "resources: [local.yaml, " + fmt.Sprintf("%q", ref) + "]\n"
	if (*(*inputs.origin)) == "nested-mount" {
		remoteFiles["outer/nested/kustomization.yaml"] = kustomization
		remoteFiles["outer/nested/local.yaml"] = cmYAML("local")
		remoteFiles["outer/kustomization.yaml"] = "resources: [nested]\n"
		kustomization = fmt.Sprintf("resources: [%q]\n", inputs.gitURL+"//outer?ref=main")
	}
	files := map[string]string{"local.yaml": cmYAML("local")}
	switch *(*inputs.origin) {
	case "patch":
		kustomization = "resources: [local.yaml]\npatches: [{path: " + ref + "}]\n"
	case "generator":
		kustomization = "configMapGenerator: [{name: generated, files: [chosen=" + ref + "]}]\n"
	case "plugin":
		kustomization = "generators: [" + ref + "]\n"
	case "plugin-directory":
		kustomization = "generators: [plugins]\n"
		files["plugins/kustomization.yaml"] = "resources: [nested]\n"
		files["plugins/nested/kustomization.yaml"] = "resources: [../g.yaml]\n"
		files["plugins/g.yaml"] = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [" + ref + "]\n"
	}
	files["kustomization.yaml"] = kustomization
	root := testutil.WriteTree(t, files)

	return remoteDependencyFixture{root: root, files: remoteFiles}
}
