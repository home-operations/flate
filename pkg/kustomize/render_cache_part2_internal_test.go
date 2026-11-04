package kustomize

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

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

func closeTestTreeCache(t *testing.T, c *TreeCache) {
	t.Helper()
	c.Close()
	c.remoteInputs.Close()
	{
		err := c.owner.Close()
		assert.NoError(t, err)
	}
}

func TestRenderKey_ContentInputs(t *testing.T) {
	t.Parallel()
	req := RenderRequest{
		Source:  &store.SourceArtifact{Key: "family", Content: "generation"},
		SubPath: ".",
		Spec:    demoSpec,
	}
	key := renderKey(&req, "/source", nil)
	for _, tc := range []struct {
		name   string
		change func(*RenderRequest)
		root   string
		ignore any
		spec   map[string]any
		typed  map[string]any
		same   bool
	}{
		{name: "substitution", same: true, change: func(r *RenderRequest) {
			r.Spec.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "typed"}}
		}},
		{name: "substitution strategy", same: true, change: func(r *RenderRequest) {
			r.Spec.PostBuild = &kustomizev1.PostBuild{SubstituteStrategy: kustomizev1.SubstituteStrategyAlways}
		}},
		{name: "substitution references", same: true, change: func(r *RenderRequest) {
			r.Spec.PostBuild = &kustomizev1.PostBuild{
				SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "provider"}},
			}
		}},
		{name: "spec", change: func(r *RenderRequest) { r.Spec.NamePrefix = "other-" }},
		{name: "owner name", change: func(r *RenderRequest) { r.Spec.Name = "other" }},
		{name: "owner namespace", change: func(r *RenderRequest) { r.Spec.Namespace = "other" }},
		{name: "explicit empty namespace", same: true, spec: map[string]any{"targetNamespace": ""}},
		{name: "explicit empty prefix", same: true, spec: map[string]any{"namePrefix": ""}},
		{name: "explicit empty suffix", same: true, spec: map[string]any{"nameSuffix": ""}},
		{name: "null fields", same: true, spec: map[string]any{"components": nil}},
		{name: "null whole fields", same: true, spec: map[string]any{"patches": nil, "images": nil, "components": nil, "buildMetadata": nil, "commonMetadata": nil, "targetNamespace": nil, "namePrefix": nil, "nameSuffix": nil}},
		{name: "empty lists", same: true, spec: map[string]any{"patches": []any{}, "images": []any{}, "components": []any{}, "buildMetadata": []any{}}},
		{name: "null image elements", spec: map[string]any{"images": []any{nil}}, typed: map[string]any{"images": []any{map[string]any{"name": ""}}}},
		{name: "null component elements", spec: map[string]any{"components": []any{nil}}, typed: map[string]any{"components": []any{""}}},
		{name: "null build metadata elements", spec: map[string]any{"buildMetadata": []any{nil}}, typed: map[string]any{"buildMetadata": []any{""}}},
		{name: "null patch elements", spec: map[string]any{"patches": []any{nil}}, typed: map[string]any{"patches": []any{map[string]any{"patch": ""}}}},
		{name: "null common labels", spec: map[string]any{"commonMetadata": map[string]any{"labels": map[string]any{"app": nil}}}, typed: map[string]any{"commonMetadata": map[string]any{"labels": map[string]any{"app": ""}}}},
		{name: "null common annotations", spec: map[string]any{"commonMetadata": map[string]any{"annotations": map[string]any{"note": nil}}}, typed: map[string]any{"commonMetadata": map[string]any{"annotations": map[string]any{"note": ""}}}},
		{name: "strategic merge patches", same: true, spec: map[string]any{"patchesStrategicMerge": []any{map[string]any{"metadata": map[string]any{"name": "app"}}}}},
		{name: "JSON 6902 patches", same: true, spec: map[string]any{"patchesJson6902": []any{map[string]any{"patch": []any{map[string]any{"op": "remove", "path": "/data/old"}}}}}},
		{name: "root", root: "/other"},
		{name: "subpath", change: func(r *RenderRequest) { r.SubPath = "app" }},
		{name: "ignore selection", change: func(r *RenderRequest) { r.ApplyIgnore = true }},
		{name: "ignore bytes", ignore: []byte("*.yaml")},
		{name: "acquisition", change: func(r *RenderRequest) { r.Source.Key = "other-family" }},
		{name: "revision", same: true, change: func(r *RenderRequest) { r.Source.Revision = "revision" }},
		{name: "generation", change: func(r *RenderRequest) { r.Source.Content = "next" }},
		{name: "origin", same: true, change: func(r *RenderRequest) {
			r.Source.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
		}},
		{name: "digest", change: func(r *RenderRequest) { r.Source.Digest = "digest" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderKeyContentInputsCase(t, &testRenderKeyContentInputsCaseInputs{req: &req, key: &key, tc: &tc})
		},
		)
	}
	for _, field := range []string{"targetNamespace", "namePrefix", "nameSuffix"} {
		t.Run("typed override/"+field, func(t *testing.T) {
			t.Parallel()
			testRenderKeyContentInputsCase2(t, &testRenderKeyContentInputsCase2Inputs{req: &req, field: &field})
		},
		)
	}

	stamped := req
	stamped.Spec = req.Spec.Clone()
	stamped.Spec.Labels = map[string]string{"owner": "parent"}
	stamped.Spec.Annotations = map[string]string{"stamp": "changed"}
	{
		got := renderKey(&stamped, "/source", nil)
		assert.FatalIf(t, got != key, "%s", "emission metadata changed the render key")
	}
	{
		got := renderKey(&req, "/source", nil)
		assert.FatalIf(t, got != key, "%s", "unchanged render input changed the key")
	}
}

func TestRender_CacheIdentityIsolated(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("fresh"),
	})
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	key := renderKey(&RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec, SubPath: "."}, root, nil)
	publishTestFrame(
		t,
		cache.owner,
		sourcecache.BlobKey{Content: struct {
			Render string `json:"Render"`
		}{key}},
		testRenderFrame(t, new(readSetSnapshot{}), []byte("ORDINARY\n")),
	)
	out, receipt20, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := cache.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt20},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.FatalIf(
			t,
			err != nil || !hasRenderField(out, "name", "fresh"),
			"ordinary raw identity reused cached output: %s, %v",
			out,
			err,
		)
	}
	_, stored, hit := readTestRenderFrame(t, cache, key)
	{
		assert.FatalIf(t, !hit || !reflect.DeepEqual(stored, out), "completed render = %s, %v", stored, hit)
	}
	bare := req
	bare.Spec = nil
	{
		assert.FatalIf(
			t,
			renderKey(&bare, root, nil) == renderKey(&req, root, nil),
			"%s",
			"bare and owned cache identities collide",
		)
	}
	first, _, err := testutil.RenderMaps(Render(t.Context(), cache, bare, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(t, err != nil || hasRenderField(first, manifest.FluxKustomizeDomain+"/name", "demo") ||
			!hasRenderField(first, "name", "fresh"), "bare render acquired owner metadata: %s, %v", first, err)
	}
	warm, _, err := testutil.RenderMaps(Render(t.Context(), cache, bare, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(t, err != nil || !reflect.DeepEqual(first, warm), "bare replay changed: %s, %v", warm, err)
	}
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("changed"))
	updated, _, err := testutil.RenderMaps(Render(t.Context(), cache, bare, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(
			t,
			err != nil || !hasRenderField(updated, "name", "changed"),
			"bare cache missed input invalidation: %s, %v",
			updated,
			err,
		)
	}
	req.Spec = req.Spec.Clone()
	req.Spec.NamePrefix = "owned-"
	owned, _, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(t, err != nil || hasRenderField(owned, manifest.FluxKustomizeDomain+"/name", "demo") ||
			!hasRenderField(owned, "name", "owned-changed"), "owned render reused bare output: %s, %v", owned, err)
	}
}

func TestRender_CacheFrames(t *testing.T) {
	t.Parallel()
	eligible := renderPersistenceEligible(t)
	t.Logf("render persistence eligible=%v", eligible)
	for _, tc := range []struct {
		name      string
		snapshot  readSetSnapshot
		malformed bool
		corrupt   bool
		invalid   bool
		empty     bool
		immutable bool
	}{
		{name: "malformed", malformed: true},
		{name: "validated corrupt payload", corrupt: true},
		{name: "corrupt payload invalid source", corrupt: true, invalid: true},
		{name: "mismatched", snapshot: readSetSnapshot{Files: []localEntry{{Path: "cm.yaml", Hash: "mismatch"}}}},
		{name: "empty", empty: true},
		{name: "immutable", immutable: true, snapshot: readSetSnapshot{Files: []localEntry{{Path: "unavailable", Hash: "mismatch"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderCacheFramesCase(t, &testRenderCacheFramesCaseInputs{eligible: &eligible, tc: &tc})
		},
		)
	}
}

type pausedRenderFS struct {
	fs.FS

	entered chan struct{}
	release chan struct{}
	once    sync.Once
	builds  *atomic.Int64
}

func (p *pausedRenderFS) ReadFile(name string) ([]byte, error) {
	if name != "cm.yaml" {
		return fs.ReadFile(p.FS, name)
	}
	if p.entered != nil {
		p.once.Do(func() { close(p.entered); <-p.release })
	}
	var pcs [64]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if frame.Function == "github.com/home-operations/flate/internal/kustomize.Build" {
			if p.builds != nil {
				p.builds.Add(1)
			}
			break
		}
		if !more {
			break
		}
	}

	return fs.ReadFile(p.FS, name)
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

type testRenderKeyContentInputsCaseInputs struct {
	req *RenderRequest
	key *string
	tc  *struct {
		name   string
		change func(*RenderRequest)
		root   string
		ignore any
		spec   map[string]any
		typed  map[string]any
		same   bool
	}
}

func testRenderKeyContentInputsCase(t *testing.T, testCaseInput *testRenderKeyContentInputsCaseInputs) {
	t.Helper()

	changed := (*testCaseInput.req)
	changed.Spec = testCaseInput.req.Spec.Clone()
	source := *testCaseInput.req.Source
	changed.Source = &source
	if testCaseInput.tc.spec != nil {
		doc := renderDoc(t, changed.Spec)
		maps.Copy(doc["spec"].(map[string]any), testCaseInput.tc.spec)
		changed.Spec = parsedKS(t, doc)
	}
	if testCaseInput.tc.change != nil {
		testCaseInput.tc.change(&changed)
	}
	root := "/source"
	if testCaseInput.tc.root != "" {
		root = testCaseInput.tc.root
	}
	got := renderKey(&changed, root, testCaseInput.tc.ignore)
	if got == "" || (got == (*testCaseInput.key)) != testCaseInput.tc.same {
		t.Fatalf("render key equality = %t, want %t", got == (*testCaseInput.key), testCaseInput.tc.same)
	}
	if testCaseInput.tc.typed != nil {
		typed := (*testCaseInput.req)
		doc := renderDoc(t, testCaseInput.req.Spec)
		maps.Copy(doc["spec"].(map[string]any), testCaseInput.tc.typed)
		typed.Spec = parsedKS(t, doc)
		{
			assert.FatalIf(
				t,
				got != renderKey(&typed, root, testCaseInput.tc.ignore),
				"%s",
				"null input differs from its typed zero value",
			)
		}
	}
}

type testRenderKeyContentInputsCase2Inputs struct {
	req   *RenderRequest
	field *string
}

func testRenderKeyContentInputsCase2(t *testing.T, testCaseInput *testRenderKeyContentInputsCase2Inputs) {
	t.Helper()

	changed := (*testCaseInput.req)
	changed.Spec = testCaseInput.req.Spec.Clone()
	doc := renderDoc(t, changed.Spec)
	doc["spec"].(map[string]any)[(*testCaseInput.field)] = nil
	changed.Spec = parsedKS(t, doc)
	plain := (*testCaseInput.req)
	plain.Spec = testCaseInput.req.Spec.Clone()
	switch *testCaseInput.field {
	case "targetNamespace":
		changed.Spec.TargetNamespace, plain.Spec.TargetNamespace = "apps", "apps"
	case "namePrefix":
		changed.Spec.NamePrefix, plain.Spec.NamePrefix = "pre-", "pre-"
	case "nameSuffix":
		changed.Spec.NameSuffix, plain.Spec.NameSuffix = "-post", "-post"
	}
	{
		assert.FatalIf(
			t,
			renderKey(&changed, "/source", nil) != renderKey(&plain, "/source", nil),
			"%s",
			"authored null changed typed content",
		)
	}
}

type testRenderCacheFramesCaseInputs struct {
	eligible *bool
	tc       *struct {
		name      string
		snapshot  readSetSnapshot
		malformed bool
		corrupt   bool
		invalid   bool
		empty     bool
		immutable bool
	}
}

func testRenderRemoteCacheDependenciesCaseOrigin(
	t *testing.T,
	origin *string,
	testCaseInput **testRenderRemoteCacheDependenciesCaseInputs,
) {
	t.Helper()

	var version atomic.Int64
	remoteBody := func() string {
		name := fmt.Sprintf("remote-v%d", version.Load())
		switch *origin {
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
	var trace remoteHTTPTrace
	srv := trace.serve(t, remoteBody)
	const gitURL = "file:///example.invalid/o/r.git"
	ref := srv.URL + "/remote.yaml"
	remoteFiles := map[string]string{}
	if (*origin) == "git" || (*origin) == "nested-mount" {
		ref = gitURL + "//remote?ref=main"
		remoteFiles["remote/kustomization.yaml"] = "resources: [cm.yaml]\n"
	}
	kustomization := "resources: [local.yaml, " + fmt.Sprintf("%q", ref) + "]\n"
	if (*origin) == "nested-mount" {
		remoteFiles["outer/nested/kustomization.yaml"] = kustomization
		remoteFiles["outer/nested/local.yaml"] = cmYAML("local")
		remoteFiles["outer/kustomization.yaml"] = "resources: [nested]\n"
		kustomization = fmt.Sprintf("resources: [%q]\n", gitURL+"//outer?ref=main")
	}
	files := map[string]string{"local.yaml": cmYAML("local")}
	switch *origin {
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

	{
		assert.FatalIf(
			t,
			err != nil || !hasRenderField(out1, "", "remote-v1"),
			"initial render: %s, %v",
			out1,
			err,
		)
	}
	inputRec := newReadSet(root)
	inputRec.immutable = (*(*testCaseInput).immutable)
	view := newTreeFS(request().Source.FS, root, inputRec)
	{
		preflightRemoteResourcesErr := preflightRemoteResources(t.Context(), cache, view, root)
		assert.NoError(t, preflightRemoteResourcesErr)
	}
	recorded := inputRec.snapshot()
	{
		assert.FatalIf(t, len(recorded.Remotes) == 0 || (*(*testCaseInput).immutable) &&
			(len(recorded.Files) != 0 || len(recorded.Dirs) != 0 || len(recorded.Nodes) != 0), "incomplete remote recorder: %+v", recorded)
	}
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
	{
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
	}
	fresh := newCache("")
	want, receipt17, err := testutil.RenderMaps(
		Render(t.Context(), fresh, request(), sourcecache.RetryConfig{}),
	)
	if err == nil {
		{
			recordErr := fresh.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt17},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.FatalIf(
			t,
			err != nil || !reflect.DeepEqual(want, out1) || hit && !reflect.DeepEqual(want, stored),
			"fresh/cached bytes differ: %s, %s, %v",
			want,
			stored,
			err,
		)
	}
	closeTestTreeCache(t, fresh)
	if *(*testCaseInput).eligible {
		publishTestFrame(
			t,
			cache.owner,
			sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
			testRenderFrame(t, new(snap), testRenderPayload(t, "SENTINEL")),
		)
	}
	closeTestTreeCache(t, cache)
	before := len(trace.snapshot())
	cache = newCache(cacheDir)
	{
		valid, stillValidErr2 := recorded.stillValid(
			t.Context(),
			cache,
			&treeFS{lower: request().Source.FS}, (*(*testCaseInput).immutable),
		)
		assert.FatalIf(t, stillValidErr2 != nil ||
			!valid, "reopened unchanged remote identity: %t, %v", valid, stillValidErr2)
	}
	out2, receipt18, err := testutil.RenderMaps(
		Render(t.Context(), cache, request(), sourcecache.RetryConfig{}),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt18},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.FatalIf(
			t,
			err != nil || (*(*testCaseInput).eligible) && !hasRenderField(out2, "witness", "SENTINEL") ||
				!(*(*testCaseInput).eligible) &&
					!reflect.DeepEqual(out2, out1),
			"unchanged fresh-owner render: %s, %v",
			out2,
			err,
		)
	}
	testRenderRemoteCacheDependenciesCaseOriginLoop(t, struct {
		origin        **string
		trace         *remoteHTTPTrace
		before        *int
		testCaseInput ***testRenderRemoteCacheDependenciesCaseInputs
		out2          *[]map[string]any
	}{origin: &origin, trace: &trace, before: &before, testCaseInput: &testCaseInput, out2: &out2})
	closeTestTreeCache(t, cache)
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

	{
		assert.FatalIf(
			t,
			err != nil || !hasRenderField(out3, "", "remote-v2") || reflect.DeepEqual(out3, out1),
			"changed remote render: %s, %v",
			out3,
			err,
		)
	}
	if (*origin) != "git" && (*origin) != "nested-mount" {
		got := trace.snapshot()[before:]
		t.Logf(
			"changed render and preflight: requests=%+v accepted_connections=%d",
			got,
			trace.connections.Load(),
		)
		{
			assert.ErrorIf(
				t,
				len(got) != 1 || got[0].Status != http.StatusOK || got[0].Bytes == 0,
				"changed render must acquire one response including subsequent preflight: %+v",
				got,
			)
		}
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
		{
			assert.ErrorIf(t, len(got) != 1 || got[0].Method != http.MethodGet || got[0].ETag == "" ||
				got[0].Modified == "" ||
				got[0].Status != http.StatusNotModified ||
				got[0].Bytes != 0, "unchanged render must make one conditional GET with zero body bytes: %+v", got)
		}
	}
}
