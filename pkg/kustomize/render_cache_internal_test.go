package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

//nolint:gochecknoglobals // Cache fixtures share one immutable manifest identity.
var demoSpec = &manifest.Kustomization{Name: "demo", Namespace: "flux-system", Path: "./"}

func renderPersistenceEligible(tb testing.TB) bool {
	tb.Helper()
	info, ok := debug.ReadBuildInfo()
	eligible := false
	if ok {
		const module = "github.com/home-operations/flate"
		eligible = info.Main.Path == module && info.Main.Version != "" && info.Main.Version != "(devel)"
		for _, dep := range info.Deps {
			if dep.Path == module && dep.Version != "" && dep.Version != "(devel)" {
				eligible = true
			}
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				eligible = true
			}
		}
		renderPersistenceEligibleLoop(struct {
			info     **debug.BuildInfo
			eligible *bool
		}{info: &info, eligible: &eligible})
		for _, setting := range info.Settings {
			if setting.Key == "vcs.modified" && setting.Value == "true" {
				eligible = false
			}
		}
	}
	return eligible
}

func readTestRenderFrame(tb testing.TB, cache *TreeCache, key string) (readSetSnapshot, []map[string]any, bool) {
	tb.Helper()
	var r fs.FS
	var err error
	for _, phase := range []sourcecache.RenderMode{sourcecache.RenderAccepted, sourcecache.RenderPending} {
		r, _, _, err = cache.owner.Blob(
			tb.Context(),
			sourcecache.BlobKey{Content: key, Render: phase},
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
			nil,
		)
		if !errors.Is(err, errdef.ErrNotFound) {
			break
		}
	}
	if errors.Is(err, errdef.ErrNotFound) {
		return readSetSnapshot{}, nil, false
	}
	if err != nil {
		tb.Fatal(err)
	}
	raw, err := fs.ReadFile(r, "content")
	if err != nil {
		tb.Fatal(err)
	}
	snapFrame, err := decodeFrame(raw)
	snap := snapFrame.header.Reads
	out := snapFrame.output
	if err != nil {
		tb.Fatal(err)
	}
	docs, err := testutil.DecodeRender(out)
	if err != nil {
		tb.Fatal(err)
	}
	return snap, docs, true
}

//nolint:gocritic // Test helpers preserve independent key values while replacing callbacks.
func publishTestFrame(tb testing.TB, owner *sourcecache.Cache, key sourcecache.BlobKey, raw []byte) {
	tb.Helper()
	if key.Render == sourcecache.RenderPending && renderPersistenceEligible(tb) {
		key.Render = sourcecache.RenderAccepted
	}
	if frame, err := decodeFrame(raw); err == nil {
		if render, ok := key.Content.(string); ok {
			frame.header.Key = render
		}
		raw, err = encodeFrame(&frame.header, frame.output)
		if err != nil {
			tb.Fatal(err)
		}
	}
	_, _, _, err := owner.Blob(
		tb.Context(),
		key,
		&sourcecache.Policy{MaxAge: 0},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, err := w.Write(raw)
			return sourcecache.ProducerResult{}, err
		},
	)
	if err != nil {
		tb.Fatal(err)
	}
}

func testRenderFrame(tb testing.TB, snap *readSetSnapshot, output []byte) []byte {
	tb.Helper()
	frame, err := encodeFrame(&renderHeader{Reads: *snap}, output)
	if err != nil {
		tb.Fatal(err)
	}
	return frame
}

func TestRender_CacheHitMissInvalidate(t *testing.T) {
	t.Parallel()
	t.Run("empty stream", func(t *testing.T) {
		t.Parallel()
		renderCacheHitMissInvalidateEmptyStream(t)
	})
	t.Logf("render persistence eligible=%v", renderPersistenceEligible(t))
	root := testutil.WriteTree(t, map[string]string{
		"selected/kustomization.yaml": "resources: [../child]\n",
		"child/kustomization.yaml":    "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../cm.yaml\ngenerators: [../plugins]\npatches:\n  - path: ../patch.yaml\n",
		"plugins/kustomization.yaml":  "resources: [g.yaml]\n",
		"plugins/g.yaml":              "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [../value.txt]\n",
		"value.txt":                   "v1",
		"patch.yaml":                  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {patched: v1}\n",
		"cm.yaml":                     "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v1\nextra: {sequence: [{value: original}]}\n",
	})
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	ctx := t.Context()

	out1, receipt2, err := testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     "selected",
				Spec:        demoSpec,
			},
			sourcecache.RetryConfig{},
		),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt2},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(t, err != nil, "first render: %v", err)
	assert.FatalIf(t, !hasRenderField(out1, "k", "v1"), "first render missing data; got %s", out1)

	want := make([]map[string]any, len(out1))
	for i, doc := range out1 {
		want[i] = manifest.DeepCopyMap(doc)
	}
	renderCacheHitMissInvalidateLoop(struct{ out1 *[]map[string]any }{out1: &out1})
	out1[0]["metadata"].(map[string]any)["name"] = "mutated"
	var receipt3 *manifest.RenderReceipt
	out1, receipt3, err = testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt3},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, err)
	assert.Diff(t, out1, want)

	key := renderKey(&RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec, SubPath: "selected"}, root, nil)
	snap, _, ok := readTestRenderFrame(t, cache, key)
	eligible := renderPersistenceEligible(t)
	assert.FatalIf(t, !ok, "%s", "owner did not retain completed render")
	if eligible {
		publishTestFrame(
			t,
			cache.owner,
			sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
			testRenderFrame(t, new(snap), testRenderPayload(t, "SENTINEL")),
		)
	}
	cache.Close()
	{
		closeErr := cache.owner.Close()
		assert.NoError(t, closeErr)
	}
	cache = newTestTreeCache(t, cacheDir)
	out2, receipt4, err := testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     "selected",
				Spec:        demoSpec,
			},
			sourcecache.RetryConfig{},
		),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt4},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(t, err != nil, "second render: %v", err)
	assert.ErrorIf(
		t,
		eligible && !hasRenderField(out2, "witness", "SENTINEL"),
		"expected a cache hit returning stored bytes; got %s",
		out2,
	)
	assert.ErrorIf(t, !eligible && !reflect.DeepEqual(out2, out1), "ineligible owner changed output: %s", out2)

	// Editing a recorded input must invalidate: the cache misses, krusty re-runs,
	// and the fresh output reflects the change (never the stale sentinel).
	{
		writeFileErr := os.WriteFile(filepath.Join(root, "cm.yaml"),
			[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v2\n"), 0o600)
		assert.NoError(t, writeFileErr)
	}
	out3, receipt5, err := testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     "selected",
				Spec:        demoSpec,
			},
			sourcecache.RetryConfig{},
		),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt5},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(t, err != nil, "post-edit render: %v", err)
	assert.FatalIf(
		t,
		hasRenderField(out3, "witness", "SENTINEL"),
		"%s",
		"edit did not invalidate the cache (stale hit — unsound)",
	)
	assert.ErrorIf(t, !hasRenderField(out3, "k", "v2"), "re-render should reflect the edit; got %s", out3)
	testutil.WriteFile(
		t,
		root,
		"patch.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {patched: v2}\n",
	)
	out4, receipt6, err := testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt6},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		err != nil || !hasRenderField(out4, "patched", "v2"),
		"consumed patch edit replayed stale output: %s, %v",
		out4,
		err,
	)
	testutil.WriteFile(
		t,
		root,
		"child/kustomization.yaml",
		"namePrefix: child-\nresources: [../cm.yaml]\npatches: [{path: ../patch.yaml}]\n",
	)
	var receipt7 *manifest.RenderReceipt
	out4, receipt7, err = testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt7},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		err != nil || !hasRenderField(out4, "name", "child-demo"),
		"outside-subpath control edit replayed stale output: %s, %v",
		out4,
		err,
	)
	testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [../cm.yaml]\ngenerators: [../plugins]\n")
	testutil.WriteFile(t, root, "value.txt", "v2")
	var receipt8 *manifest.RenderReceipt
	out4, receipt8, err = testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt8},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		err != nil || !hasRenderField(out4, "value.txt", "v2"),
		"local plugin directory lost its source read set: %s, %v",
		out4,
		err,
	)
	cache.Close()
	{
		err := cache.owner.Close()
		assert.NoError(t, err)
	}
	cache = newTestTreeCache(t, cacheDir)
	_, persisted, hit := readTestRenderFrame(t, cache, key)
	assert.FatalIf(
		t,
		hit != eligible || hit && !reflect.DeepEqual(persisted, out4),
		"replacement after reopen = %s, %v; eligible = %v",
		persisted,
		hit,
		eligible,
	)
	testRenderCacheTypedEdits(ctx, t, &renderCacheTypedEditsInputs{root: &root, cache: &cache, output: &out4})
}

// TestRender_SourceignoreContentInvalidates verifies policy content and path
// changes invalidate persisted renders after re-admission.
func TestRender_SourceignoreContentInvalidates(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"a.yaml":        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n",
		"b.yaml":        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
		".sourceignore": "# excludes nothing yet\n",
	})
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	ctx := t.Context()
	req := RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoSpec}
	input, err := cache.sourceRootFor(ctx, req.Source, root)
	assert.NoError(t, err)
	key := renderKey(&req, root, input.ignore)

	out1, receipt11, err := testutil.RenderMaps(Render(ctx, cache, req, sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt11},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(t, err != nil, "first render: %v", err)
	assert.FatalIf(
		t,
		!hasRenderField(out1, "name", "a") || !hasRenderField(out1, "name", "b"),
		"first render should include both a and b; got %s",
		out1,
	)

	{
		writeFileErr2 := os.WriteFile(filepath.Join(root, ".sourceignore"), []byte("b.yaml\n"), 0o600)
		assert.NoError(t, writeFileErr2)
	}

	cache = newTestTreeCache(t, cacheDir)
	req.Source = localSource(t, root)
	input, err = cache.sourceRootFor(ctx, req.Source, root)
	assert.NoError(t, err)
	changedKey := renderKey(&req, root, input.ignore)
	assert.FatalIf(t, changedKey == key, "%s", "re-admitted ignore bytes reused the render key")
	out2, receipt12, err := testutil.RenderMaps(Render(ctx, cache, req, sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt12},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(t, err != nil, "second render: %v", err)
	assert.ErrorIf(
		t,
		hasRenderField(out2, "name", "b"),
		"%s",
		"STALE HIT: a .sourceignore edit excluding b.yaml did not invalidate the cache",
	)
	assert.ErrorIf(t, !hasRenderField(out2, "name", "a"), "re-render should still include a; got %s", out2)
	{
		removeErr := os.Remove(filepath.Join(root, ".sourceignore"))
		assert.NoError(t, removeErr)
	}
	testutil.WriteFile(t, root, "app/.sourceignore", "b.yaml\n")
	cache = newTestTreeCache(t, cacheDir)
	req.Source = localSource(t, root)
	input, err = cache.sourceRootFor(ctx, req.Source, root)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		renderKey(&req, root, input.ignore) == changedKey,
		"%s",
		"moved ignore file reused the render key",
	)
	out3, receipt13, err := testutil.RenderMaps(Render(ctx, cache, req, sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := cache.RecordRender(
				ctx,
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt13},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.FatalIf(
		t,
		err != nil || !hasRenderField(out3, "name", "a") || !hasRenderField(out3, "name", "b"),
		"nested policy reused root policy membership: %s, %v",
		out3,
		err,
	)
}

// TestRender_MemoryOwnerUnchanged pins output parity across backing choices.
func TestRender_MemoryOwnerUnchanged(t *testing.T) {
	t.Parallel()
	files := map[string]string{
		"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v1\n",
	}
	var want []map[string]any
	closeCaches := make([]func(), 0, 2)
	defer func() {
		for _, closeCache := range slices.Backward(closeCaches) {
			closeCache()
		}
	}()
	for _, disk := range []bool{false, true} {
		root, storage := testutil.WriteTree(t, files), ""
		if disk {
			storage = t.TempDir()
		}
		first := newTestTreeCache(t, storage)
		second := NewTreeCache(first.owner, nil, task.NewBounded(2))
		closeCaches = append(closeCaches, second.Close)
		var builds atomic.Int64
		lower := &pausedRenderFS{FS: os.DirFS(root), builds: &builds}
		req := RenderRequest{Source: &store.SourceArtifact{FS: lower}, Label: root, SubPath: ".", Spec: demoSpec}
		var outputs [2][]map[string]any
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i, cache := range []*TreeCache{first, second} {
			wg.Go(func() {
				<-start
				var err error
				outputs[i], _, err = testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
				assert.NoError(t, err)
			})
		}
		close(start)
		wg.Wait()
		assert.Equal(t, builds.Load(), int64(1))
		assert.Diff(t, outputs[0], outputs[1])
		if !disk {
			want = outputs[1]
		}
		assert.Diff(t, outputs[0], want)
		outputs[0][0]["data"].(map[string]any)["k"] = "mutated"
		again, _, err := testutil.RenderMaps(Render(t.Context(), second, req, sourcecache.RetryConfig{}))
		assert.NoError(t, err)
		assert.Diff(t, again, outputs[1])
		assert.Equal(t, builds.Load(), int64(1))
	}
}

// TestEncodeDecodeFrame_RoundTrip pins the kustomize-specific value codec: the
// read-set snapshot + rendered output survive framing intact. The frame is the
// plain bytes handed to the shared Blob owner.
func TestEncodeDecodeFrame_RoundTrip(t *testing.T) {
	t.Parallel()
	snap := readSetSnapshot{
		Files: []localEntry{{Path: "kustomization.yaml", Hash: "h1"}, {Path: "a.yaml", Hash: "h2"}},
		Dirs:  []localEntry{{Path: ".", Hash: "dirhash"}},
		Nodes: []localEntry{{Path: "comp", Kind: kindAbsent}},
	}
	for _, output := range []string{"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\n", ""} {
		t.Run(fmt.Sprintf("output-bytes-%d", len(output)), func(t *testing.T) {
			t.Parallel()
			frame, err := encodeFrame(&renderHeader{Reads: snap}, []byte(output))
			assert.FatalIf(t, err != nil, "encodeFrame: %v", err)
			gotSnapFrame, err := decodeFrame(frame)
			gotSnap := gotSnapFrame.header.Reads
			gotOut := gotSnapFrame.output
			assert.FatalIf(t, err != nil, "decodeFrame: %v", err)
			assert.ErrorIf(t, string(gotOut) != output, "output mismatch: want %q, got %q", output, gotOut)
			assert.ErrorIf(
				t,
				!reflect.DeepEqual(gotSnap, snap),
				"snapshot mismatch: want %+v, got %+v",
				snap,
				gotSnap,
			)
		})
	}
}

// TestDecodeFrame_RejectsMalformed pins the bounds checks: a frame shorter than
// the 4-byte header, or one whose header claims more snapshot bytes than exist,
// is a decode error (→ a cache miss, never a panic or torn read).
func TestDecodeFrame_RejectsMalformed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{"short header", []byte{0, 0}},
		{"truncated snapshot", []byte{0, 0, 0, 10, 'x'}},
		{"invalid snapshot JSON", []byte{0, 0, 0, 1, 'x'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			{
				_, err := decodeFrame(tc.frame)
				assert.FatalIf(t, err == nil, "%s", "malformed frame decoded successfully")
			}
		})
	}
}

func testRenderCacheHitMissInvalidateEmptyStream(t *testing.T) {
	t.Helper()

	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": "resources: []\n"})
	storage := t.TempDir()
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	for range 2 {
		c := newTestTreeCache(t, storage)
		docs, receipt1, err := testutil.RenderMaps(Render(t.Context(), c, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := c.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt1},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		assert.Equal(t, len(docs), 0)
		_, stored, hit := readTestRenderFrame(t, c, renderKey(&req, root, nil))
		assert.Equal(t, hit, true)
		assert.Equal(t, len(stored), 0)
	}
}

func renderCacheHitMissInvalidateEmptyStream(t *testing.T) {
	t.Helper()

	testRenderCacheHitMissInvalidateEmptyStream(t)
}

func renderPersistenceEligibleLoop(loopInputs struct {
	info     **debug.BuildInfo
	eligible *bool
},
) {
	for _, dep := range (*loopInputs.info).Deps {
		if dep.Replace != nil && dep.Replace.Version == "" {
			(*loopInputs.eligible) = false
		}
	}
}

func renderCacheHitMissInvalidateLoop(loopInputs struct{ out1 *[]map[string]any }) {
	for _, doc := range *loopInputs.out1 {
		if manifest.DocMetadata(doc).Name == "demo" {
			doc["extra"].(map[string]any)["sequence"].([]any)[0].(map[string]any)["value"] = "mutated"
		}
	}
}

type renderCacheTypedEditsInputs struct {
	root   *string
	cache  **TreeCache
	output *[]map[string]any
}

func testRenderCacheTypedEdits(ctx context.Context, t *testing.T, inputs *renderCacheTypedEditsInputs) {
	t.Helper()
	spec := demoSpec.Clone()
	for _, tc := range []struct {
		name, want string
		edit       func()
	}{
		{name: "prefix", want: "name: typed-demo", edit: func() { spec.NamePrefix = "typed-" }},
		{name: "namespace", want: "namespace: typed", edit: func() { spec.TargetNamespace = "typed" }},
		{name: "patch", want: "from-spec: typed", edit: func() {
			spec.Patches = []fluxapis.Patch{{Patch: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {from-spec: typed}\n"}}
		}},
	} {
		tc.edit()
		req := RenderRequest{Source: localSource(t, *inputs.root), Label: *inputs.root, SubPath: "selected", Spec: spec}
		out, receipt9, err := testutil.RenderMaps(Render(ctx, *inputs.cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := (*inputs.cache).RecordRender(
					ctx,
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt9},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.FatalIf(t, err != nil ||
			!hasRenderField(out, strings.SplitN(tc.want, ": ", 2)[0], strings.SplitN(tc.want, ": ", 2)[1]) ||
			reflect.DeepEqual(out, *inputs.output), "typed edit returned stale output: %s, %v", out, err)
		repeated, receipt10, err := testutil.RenderMaps(Render(ctx, *inputs.cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := (*inputs.cache).RecordRender(
					ctx,
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt10},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.FatalIf(
			t,
			err != nil || !reflect.DeepEqual(out, repeated),
			"typed render is not deterministic: %s, %v",
			repeated,
			err,
		)
		*inputs.output = out
	}
}
