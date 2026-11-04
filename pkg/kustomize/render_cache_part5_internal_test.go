package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_RemoteSelectionTransition(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		initialStatus int
		changeGit     bool
	}{
		{name: "404-to-http", initialStatus: http.StatusNotFound},
		{name: "non-resource-to-http", initialStatus: http.StatusOK},
		{name: "404-to-git", initialStatus: http.StatusNotFound, changeGit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderRemoteSelectionTransitionCase(t, &tc)
		},
		)
	}
}
func emptyRenderInventory(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}

func TestRender_AcceptedCacheAcquisitionOverlap(t *testing.T) {
	t.Parallel()
	for _, validate := range []bool{false, true} {
		t.Run(fmt.Sprintf("validation_%t", validate), func(t *testing.T) {
			t.Parallel()
			testRenderAcceptedCacheAcquisitionOverlapCase(
				t,
				&validate,
			)
		},

		// Release the filesystem only after all acquisitions completed.

		)
	}
}

func TestRender_UnidentifiedContent(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("first")},
	)
	cache := newTestTreeCache(t, t.TempDir())
	request := func() RenderRequest {
		source := localSource(t, root)
		source.Key, source.Revision, source.Digest = "family", "revision", "digest"
		return RenderRequest{Source: source, Label: root, SubPath: ".", Spec: demoSpec}
	}
	first, _, err := testutil.RenderMaps(Render(t.Context(), cache, request(), sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	{
		assert.FatalIf(t, !hasRenderField(first, "name", "first"), "%s", "missing first render")
	}
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("second"))
	second, _, err := testutil.RenderMaps(Render(t.Context(), cache, request(), sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			!hasRenderField(second, "name", "second"),
			"%s",
			"descriptor metadata pinned unidentified mutable bytes",
		)
	}
}

func TestRender_SubstitutionRetainsRawBuild(t *testing.T) {
	t.Parallel()
	for _, postBuild := range []struct {
		name  string
		value *kustomizev1.PostBuild
	}{
		{"inline", &kustomizev1.PostBuild{Substitute: map[string]string{"CLUSTER_NAME": "home"}}},
		{
			"reference",
			&kustomizev1.PostBuild{
				SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: "ConfigMap", Name: "settings"}},
				Substitute:     map[string]string{"CLUSTER_NAME": "home"},
			},
		},
		{"strategy", &kustomizev1.PostBuild{SubstituteStrategy: "Always"}},
	} {
		t.Run(postBuild.name, func(t *testing.T) {
			t.Parallel()
			testRenderSubstitutionRetainsRawBuildCase(
				t,
				&postBuild,
			)
		},
		)
	}
}

func testRenderRemoteSelectionTransitionCase(
	t *testing.T,
	testCaseInput *struct {
		name          string
		initialStatus int
		changeGit     bool
	},
) {
	t.Helper()

	var changed atomic.Bool
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if changed.Load() && !testCaseInput.changeGit {
			_, _ = io.WriteString(w, cmYAML("file-output"))
			return
		}
		w.WriteHeader(testCaseInput.initialStatus)
		_, _ = io.WriteString(w, "<html>repository page</html>")
	}))
	t.Cleanup(srv.Close)
	url := srv.URL + "/org/repo/base"
	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": "resources: ['" + url + "']\n"})
	cacheDir := t.TempDir()
	newCache := func() *TreeCache {
		c := newTestTreeCache(t, cacheDir)
		c.SetGitBaseFetcher(
			func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				name := "git-output"
				if changed.Load() && testCaseInput.changeGit {
					name = "changed-git-output"
				}
				return &store.SourceArtifact{
					Key:      "repository",
					Content:  name,
					Revision: name,
					FS: memoryTree(
						t,
						map[string]string{
							"base/kustomization.yaml": "resources: [cm.yaml]\n",
							"base/cm.yaml":            cmYAML(name),
						},
					),
				}, nil
			},
		)
		return c
	}
	c := newCache()
	request := RenderRequest{Source: localSource(t, root), Label: root, Spec: demoSpec}
	out, receipt, err := testutil.RenderMaps(Render(t.Context(), c, request, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(
			t,
			err != nil || receipt == nil || !hasRenderField(out, "name", "git-output"),
			"initial directory selection: %s, %v",
			out,
			err,
		)
	}
	realOutput := testRenderEncoding(t, out)
	{
		recordRenderErr := c.RecordRender(
			t.Context(),
			RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt},
		)
		assert.NoError(t, recordRenderErr)
	}
	frame := receipt.Value().(renderFrame)
	snap := frame.header.Reads
	key := frame.header.Key
	eligible := renderPersistenceEligible(t)
	persisted, saved, hit := readTestRenderFrame(t, c, key)
	assert.FatalIf(t, hit != eligible ||
		hit &&
			(!reflect.DeepEqual(persisted, snap) || !bytes.Equal(realOutput, testRenderEncoding(t, saved))), "persisted=%t eligible=%t observations=%+v output=%s", hit, eligible, persisted.Remotes, saved)
	t.Logf("persistence eligible=%t persisted hit=%t", eligible, hit)
	if hit {
		snap = persisted
	}
	assert.FatalIf(
		t,
		len(snap.Remotes) != 2 || snap.Remotes[0].Input != (remoteInput{Operation: remoteHTTP, URL: url}) ||
			snap.Remotes[1].Input.Operation != remoteGit ||
			snap.Remotes[1].Identity != (remoteIdentity{Present: true, Readable: true, Key: "repository", Content: "git-output"}),
		"selection observations=%+v",
		snap.Remotes,
	)
	if testCaseInput.initialStatus == http.StatusNotFound {
		if snap.Remotes[0].Identity != (remoteIdentity{HTTPStatus: http.StatusNotFound}) {
			t.Fatalf("HTTP probe identity=%+v", snap.Remotes[0].Identity)
		}
	} else if snap.Remotes[0].Identity.Content == "" || snap.Remotes[0].Identity.HTTPStatus != 0 {
		t.Fatalf("HTTP content identity=%+v", snap.Remotes[0].Identity)
	}
	if hits.Load() != 1 {
		t.Fatalf("one invocation probed %d times", hits.Load())
	}
	closeTestTreeCache(t, c)
	c = newCache()
	{
		valid, stillValidErr := snap.stillValid(
			t.Context(),
			c,
			&treeFS{lower: request.Source.FS},
			false,
		)
		assert.FatalIf(t, stillValidErr != nil ||
			!valid, "unchanged resource selection rejected Git: valid=%t, %v", valid, stillValidErr)
	}
	out, _, err = testutil.RenderMaps(Render(t.Context(), c, request, sourcecache.RetryConfig{}))
	{
		assert.FatalIf(
			t,
			err != nil || !bytes.Equal(realOutput, testRenderEncoding(t, out)),
			"unchanged real output: %s, %v",
			out,
			err,
		)
	}
	if eligible {
		publishTestFrame(
			t,
			c.owner,
			sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
			testRenderFrame(t, persisted, testRenderPayload(t, "REMOTE-SELECTION")),
		)
	}
	closeTestTreeCache(t, c)
	c = newCache()
	out, _, err = testutil.RenderMaps(Render(t.Context(), c, request, sourcecache.RetryConfig{}))
	witness := hasRenderField(out, "witness", "REMOTE-SELECTION")
	{
		assert.FatalIf(
			t,
			err != nil || witness != eligible ||
				!eligible && !bytes.Equal(realOutput, testRenderEncoding(t, out)),
			"saved-render reuse: %s, %v; eligible=%t",
			out,
			err,
			eligible,
		)
	}
	t.Logf(
		"persistence eligible=%t witness assertion executed=%t witness reused=%t",
		eligible,
		eligible,
		witness,
	)
	closeTestTreeCache(t, c)
	changed.Store(true)
	c = newCache()
	{
		valid, stillValidErr2 := snap.stillValid(
			t.Context(),
			c,
			&treeFS{lower: request.Source.FS},
			false,
		)
		assert.FatalIf(t, stillValidErr2 != nil ||
			valid, "changed resource selection reused Git: valid=%t, %v", valid, stillValidErr2)
	}
	out, _, err = testutil.RenderMaps(Render(t.Context(), c, request, sourcecache.RetryConfig{}))
	want := "file-output"
	if testCaseInput.changeGit {
		want = "changed-git-output"
	}
	{
		assert.FatalIf(
			t,
			err != nil || !hasRenderField(out, "name", want) || hasRenderField(out, "name", "git-output") ||
				hasRenderField(out, "witness", "REMOTE-SELECTION"),
			"resource transition: %s, %v",
			out,
			err,
		)
	}
	if hits.Load() != 4 {
		t.Fatalf("validation/preflight acquisitions=%d, want one per invocation", hits.Load())
	}
}

func testRenderAcceptedCacheAcquisitionOverlapCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()
	eligible := renderPersistenceEligible(t)
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")},
	)
	cache := newTestTreeCache(t, t.TempDir())
	key := sourcecache.BlobKey{
		Content: renderKey(
			&RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec, SubPath: "."},
			root,
			nil,
		),
		Render: sourcecache.RenderAccepted,
	}
	frame := testRenderFrame(
		t,
		readSetSnapshot{
			Files: []localEntry{{Path: "cm.yaml", Hash: manifest.SHA256Hex([]byte(cmYAML("fresh")))}},
		},
		testRenderPayload(t, "VALIDATED"),
	)
	if *testCaseInput {
		publishTestFrame(t, cache.owner, key, frame)
	}
	lower := &pausedRenderFS{FS: os.DirFS(root), entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	released := false
	defer func() {
		if !released {
			close(lower.release)
		}
		wg.Wait()
	}()
	rendered := make(chan error, 1)
	wg.Go(func() {
		out, receipt25, err := testutil.RenderMaps(
			Render(
				ctx,
				cache,
				RenderRequest{
					Source:  &store.SourceArtifact{FS: lower},
					Label:   root,
					SubPath: ".",
					Spec:    demoSpec,
				},
				sourcecache.RetryConfig{},
			),
		)
		if err == nil {
			{
				recordErr := cache.RecordRender(
					ctx,
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt25},
				)
				assert.NoError(t, recordErr)
			}
		}

		if err == nil && (*testCaseInput) && eligible && !hasRenderField(out, "witness", "VALIDATED") {
			err = fmt.Errorf("validation did not return its acquired frame: %q", out)
		}
		if err == nil && (!(*testCaseInput) || !eligible) && !hasRenderField(out, "name", "fresh") {
			err = fmt.Errorf("real render = %q", out)
		}
		rendered <- err
	})
	select {
	case <-lower.entered:
	case <-ctx.Done():
		t.Fatal("render did not reach input-read barrier")
	}
	progress := make(chan error, 1)
	wg.Go(func() {
		progress <- func() error {
			_, _, _, err := cache.owner.Blob(ctx, key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
			if (*testCaseInput) && eligible && err != nil {
				return err
			}
			if (!(*testCaseInput) || !eligible) && !errors.Is(err, errdef.ErrNotFound) {
				return fmt.Errorf("lookup before completed publication: %w", err)
			}
			_, _, _, err = cache.owner.Blob(ctx, key, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				_, writeErr := w.Write(frame)
				return sourcecache.ProducerResult{}, writeErr
			})
			if err != nil {
				return err
			}
			_, _, _, err = cache.owner.Blob(ctx, sourcecache.BlobKey{Content: struct {
				Name string `json:"Name"`
			}{"ordinary-source"}}, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				_, writeStringErr := io.WriteString(w, "source")
				return sourcecache.ProducerResult{}, writeStringErr
			})
			if err != nil {
				return err
			}
			return nil
		}()
	})
	select {
	case err := <-progress:
		{
			assert.NoError(t, err)
		}
	case <-ctx.Done():
		t.Fatal("same-frame lookup/publication or source acquisition blocked behind input read")
	}

	close(lower.release)
	released = true
	select {
	case err := <-rendered:
		{
			assert.NoError(t, err)
		}
	case <-ctx.Done():
		t.Fatal("render did not finish")
	}
}

func testRenderSubstitutionRetainsRawBuildCase(
	t *testing.T,
	testCaseInput *struct {
		name  string
		value *kustomizev1.PostBuild
	},
) {
	t.Helper()

	root := testutil.WriteTree(
		t,
		map[string]string{
			"kustomization.yaml": existingKustomization("cm.yaml"),
			"cm.yaml":            cmYAML("original"),
		},
	)
	cache := newTestTreeCache(t, t.TempDir())
	var builds atomic.Int64
	source := localSource(t, root)
	source.FS = &pausedRenderFS{FS: source.FS, builds: &builds}
	spec := demoSpec.Clone()
	req := RenderRequest{Source: source, Spec: spec, SubPath: "."}
	content := func(request RenderRequest, docs []manifest.DocOccurrence) any {
		private := slices.Clone(docs)
		for i := range private {
			private[i].Document = manifest.DeepCopyMap(private[i].Document)
		}
		return request.Content(private)
	}
	docs, receipt, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.Equal(t, builds.Load(), int64(1))
	assert.NoError(
		t,
		cache.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   spec.Named(),
				Receipt: receipt,
				Records: emptyRenderInventory,
				Content: content(req, docs),
			},
		),
	)
	history := func(request RenderRequest) bool {
		raw, pending, found := cache.PreviousRender(t.Context(), request)
		if !found {
			return false
		}
		_, known := cache.RenderHistory(spec.Named(), pending, content(request, raw))
		return known
	}
	if cache.owner.CanPersistRenders() {
		known := history(req)
		assert.Equal(t, known, true)
	}
	changed := req
	changed.Spec = spec.Clone()
	changed.Spec.PostBuild = testCaseInput.value
	if cache.owner.CanPersistRenders() {
		known := history(changed)
		assert.Equal(t, known, false)
	}
	again, next, err := Render(t.Context(), cache, changed, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.Diff(t, again, docs)
	assert.Equal(t, builds.Load(), int64(1))
	beforeFrame := receipt.Value().(renderFrame)
	afterFrame := next.Value().(renderFrame)
	assert.Equal(t, beforeFrame.header.Key, afterFrame.header.Key)
	assert.Equal(t, manifest.ValueEqual(content(req, docs), content(changed, again)), false)
	_, accepted := cache.RenderHistory(spec.Named(), next, content(changed, again))
	assert.Equal(t, accepted, false)
	assert.Equal(t, spec.PostBuild == nil, true)
	assert.NoError(
		t,
		cache.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   spec.Named(),
				Receipt: next,
				Records: emptyRenderInventory,
				Content: content(changed, again),
			},
		),
	)
	if cache.owner.CanPersistRenders() {
		known := history(changed)
		assert.Equal(t, known, true)
		known = history(req)
		assert.Equal(t, known, false)
	}
}
