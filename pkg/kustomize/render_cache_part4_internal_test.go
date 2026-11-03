package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"

	"github.com/home-operations/flate/internal/assert"
	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

func TestRender_RemoteCacheLocalAttribution(t *testing.T) {
	t.Parallel()
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted attribution assertions executed=%v; recorded snapshot assertions always execute", eligible)
	for _, scenario := range []string{"after-git", "rewritten-plugin", "source-link"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testRenderRemoteCacheLocalAttributionCase(
				t,
				&scenario,
			)
		},
		)
	}
	t.Run(
		"source-link-to-injected-file",
		func(t *testing.T) {
			t.Parallel()
			testRenderRemoteCacheLocalAttributionSourceLinkToInjectedFile(t)
		},
	)
	t.Run(
		"nested-snapshot-observations",
		func(t *testing.T) {
			t.Parallel()
			testRenderRemoteCacheLocalAttributionNestedSnapshotObservations(t)
		},
	)
}

func TestRender_RemoteCacheIsolation(t *testing.T) {
	t.Parallel()
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted isolation assertions executed=%v; concurrent snapshot assertions always execute", eligible)
	var shared, distinct atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/shared" {
			shared.Add(1)
			entered <- struct{}{}
			<-release
		} else {
			distinct.Add(1)
		}
		_, _ = io.WriteString(w, "contents")
	}))
	t.Cleanup(srv.Close)
	c := newTestTreeCache(t, t.TempDir())
	type result struct {
		request RenderRequest
		snap    readSetSnapshot
		output  []map[string]any
		err     error
	}
	results := make(chan result, 2)
	requests := make([]RenderRequest, 2)
	for i := range requests {
		name := fmt.Sprintf("local-%d", i)
		root := testutil.WriteTree(t, map[string]string{
			"kustomization.yaml": "resources: [" + name + ".yaml]\nconfigMapGenerator: [{name: generated, files: ['first=" + srv.URL + "/shared', 'second=" + srv.URL + "/shared', 'distinct=" + srv.URL + "/" + name + "']}]\n",
			name + ".yaml":       cmYAML(name),
		})
		requests[i] = RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	for _, request := range requests {
		wg.Go(func() {
			rec := newReadSet(request.Label)
			view := newTreeFS(request.Source.FS, request.Label, rec)
			err := preflightRemoteResources(t.Context(), c, view, request.Label)
			if err != nil {
				results <- result{err: err}
				return
			}
			if _, buildTestTreeFSErr := buildTestTreeFS(view); buildTestTreeFSErr != nil {
				results <- result{err: buildTestTreeFSErr}
				return
			}
			out, receipt32, err := testutil.RenderMaps(Render(t.Context(), c, request, sourcecache.RetryConfig{}))
			if err == nil {
				{
					recordErr := c.RecordRender(
						t.Context(),
						RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt32},
					)
					assert.NoError(t, recordErr)
				}
			}

			results <- result{request: request, snap: rec.snapshot(), output: out, err: err}
		})
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent renders did not reach shared acquisition")
	}
	close(release)
	released = true
	for range requests {
		got := <-results
		assert.FatalIf(
			t,
			got.err != nil || !hasRenderField(got.output, "first", "contents"),
			"concurrent render: %s, %v",
			got.output,
			got.err,
		)
		snap := got.snap
		if persisted, _, hit := readTestRenderFrame(t, c, renderKey(got.request, got.request.Label, nil)); hit {
			snap = persisted
		}
		{
			assert.FatalIf(
				t,
				len(snap.Remotes) != 2 || snap.Remotes[0].Input.URL >= snap.Remotes[1].Input.URL,
				"remote descriptors not private/deduplicated/sorted: %+v",
				snap,
			)
		}
		control, err := fs.ReadFile(got.request.Source.FS, "kustomization.yaml")
		{
			assert.NoError(t, err)
		}
		for _, entry := range snap.Remotes {
			{
				assert.FatalIf(
					t,
					!bytes.Contains(control, []byte(entry.Input.URL)),
					"another render's dependency leaked: %+v",
					entry,
				)
			}
		}
		for _, file := range snap.Files {
			{
				_, err := fs.Stat(got.request.Source.FS, file.Path)
				assert.FatalIf(t, err != nil, "another source's read leaked: %+v, %v", file, err)
			}
		}
		{
			valid, err := snap.stillValid(
				t.Context(),
				c,
				&treeFS{lower: got.request.Source.FS},
				false,
			)
			assert.FatalIf(t, err != nil ||
				!valid, "private snapshot invalid: %+v, %t, %v", snap, valid, err)
		}
	}
	if shared.Load() != 1 || distinct.Load() != 2 {
		t.Fatalf("shared requests=%d, distinct=%d", shared.Load(), distinct.Load())
	}
}

func buildTestTreeFS(view *treeFS) ([]byte, error) {
	kustomizestate.BuildMutex.Lock()
	defer kustomizestate.BuildMutex.Unlock()
	rm, err := fluxkustomize.Build(view, view.root)
	if err != nil {
		return nil, err
	}
	return rm.AsYaml()
}

func hasRenderField(value any, key, want string) bool {
	switch value := value.(type) {
	case string:
		return key == "" && strings.Contains(value, want)
	case []map[string]any:
		for _, doc := range value {
			if hasRenderField(doc, key, want) {
				return true
			}
		}
	case map[string]any:
		if value[key] == want {
			return true
		}
		if items, ok := value[key].([]any); ok {
			for _, item := range items {
				if item == want {
					return true
				}
			}
		}
		for _, item := range value {
			if hasRenderField(item, key, want) {
				return true
			}
		}
	case []any:
		for _, item := range value {
			if hasRenderField(item, key, want) {
				return true
			}
		}
	}
	return false
}

func testRenderEncoding(tb testing.TB, docs []map[string]any) []byte {
	tb.Helper()
	payload, err := testutil.EncodeRender(docs)
	if err != nil {
		tb.Fatal(err)
	}
	return payload
}

func testRenderPayload(tb testing.TB, witness string) []byte {
	tb.Helper()
	return testRenderEncoding(tb, []map[string]any{{"witness": witness}})
}

func testRenderRemoteCacheLocalAttributionCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "remote data")
	}))
	t.Cleanup(srv.Close)
	const gitURL = "file:///example.invalid/o/repo.git"
	files := map[string]string{
		"kustomization.yaml": "resources: ['" + gitURL + "?ref=main', local.yaml]\n",
		"local.yaml":         cmYAML("local-v1"),
	}
	changed, value, want := "local.yaml", cmYAML("local-v2"), "name: local-v2"
	if (*testCaseInput) == "rewritten-plugin" {
		files["kustomization.yaml"] = "resources: ['" + gitURL + "?ref=main']\ngenerators: [plugins]\n"
		files["plugins/kustomization.yaml"] = "resources: [nested]\n"
		files["plugins/nested/kustomization.yaml"] = "resources: [../g.yaml]\n"
		files["plugins/g.yaml"] = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [local.txt, 'remote=" + srv.URL + "/value']\n"
		files["local.txt"] = "v1"
		changed, value, want = "local.txt", "v2", "local.txt: v2"
	}
	if (*testCaseInput) == "source-link" {
		files["kustomization.yaml"] = "resources: ['" + gitURL + "?ref=main', link.yaml]\n"
		files["other.yaml"] = cmYAML("other")
		want = "name: other"
	}
	root := testutil.WriteTree(t, files)
	var linked fs.FS
	if (*testCaseInput) == "source-link" {
		links := tree.NewBuilder(memoryTree(t, files))
		{
			err := links.AddSymlink("link.yaml", "local.yaml")
			assert.NoError(t, err)
		}
		var err error
		linked, err = links.Build()
		{
			assert.NoError(t, err)
		}
	}
	remote := memoryTree(
		t,
		map[string]string{"kustomization.yaml": "resources: [cm.yaml]\n", "cm.yaml": cmYAML("git")},
	)
	cacheDir := t.TempDir()
	newCache := func() *TreeCache {
		c := newTestTreeCache(t, cacheDir)
		c.SetGitBaseFetcher(
			func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				return &store.SourceArtifact{
					FS:       remote,
					Key:      "family",
					Content:  "family-generation",
					Revision: "1",
				}, nil
			},
		)
		return c
	}
	request := func() RenderRequest {
		artifact := localSource(t, root)
		if linked != nil {
			artifact.FS = linked
		}
		return RenderRequest{Source: artifact, Label: root, SubPath: ".", Spec: demoSpec}
	}
	c := newCache()
	rec := newReadSet(root)
	view := newTreeFS(request().Source.FS, root, rec)
	{
		err := preflightRemoteResources(t.Context(), c, view, root)
		assert.NoError(t, err)
	}
	_, err := buildTestTreeFS(view)
	{
		assert.NoError(t, err)
	}
	snap := rec.snapshot()
	{
		valid, stillValidErr := snap.stillValid(
			t.Context(),
			c,
			&treeFS{lower: request().Source.FS},
			false,
		)
		assert.FatalIf(t, stillValidErr != nil ||
			!valid, "unchanged snapshot: %+v, %v, %v", snap, valid, stillValidErr)
	}
	observed := changed
	if (*testCaseInput) == "source-link" {
		observed = "link.yaml"
	}
	{
		assert.FatalIf(
			t,
			!slices.ContainsFunc(snap.Files, func(f localEntry) bool { return f.Path == observed }),
			"source input %q absent: %+v",
			observed,
			snap,
		)
	}
	for _, f := range snap.Files {
		{
			assert.FatalIf(
				t,
				strings.Contains(f.Path, remoteResourcePrefix),
				"overlay coordinate recorded as source: %+v",
				f,
			)
		}
	}
	for name, body := range files {
		got, readFileErr := os.ReadFile(filepath.Join(root, name))
		{
			assert.FatalIf(
				t,
				readFileErr != nil || string(got) != body,
				"source %q mutated: %q, %v",
				name,
				got,
				readFileErr,
			)
		}
	}
	out, receipt30, err := testutil.RenderMaps(Render(t.Context(), c, request(), sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := c.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt30},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.FatalIf(t, err != nil, "first render: %s, %v", out, err)
	}
	key := renderKey(request(), root, nil)
	if persisted, _, hit := readTestRenderFrame(t, c, key); hit {
		publishTestFrame(
			t,
			c.owner,
			sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
			testRenderFrame(t, persisted, testRenderPayload(t, "SENTINEL")),
		)
	}
	closeTestTreeCache(t, c)
	if (*testCaseInput) == "source-link" {
		links := tree.NewBuilder(linked)
		{
			addSymlinkErr := links.AddSymlink("link.yaml", "other.yaml")
			assert.NoError(t, addSymlinkErr)
		}
		linked, err = links.Build()
		{
			assert.NoError(t, err)
		}
	} else {
		testutil.WriteFile(t, root, changed, value)
	}
	c = newCache()
	{
		valid, stillValidErr2 := snap.stillValid(
			t.Context(),
			c,
			&treeFS{lower: request().Source.FS},
			false,
		)
		assert.FatalIf(t, stillValidErr2 != nil ||
			valid, "changed source validated: %t, %v", valid, stillValidErr2)
	}
	var receipt31 *manifest.RenderReceipt
	out, receipt31, err = testutil.RenderMaps(Render(t.Context(), c, request(), sourcecache.RetryConfig{}))
	if err == nil {
		{
			recordErr := c.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt31},
			)
			assert.NoError(t, recordErr)
		}
	}

	{
		assert.FatalIf(
			t,
			err != nil ||
				!hasRenderField(out, strings.SplitN(want, ": ", 2)[0], strings.SplitN(want, ": ", 2)[1]),
			"source edit: %s, %v; want %q",
			out,
			err,
			want,
		)
	}
	closeTestTreeCache(t, c)
}

func testRenderRemoteCacheLocalAttributionSourceLinkToInjectedFile(t *testing.T) {
	t.Helper()
	const body = "remote bytes"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	for _, open := range []bool{false, true} {
		t.Run(fmt.Sprintf("open_%t", open), func(t *testing.T) {
			c := newTestTreeCache(t, "")
			const root = "/source"
			rec := newReadSet(root)
			view := newTreeFS(memoryTree(t, nil), root, rec)
			walk := inputWalk{cache: c, fs: view}
			{
				_, err := walk.acquire(
					t.Context(),
					remoteInput{Operation: remoteHTTP, URL: srv.URL},
					&walkScope{},
				)
				assert.NoError(t, err)
			}
			generated, err := walk.mount([]byte(body))
			{
				assert.NoError(t, err)
			}
			links := tree.NewBuilder(nil)
			{
				addSymlinkErr2 := links.AddSymlink("link", filepath.Base(generated))
				assert.NoError(t, addSymlinkErr2)
			}
			lower, err := links.Build()
			{
				assert.NoError(t, err)
			}
			view.lower = lower
			view.upper = tree.NewBuilder(lower)
			{
				_, mountErr := walk.mount([]byte(body))
				assert.NoError(t, mountErr)
			}
			if open {
				f, openErr := view.Open(root + "/link")
				{
					assert.NoError(t, openErr)
				}
				data, openErr := io.ReadAll(f)
				{
					openErr = errors.Join(openErr, f.Close())
					assert.FatalIf(t, openErr != nil || string(data) != body, "read: %q, %v", data, openErr)
				}
			} else {
				data, readFileErr2 := view.ReadFile(root + "/link")
				assert.FatalIf(t, readFileErr2 != nil || string(data) != body, "read: %q, %v", data, readFileErr2)
			}
			snap := rec.snapshot()
			{
				assert.FatalIf(
					t,
					len(snap.Files) != 1 || snap.Files[0].Path != "link" || snap.Files[0].Hash == "",
					"authored source link lost: %+v",
					snap,
				)
			}
			{
				assert.FatalIf(
					t,
					validReadSet(t, readSetSnapshot{Files: snap.Files}, lower),
					"%s",
					"lower source cannot certify bytes obtained through an injected target",
				)
			}
			changed := tree.NewBuilder(lower)
			{
				addFileErr := changed.AddFile("local", []byte("new bytes"), 0o644)
				assert.NoError(t, addFileErr)
			}
			{
				addSymlinkErr3 := changed.AddSymlink("link", "local")
				assert.NoError(t, addSymlinkErr3)
			}
			current, err := changed.Build()
			{
				assert.NoError(t, err)
			}
			{
				assert.FatalIf(
					t,
					validReadSet(t, readSetSnapshot{Files: snap.Files}, current),
					"%s",
					"retargeted source link replayed injected bytes",
				)
			}
		})
	}
}

func testRenderRemoteCacheLocalAttributionNestedSnapshotObservations(t *testing.T) {
	t.Helper()
	root := testutil.WriteTree(
		t,
		map[string]string{
			"local.txt":           "source",
			"view-more-data/file": "prefix",
			"listed/one":          "one",
			"walked/one":          "one",
			"node":                "file",
		},
	)
	lower := os.DirFS(root)
	rec := newReadSet(root)
	view := newTreeFS(lower, root, rec)
	snapshot, err := view.upper.Build()
	{
		assert.NoError(t, err)
	}
	view.upper = tree.NewBuilder(snapshot)
	{
		mountErr2 := view.upper.Mount("view", snapshot)
		assert.NoError(t, mountErr2)
	}
	view.origins = append(view.origins, mountOrigin{path: "view", source: "."})
	c := newTestTreeCache(t, "")
	c.SetGitBaseFetcher(
		func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			return &store.SourceArtifact{
				FS:       memoryTree(t, map[string]string{"foreign": "remote"}),
				Key:      "git-family",
				Content:  "git-family-generation",
				Revision: "1",
			}, nil
		},
	)
	walk := inputWalk{cache: c, fs: view}
	gitPath, err := walk.fetchGitBase(
		t.Context(),
		kustomizeinput.GitBase{RepoURL: "file:///example.invalid/o/repo.git"},
		&walkScope{root: filepath.Join(root, "view")},
	)
	{
		assert.NoError(t, err)
	}
	snapshot, err = view.upper.Build()
	{
		assert.NoError(t, err)
	}
	view.upper = tree.NewBuilder(snapshot)
	{
		mountErr3 := view.upper.Mount("view-more", snapshot)
		assert.NoError(t, mountErr3)
	}
	view.origins = append(view.origins, mountOrigin{path: "view-more", source: "."})
	alias := filepath.Join(root, "view-more", "view")
	originalDir := filepath.Join(root, "view", gitPath)
	nestedDir := filepath.Join(alias, gitPath)
	original := walk.location(
		&controlLocation{
			file:       filepath.Join(originalDir, "foreign"),
			root:       originalDir,
			pluginRoot: filepath.Join(root, "view"),
		},
	)
	nested := walk.location(
		&controlLocation{file: filepath.Join(nestedDir, "foreign"), root: nestedDir, pluginRoot: alias},
	)
	{
		assert.FatalIf(
			t,
			original != nested || original.file == "",
			"nested remote location lost canonical coordinates: %+v, %+v",
			original,
			nested,
		)
	}
	{
		_, readFileErr3 := view.ReadFile(filepath.Join(alias, "local.txt"))
		assert.NoError(t, readFileErr3)
	}
	f, err := view.Open(filepath.Join(alias, "local.txt"))
	{
		assert.NoError(t, err)
	}
	{
		err := f.Close()
		assert.NoError(t, err)
	}
	{
		_, err := view.ReadFile(filepath.Join(root, "view-more-data/file"))
		assert.NoError(t, err)
	}
	{
		_, err := view.ReadFile(filepath.Join(alias, gitPath, "foreign"))
		assert.NoError(t, err)
	}
	{
		err := view.WriteFile(filepath.Join(alias, "generated"), []byte("generated"))
		assert.NoError(t, err)
	}
	{
		_, err := view.ReadFile(filepath.Join(alias, "generated"))
		assert.NoError(t, err)
	}
	{
		assert.FatalIf(
			t,
			!view.Exists(filepath.Join(alias, "node")) || view.IsDir(filepath.Join(alias, "node")),
			"%s",
			"aliased source node was not a file",
		)
	}
	{
		_, err := view.ReadDir(alias)
		assert.NoError(t, err)
	}
	{
		_, err := view.ReadDir(filepath.Join(alias, "listed"))
		assert.NoError(t, err)
	}
	{
		err := view.Walk(
			filepath.Join(alias, "walked"),
			func(_ string, _ os.FileInfo, err error) error { return err },
		)
		assert.NoError(t, err)
	}
	snap := rec.snapshot()
	{
		assert.FatalIf(t, len(snap.Remotes) != 1 || len(snap.Files) != 2 || snap.Files[0].Path != "local.txt" ||
			snap.Files[1].Path != "view-more-data/file", "nested snapshot attribution: %+v", snap)
	}
	{
		valid, err := snap.stillValid(t.Context(), c, &treeFS{lower: lower}, false)
		assert.FatalIf(
			t,
			err != nil || !valid,
			"snapshot contains injected directory members: %+v, %t, %v",
			snap,
			valid,
			err,
		)
	}
	for _, dir := range []string{"listed", "walked"} {
		t.Run(dir+"-membership", func(t *testing.T) {
			index := slices.IndexFunc(snap.Dirs, func(f localEntry) bool { return f.Path == dir })
			{
				assert.FatalIf(t, index < 0, "directory %q was not observed: %+v", dir, snap)
			}
			testutil.WriteFile(t, root, dir+"/two", "two")
			{
				assert.FatalIf(
					t,
					validReadSet(t, readSetSnapshot{Dirs: snap.Dirs[index : index+1]}, lower),
					"%s",
					"aliased directory membership change was missed",
				)
			}
		})
	}
	{
		err := os.Remove(filepath.Join(root, "node"))
		assert.NoError(t, err)
	}
	testutil.WriteFile(t, root, "node/child", "child")
	{
		assert.FatalIf(
			t,
			validReadSet(t, readSetSnapshot{Nodes: snap.Nodes}, lower),
			"%s",
			"aliased node transition was missed",
		)
	}
}
