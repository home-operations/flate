package kustomize

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/kustomize/api/krusty"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestPreflightRemoteResources_RemoteObservations(t *testing.T) {
	t.Parallel()
	t.Run(
		"root-after-nested",
		func(t *testing.T) { t.Parallel(); testPreflightRemoteResourcesRemoteObservationsRootAfterNested(t) },
	)
	for _, scenario := range []string{"nested-git", "http-plugin", "shared-mount", "shared-plugin", "equal-content-plugins", "same-repository-subpaths"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testPreflightRemoteResourcesRemoteObservationsCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func TestPreflightRemoteResources_LargeBodyMatchesUpstream(t *testing.T) {
	t.Parallel()
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: large}\n" + strings.Repeat(" ", 64<<20) + "\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/large.yaml\n"})
	kustomizestate.BuildMutex.Lock()
	upstream, err := krusty.MakeKustomizer(krusty.MakeDefaultOptions()).Run(fsys, ".")
	kustomizestate.BuildMutex.Unlock()
	{
		assert.FatalIf(t, err != nil, "upstream large resource: %v", err)
	}
	out, err := upstream.AsYaml()
	{
		assert.FatalIf(
			t,
			err != nil || !strings.Contains(string(out), "name: large"),
			"upstream resource output: %q error=%v",
			out,
			err,
		)
	}
	t.Logf("upstream Kustomize accepted %d-byte valid HTTP resource", len(body))
	{
		preflightRemoteResourcesErr := preflightRemoteResources(t.Context(), newTestTreeCache(t, ""), fsys, ".")
		assert.FatalIf(
			t,
			preflightRemoteResourcesErr != nil,
			"flate large resource after upstream success: %v",
			preflightRemoteResourcesErr,
		)
	}
	files := remoteFiles(t, fsys, ".")
	{
		assert.FatalIf(t, len(files) != 1, "large remote files: %v", files)
	}
	got, err := fsys.ReadFile(files[0])
	{
		assert.FatalIf(
			t,
			err != nil || string(got) != body,
			"large resource: size=%d error=%v, want %d bytes",
			len(got),
			err,
			len(body),
		)
	}
}

func testPreflightRemoteResourcesRemoteObservationsRootAfterNested(t *testing.T) {
	t.Helper()

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, "remote data")
	}))
	t.Cleanup(srv.Close)
	url := srv.URL + "/repo.git//data"
	const gitURL = "file:///example.invalid/owner/repo.git"
	remote := memoryTree(
		t,
		map[string]string{
			"kustomization.yaml": "configMapGenerator: [{name: nested, files: ['chosen=" + url + "']}]\n",
		},
	)
	c := newTestTreeCache(t, "")
	gitCalls := 0
	c.SetGitBaseFetcher(
		func(_ context.Context, request GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			gitCalls++
			{
				assert.FatalIf(t, request.URL != gitURL || request.Ref != "main", "Git request=%+v", request)
			}
			return &store.SourceArtifact{
				FS:       remote,
				Key:      "family",
				Content:  "family-generation",
				Revision: "revision",
			}, nil
		},
	)
	rec := newReadSet("/src")
	input := newTreeFS(
		memoryTree(
			t,
			map[string]string{
				"kustomization.yaml": "resources: ['" + gitURL + "?ref=main']\nconfigMapGenerator: [{name: root, files: ['chosen=" + url + "', 'other=" + url + "']}]\n",
			},
		),
		"/src",
		rec,
	)
	err := preflightRemoteResources(t.Context(), c, input, "/src")
	{
		assert.FatalIf(t, err != nil, "preflight error=%v", err)
	}
	snap := rec.snapshot()
	assert.FatalIf(
		t,
		len(snap.Remotes) != 2 || snap.Remotes[0].Input != (remoteInput{Operation: remoteHTTP, URL: url}) ||
			snap.Remotes[1].Input != (remoteInput{Operation: remoteGit, URL: gitURL, Ref: "main", Timeout: 27 * time.Second, Submodules: true}),
		"remote observations=%+v",
		snap.Remotes,
	)
	wantParents := []remoteInput{
		{Operation: remoteGit, URL: gitURL, Ref: "main", Timeout: 27 * time.Second, Submodules: true},
	}
	if diff := cmp.Diff(wantParents, snap.Remotes[0].Parents); diff != "" || len(snap.Remotes[1].Parents) != 0 {
		t.Fatalf(
			"root sighting erased nested parents (-want +got):\n%s; Git parents=%+v",
			diff,
			snap.Remotes[1].Parents,
		)
	}
	frame, err := encodeFrame(&renderHeader{Reads: snap}, nil)
	{
		assert.NoError(t, err)
	}
	decodedFrame, err := decodeFrame(frame)
	decoded := decodedFrame.header.Reads
	{
		assert.NoError(t, err)
	}
	for entryIndex := range decoded.Remotes {
		entry := decoded.Remotes[entryIndex]
		acquired, identity, err := entry.Input.acquire(t.Context(), c, input)
		{
			assert.FatalIf(
				t,
				err != nil || identity != entry.Identity,
				"replayed identity=%+v observation=%+v error=%v",
				identity,
				entry,
				err,
			)
		}
		if entry.Input.Operation == remoteHTTP {
			body, err := fs.ReadFile(acquired, "content")
			{
				assert.FatalIf(t, err != nil || string(body) != "remote data", "HTTP body=%q error=%v", body, err)
			}
		}
	}
	if hits.Load() != 1 || gitCalls != 2 {
		t.Fatalf("HTTP requests=%d Git acquisitions=%d", hits.Load(), gitCalls)
	}
}

func testPreflightRemoteResourcesRemoteObservationsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	var mu sync.Mutex
	calls := make(map[string]int)
	record := func(url string) {
		mu.Lock()
		defer mu.Unlock()
		calls[url]++
	}
	var changed atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record(srv.URL + r.URL.RequestURI())
		if strings.HasPrefix(r.URL.Path, "/probe.git") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body := "remote data"
		if strings.HasPrefix(r.URL.Path, "/plugin-") {
			body = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: ['chosen=" + srv.URL + "/a-child']\n"
			if changed.Load() == 2 && r.URL.Path == "/plugin-one" ||
				changed.Load() == 3 && r.URL.Path == "/plugin-two" {
				body += "behavior: merge\n"
			}
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	a := remoteInput{
		Operation:  remoteGit,
		URL:        "file:///example.invalid/a-parent.git",
		Ref:        "main",
		Timeout:    27 * time.Second,
		Submodules: true,
	}
	b := remoteInput{
		Operation:  remoteGit,
		URL:        "file:///example.invalid/z-parent.git",
		Ref:        "main",
		Timeout:    27 * time.Second,
		Submodules: true,
	}
	shared := remoteInput{
		Operation:  remoteGit,
		URL:        "file:///example.invalid/m-shared.git",
		Ref:        "main",
		Timeout:    27 * time.Second,
		Submodules: true,
	}
	child := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/a-child"}
	probe := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/probe.git//subdir?ref=main"}
	selected := remoteInput{
		Operation:  remoteGit,
		URL:        srv.URL + "/probe.git",
		Ref:        "main",
		Timeout:    27 * time.Second,
		Submodules: true,
	}
	plugin := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/plugin-one"}
	otherPlugin := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/plugin-two"}
	leaf := "configMapGenerator: [{name: nested, files: ['chosen=" + child.URL + "']}]\n"
	root := "resources: ['" + a.URL + "//subdir?ref=main']\n"
	repositories := map[string]map[string]string{}
	want := map[remoteInput][]remoteInput{a: nil}
	switch *testCaseInput {
	case "nested-git":
		repositories[a.URL] = map[string]string{
			"subdir/kustomization.yaml": "resources: ['" + b.URL + "//subdir?ref=main']\n",
		}
		repositories[b.URL] = map[string]string{"subdir/kustomization.yaml": leaf}
		want[b], want[child] = []remoteInput{a}, []remoteInput{a, b}
	case "http-plugin":
		repositories[a.URL] = map[string]string{
			"subdir/kustomization.yaml": "generators: ['" + plugin.URL + "']\n",
		}
		want[plugin], want[child] = []remoteInput{a}, []remoteInput{a, plugin}
	case "shared-mount":
		root = "resources: ['" + a.URL + "//subdir?ref=main', '" + b.URL + "//subdir?ref=main']\n"
		for _, parent := range []remoteInput{a, b} {
			repositories[parent.URL] = map[string]string{
				"subdir/kustomization.yaml": "resources: ['" + shared.URL + "//subdir?ref=main']\n",
			}
		}
		repositories[shared.URL] = map[string]string{
			"subdir/kustomization.yaml": "resources: [../inner]\n",
			"inner/kustomization.yaml":  "resources: ['" + probe.URL + "']\n",
		}
		repositories[selected.URL] = map[string]string{"subdir/kustomization.yaml": leaf}
		want[b], want[shared] = nil, []remoteInput{a, b}
		want[probe], want[selected] = []remoteInput{a, b, shared}, []remoteInput{a, b, shared}
		want[child] = []remoteInput{a, b, shared, selected}
	case "shared-plugin":
		root = "generators: ['" + a.URL + "//plugins?ref=main', '" + b.URL + "//plugins?ref=main']\n"
		for _, parent := range []remoteInput{a, b} {
			repositories[parent.URL] = map[string]string{
				"plugins/kustomization.yaml": "resources: ['" + plugin.URL + "']\n",
			}
		}
		want[b], want[plugin], want[child] = nil, []remoteInput{a, b}, []remoteInput{a, b, plugin}
	case "equal-content-plugins":
		root = "generators: ['" + plugin.URL + "', '" + otherPlugin.URL + "']\n"
		want = map[remoteInput][]remoteInput{plugin: nil, otherPlugin: nil, child: {plugin, otherPlugin}}
	case "same-repository-subpaths":
		repositories[a.URL] = map[string]string{
			"subdir/kustomization.yaml": "resources: ['" + a.URL + "//leaf?ref=main']\n",
			"leaf/kustomization.yaml":   leaf,
		}
		want[child] = []remoteInput{a}
	}
	newCache := func() *TreeCache {
		c := newTestTreeCache(t, "")
		c.SetGitBaseFetcher(
			func(_ context.Context, request GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				record(request.URL)
				files, ok := repositories[request.URL]
				{
					assert.FatalIf(t, !ok || request.Ref != "main", "Git request=%+v", request)
				}
				revision := "1"
				if changed.Load() == 1 && request.URL == b.URL {
					revision = "2"
				}
				return &store.SourceArtifact{
					FS:       memoryTree(t, files),
					Key:      request.URL,
					Content:  revision,
					Revision: revision,
				}, nil
			},
		)
		return c
	}
	c := newCache()
	rec := newReadSet("/src")
	input := newTreeFS(memoryTree(t, map[string]string{"kustomization.yaml": root}), "/src", rec)
	{
		err := preflightRemoteResources(t.Context(), c, input, "/src")
		assert.NoError(t, err)
	}
	snap := rec.snapshot()
	assert.FatalIf(t, len(snap.Remotes) != len(want), "observations=%+v; want=%+v", snap.Remotes, want)
	for entryIndex := range snap.Remotes {
		entry := snap.Remotes[entryIndex]
		assert.FatalIf(
			t,
			entry.Input == probe && entry.Identity != (remoteIdentity{HTTPStatus: http.StatusNotFound}),
			"nested probe identity=%+v",
			entry.Identity,
		)
		parents, ok := want[entry.Input]
		slices.SortFunc(parents, compareRemoteInputs)
		assert.FatalIf(
			t,
			!ok || !cmp.Equal(parents, entry.Parents),
			"parents of %+v (-want +got):\n%s",
			entry.Input,
			cmp.Diff(parents, entry.Parents),
		)
	}
	mu.Lock()
	for descriptor := range want {
		if calls[descriptor.URL] != 1 {
			t.Errorf("capture acquisitions for %s=%d, want 1", descriptor.URL, calls[descriptor.URL])
		}
	}
	mu.Unlock()
	frame, err := encodeFrame(&renderHeader{Reads: snap}, nil)
	{
		assert.NoError(t, err)
	}
	snapFrame, decodeErr := decodeFrame(frame)
	snap, err = snapFrame.header.Reads, decodeErr
	{
		assert.NoError(t, err)
	}
	closeTestTreeCache(t, c)
	mu.Lock()
	clear(calls)
	mu.Unlock()
	c = newCache()
	{
		valid, err := snap.stillValid(t.Context(), c, input, true)
		assert.FatalIf(t, err != nil || !valid, "unchanged graph: valid=%t error=%v", valid, err)
	}
	mu.Lock()
	testPreflightRemoteResourcesRemoteObservationsCaseLoop(t, struct {
		want  *map[remoteInput][]remoteInput
		calls *map[string]int
	}{want: &want, calls: &calls})
	mu.Unlock()
	closeTestTreeCache(t, c)
	var changes []int32
	switch *testCaseInput {
	case "shared-mount", "shared-plugin":
		changes = []int32{1}
	case "equal-content-plugins":
		changes = []int32{2, 3}
	}
	for _, change := range changes {
		changed.Store(change)
		mu.Lock()
		clear(calls)
		mu.Unlock()
		c = newCache()
		{
			valid, err := snap.stillValid(t.Context(), c, input, true)
			assert.FatalIf(t, err != nil || valid, "changed parent %d: valid=%t error=%v", change, valid, err)
		}
		mu.Lock()
		{
			assert.ErrorIf(t, calls[child.URL] != 0 || (*testCaseInput) ==
				"shared-mount" &&
				(calls[shared.URL] != 0 || calls[probe.URL] != 0 || calls[selected.URL] != 0), "changed parent %d acquired descendants: %v", change, calls)
		}
		mu.Unlock()
		closeTestTreeCache(t, c)
	}
}

func testPreflightRemoteResourcesRemoteObservationsCaseLoop(t *testing.T, loopInputs struct {
	want  *map[remoteInput][]remoteInput
	calls *map[string]int
},
) {
	t.Helper()

	for descriptor := range *loopInputs.want {
		if (*loopInputs.calls)[descriptor.URL] != 1 {
			t.Errorf("replay acquisitions for %s=%d, want 1", descriptor.URL, (*loopInputs.calls)[descriptor.URL])
		}
	}
}
