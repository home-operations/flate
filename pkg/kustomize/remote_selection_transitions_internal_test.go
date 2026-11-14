package kustomize

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

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
	assert.FatalIf(
		t,
		err != nil || receipt == nil || !hasRenderField(out, "name", "git-output"),
		"initial directory selection: %s, %v",
		out,
		err,
	)
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
	eligible := sourcecache.RenderPersistenceDisabledReason() == ""
	var persisted readSetSnapshot
	assertRemoteSelectionObservations(t, &remoteSelectionObservationInputs{
		scenario: &testCaseInput, cache: &c, key: &key,
		eligible: &eligible, encoded: &realOutput, snapshot: &snap,
		persisted: &persisted, url: &url, hits: &hits,
	})
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
	assert.FatalIf(
		t,
		err != nil || !bytes.Equal(realOutput, testRenderEncoding(t, out)),
		"unchanged real output: %s, %v",
		out,
		err,
	)
	if eligible {
		publishTestFrame(
			t,
			c.owner,
			sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
			testRenderFrame(t, new(persisted), testRenderPayload(t, "REMOTE-SELECTION")),
		)
	}
	closeTestTreeCache(t, c)
	c = newCache()
	out, _, err = testutil.RenderMaps(Render(t.Context(), c, request, sourcecache.RetryConfig{}))
	witness := hasRenderField(out, "witness", "REMOTE-SELECTION")
	assert.FatalIf(
		t,
		err != nil || witness != eligible ||
			!eligible && !bytes.Equal(realOutput, testRenderEncoding(t, out)),
		"saved-render reuse: %s, %v; eligible=%t",
		out,
		err,
		eligible,
	)
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
	assert.FatalIf(
		t,
		err != nil || !hasRenderField(out, "name", want) || hasRenderField(out, "name", "git-output") ||
			hasRenderField(out, "witness", "REMOTE-SELECTION"),
		"resource transition: %s, %v",
		out,
		err,
	)
	if hits.Load() != 4 {
		t.Fatalf("validation/preflight acquisitions=%d, want one per invocation", hits.Load())
	}
}

type remoteSelectionObservationInputs struct {
	scenario **struct {
		name          string
		initialStatus int
		changeGit     bool
	}
	cache     **TreeCache
	key       *string
	eligible  *bool
	encoded   *[]byte
	snapshot  *readSetSnapshot
	persisted *readSetSnapshot
	url       *string
	hits      *atomic.Int32
}

func assertRemoteSelectionObservations(t *testing.T, inputs *remoteSelectionObservationInputs) {
	t.Helper()

	accepted, _, _, acceptedErr := (*inputs.cache).owner.Blob(
		t.Context(),
		sourcecache.BlobKey{Content: *inputs.key, Render: sourcecache.RenderAccepted},
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.Equal(t, accepted != nil, *inputs.eligible)
	if *inputs.eligible {
		assert.NoError(t, acceptedErr)
	} else {
		assert.Equal(t, errors.Is(acceptedErr, errdef.ErrNotFound), true)
	}
	var saved []map[string]any
	var hit bool
	(*inputs.persisted), saved, hit = readTestRenderFrame(t, (*inputs.cache), (*inputs.key))
	assert.FatalIf(t, !hit ||
		hit &&
			(!reflect.DeepEqual((*inputs.persisted), (*inputs.snapshot)) || !bytes.Equal((*inputs.encoded), testRenderEncoding(t, saved))), "persisted=%t eligible=%t observations=%+v output=%s", hit, (*inputs.eligible), inputs.persisted.Remotes, saved)
	t.Logf("persistence eligible=%t persisted hit=%t", (*inputs.eligible), hit)
	if hit {
		(*inputs.snapshot) = (*inputs.persisted)
	}
	assert.FatalIf(
		t,
		len(inputs.snapshot.Remotes) != 2 || inputs.snapshot.Remotes[0].Input != (remoteInput{Operation: remoteHTTP, URL: (*inputs.url)}) ||
			inputs.snapshot.Remotes[1].Input.Operation != remoteGit ||
			inputs.snapshot.Remotes[1].Identity != (remoteIdentity{Present: true, Readable: true, Key: "repository", Content: "git-output"}),
		"selection observations=%+v",
		inputs.snapshot.Remotes,
	)
	if (*inputs.scenario).initialStatus == http.StatusNotFound {
		if inputs.snapshot.Remotes[0].Identity != (remoteIdentity{HTTPStatus: http.StatusNotFound}) {
			t.Fatalf("HTTP probe identity=%+v", inputs.snapshot.Remotes[0].Identity)
		}
	} else if inputs.snapshot.Remotes[0].Identity.Content == "" || inputs.snapshot.Remotes[0].Identity.HTTPStatus != 0 {
		t.Fatalf("HTTP content identity=%+v", inputs.snapshot.Remotes[0].Identity)
	}
	if inputs.hits.Load() != 1 {
		t.Fatalf("one invocation probed %d times", inputs.hits.Load())
	}
}
