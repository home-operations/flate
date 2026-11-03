package loader

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestFinalizeIndex_RemoteInputs(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing-owner", "remote-failure", "local-without-owner", "local-closed-owner", "local-file-without-owner", "local-file-closed-owner", "literal-without-owner", "literal-closed-owner", "canceled", "deadline", "canceled-empty", "deadline-empty"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexRemoteInputsCase(t, &mode)
		},
		)
	}
}

func TestFinalizeIndex_RemoteCancellation(t *testing.T) {
	t.Parallel()
	entered, canceled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		fmt.Sprintf("configMapGenerator:\n- name: remote\n  envs: [%s]\n", server.URL),
	)
	st := store.New()
	l := loaderFixture(st, nil)
	l.documents = readerForTest(root)
	admitForTest(st, root)
	l.mountSources()
	{
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
	}
	network := new(source.Network)
	defer network.Close()
	inputs := source.NewHTTPInputs(cachetest.New(t, "", cache.New), network, task.NewBounded(2))
	defer inputs.Close()
	l.Options.RemoteInputs = &inputs
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.FinalizeIndex(ctx, root, true) }()
	<-entered
	cancel()
	{
		err := <-done
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "remote cancellation: %v", err)
	}
	select {
	case <-canceled:
		t.Error("waiter cancellation stopped shared acquisition")
	default:
	}
	inputs.Close()
	<-canceled
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "remote"}
	assert.Equal(t, len(st.Productions(id, nil)), 0)
}

func testFinalizeIndexRemoteInputsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	var requests atomic.Int32
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); http.NotFound(w, r) }),
	)
	defer server.Close()
	root := t.TempDir()
	input := fmt.Sprintf("envs: [%s]", server.URL)
	if (*testCaseInput) == "local-without-owner" || (*testCaseInput) == "local-closed-owner" {
		input = "envs: [../input.env]"
	}
	if strings.HasPrefix((*testCaseInput), "local-file-") {
		input = "files: [VALUE=../input.txt]"
	}
	if strings.HasPrefix((*testCaseInput), "literal-") {
		input = "literals: [VALUE=local]"
	}
	testutil.WriteFile(t, root, "apps/input.env", "VALUE=local\n")
	testutil.WriteFile(t, root, "apps/input.txt", "local\n")
	control := "configMapGenerator:\n- name: input\n  namespace: apps\n  " + input + "\n- name: sibling\n  namespace: apps\n  literals: [VALUE=sibling]\n"
	if strings.HasSuffix((*testCaseInput), "-empty") {
		control = "resources: []\n"
	}
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", control)
	st := store.New()
	ks := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps/a"}
	st.AddObject(ks)
	l := loaderFixture(st, nil)
	l.documents = readerForTest(root)
	admitForTest(st, root)
	l.mountSources()
	l.documents.read(filepath.Join(root, "apps/a"))
	if (*testCaseInput) == "remote-failure" || strings.HasSuffix((*testCaseInput), "-closed-owner") {
		network := new(source.Network)
		defer network.Close()
		inputs := source.NewHTTPInputs(cachetest.New(t, "", cache.New), network, task.NewBounded(2))
		defer inputs.Close()
		l.Options.RemoteInputs = &inputs
		if (*testCaseInput) != "remote-failure" {
			inputs.Close()
		}
	}
	ctx := t.Context()
	var wantErr error
	if (*testCaseInput) == "canceled" || (*testCaseInput) == "canceled-empty" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
		wantErr = context.Canceled
	}
	if strings.HasPrefix((*testCaseInput), "deadline") {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Time{})
		defer cancel()
		wantErr = context.DeadlineExceeded
	}
	if (*testCaseInput) == "missing-owner" {
		wantErr = manifest.ErrInput
	}
	err := l.FinalizeIndex(ctx, root, true)
	{
		assert.FatalIf(t, !errors.Is(err, wantErr), "finalization: %v, want %v", err, wantErr)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	if wantErr != nil {
		assert.Equal(t, requests.Load(), int32(0))
		assert.Equal(t, len(st.Productions(id, nil)), 0)
		if (*testCaseInput) == "missing-owner" {
			assert.Equal(t, l.scopes != nil, true)
		}
		return
	}
	obj, ok := lookupDataForTest[*manifest.ConfigMap](t, st, id)
	if (*testCaseInput) == "remote-failure" {
		assert.Equal(t, requests.Load(), int32(1))
		assert.Equal(t, ok, false)
		assert.Diff(t, testutil.Owners(l.Index.Records[id].Productions), []manifest.NamedResource{ks.Named()})
	} else {
		assert.Equal(t, requests.Load(), int32(0))
		{
			assert.FatalIf(t, !ok, "%s", "local generator missing")
		}
		want := "local"
		if strings.HasPrefix((*testCaseInput), "local-file-") {
			want += "\n"
		}
		assert.Diff(t, obj.Data, map[string]any{"VALUE": want})
	}
	id.Name = "sibling"
	sibling, ok := lookupDataForTest[*manifest.ConfigMap](t, st, id)
	{
		assert.FatalIf(t, !ok, "%s", "ordinary input error stopped sibling materialization")
	}
	assert.Diff(t, sibling.Data, map[string]any{"VALUE": "sibling"})
}
