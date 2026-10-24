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
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); http.NotFound(w, r) }))
			defer server.Close()
			root := t.TempDir()
			input := fmt.Sprintf("envs: [%s]", server.URL)
			if mode == "local-without-owner" || mode == "local-closed-owner" {
				input = "envs: [../input.env]"
			}
			if strings.HasPrefix(mode, "local-file-") {
				input = "files: [VALUE=../input.txt]"
			}
			if strings.HasPrefix(mode, "literal-") {
				input = "literals: [VALUE=local]"
			}
			testutil.WriteFile(t, root, "apps/input.env", "VALUE=local\n")
			testutil.WriteFile(t, root, "apps/input.txt", "local\n")
			control := "configMapGenerator:\n- name: input\n  namespace: apps\n  " + input + "\n- name: sibling\n  namespace: apps\n  literals: [VALUE=sibling]\n"
			if strings.HasSuffix(mode, "-empty") {
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
			if mode == "remote-failure" || strings.HasSuffix(mode, "-closed-owner") {
				network := new(source.Network)
				defer network.Close()
				inputs := source.NewHTTPInputs(cachetest.New(t, "", cache.New), network, task.NewBounded(2))
				defer inputs.Close()
				l.Options.RemoteInputs = &inputs
				if mode != "remote-failure" {
					inputs.Close()
				}
			}
			ctx := t.Context()
			var wantErr error
			if mode == "canceled" || mode == "canceled-empty" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				wantErr = context.Canceled
			}
			if strings.HasPrefix(mode, "deadline") {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Time{})
				defer cancel()
				wantErr = context.DeadlineExceeded
			}
			if mode == "missing-owner" {
				wantErr = manifest.ErrInput
			}
			err := l.FinalizeIndex(ctx, root)
			if !errors.Is(err, wantErr) {
				t.Fatalf("finalization: %v, want %v", err, wantErr)
			}
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
			if wantErr != nil {
				assert.Equal(t, requests.Load(), int32(0))
				assert.Equal(t, len(st.Productions(id, nil)), 0)
				if mode == "missing-owner" {
					assert.Equal(t, l.scopes != nil, true)
				}
				return
			}
			obj, ok := st.Get[*manifest.ConfigMap](id)
			if mode == "remote-failure" {
				assert.Equal(t, requests.Load(), int32(1))
				assert.Equal(t, ok, false)
				assert.Diff(t, testutil.ProductionOwners(l.Index.Records[id]), []manifest.NamedResource{ks.Named()})
			} else {
				assert.Equal(t, requests.Load(), int32(0))
				if !ok {
					t.Fatal("local generator missing")
				}
				want := "local"
				if strings.HasPrefix(mode, "local-file-") {
					want += "\n"
				}
				assert.Diff(t, obj.Data, map[string]any{"VALUE": want})
			}
			id.Name = "sibling"
			sibling, ok := st.Get[*manifest.ConfigMap](id)
			if !ok {
				t.Fatal("ordinary input error stopped sibling materialization")
			}
			assert.Diff(t, sibling.Data, map[string]any{"VALUE": "sibling"})
		})
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
	testutil.WriteFile(t, root, "kustomization.yaml", fmt.Sprintf("configMapGenerator:\n- name: remote\n  envs: [%s]\n", server.URL))
	st := store.New()
	l := loaderFixture(st, nil)
	l.documents = readerForTest(root)
	admitForTest(st, root)
	l.mountSources()
	if _, err := l.Load(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	network := new(source.Network)
	defer network.Close()
	inputs := source.NewHTTPInputs(cachetest.New(t, "", cache.New), network, task.NewBounded(2))
	defer inputs.Close()
	l.Options.RemoteInputs = &inputs
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.FinalizeIndex(ctx, root) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("remote cancellation: %v", err)
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
