package kustomization

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestController_RemoteValidationSlots(t *testing.T) {
	t.Parallel()
	c, s, root := newControllerWithFixture(t)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		entered <- struct{}{}
		<-release
		_, _ = fmt.Fprintf(w, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote-%s}\n", r.URL.Path[1:])
	}))
	t.Cleanup(srv.Close)
	ready, begin := make(chan struct{}, 2), make(chan struct{})
	var ids []manifest.NamedResource
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(t, root, name+"/kustomization.yaml", "resources: ['"+srv.URL+"/"+name+"']\n")
		ks := &manifest.Kustomization{
			Name: name, Namespace: "flux-system", Path: name,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
			},
		}
		s.AddObject(ks)
		ids = append(ids, ks.Named())
		c.Tasks.Go(t.Context(), name, func(ctx context.Context) {
			ready <- struct{}{}
			<-begin
			if blocked := c.ReconcileNode(ctx, ks.Named(), manifest.ReconcileAttempt{Drain: manifest.DrainNone}); !blocked.Empty() {
				t.Errorf("unexpected blocked dependencies: %v", blocked)
			}
		})
	}
	<-ready
	<-ready
	close(begin)
	<-entered
	<-entered
	unblock()
	c.Tasks.BlockTillDone()
	assert.Equal(t, c.Tasks.Failures(), int64(0))
	assert.Equal(t, requests.Load(), int32(2))
	for _, id := range ids {
		status, _ := s.GetStatus(id)
		assert.Equal(t, status.Status, store.StatusReady)
		_, artifact := s.GetEmission(id)
		if len(artifact.Manifests) != 1 {
			t.Fatalf("missing rendered artifact for %s", id)
		}
		name, _ := manifest.DocMetadata(artifact.Manifests[0])
		assert.Equal(t, name, "remote-"+id.Name)
		assert.Equal(t, manifest.DocKind(artifact.Manifests[0]), "ConfigMap")
	}
}
