package kustomization

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// tasksRenderInflight mirrors the orchestrator's orchestratorRenderInflight:
// QuiescenceCh(0), since depwait callers reach it from inside YieldQuiescent.
type tasksRenderInflight struct{ ts *task.Service }

func (r tasksRenderInflight) QuiescenceCh() <-chan struct{} { return r.ts.QuiescenceCh(0) }

// TestReconcile_SourceRefParksOnFetch is the global-determinism regression for
// the Kustomization sourceRef path (the analogue of the HR chart-source bug).
// A KS whose GitRepository sourceRef is still being fetched (present+Pending)
// when its short spec.timeout would expire must NOT be skipped: the reconcile
// parks on the fetch's completion (orchestrator quiescence) and renders once
// the source goes Ready. Before the fix the KS rode the 50ms wall clock,
// resolveSource saw no artifact, and the KS Failed — dropping its rendered
// resources nondeterministically.
func TestReconcile_SourceRefParksOnFetch(t *testing.T) {
	for i := range 10 {
		root := t.TempDir()
		testutil.WriteFileAt(t, filepath.Join(root, "apps", "kustomization.yaml"),
			"resources:\n- cm.yaml\n")
		testutil.WriteFileAt(t, filepath.Join(root, "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: default}
data: {greeting: hi}
`)

		s := store.New()
		// The KS's sourceRef: present but PENDING (fetch in flight), no artifact
		// yet — stands in for a slow GitRepository fetch dispatched by the source
		// controller.
		src := &manifest.GitRepository{
			Name: "apps-src", Namespace: "flux-system",
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + root},
		}
		s.AddObject(src)
		s.UpdateStatus(src.Named(), store.StatusPending, "fetching")

		tasks := task.New()
		c := New(s, tasks, kustomize.NewTreeCache(), true)
		c.Configure(Options{Renders: tasksRenderInflight{tasks}})
		c.Start(context.Background())

		// The fetch completes only at 200ms — far past the KS's 50ms timeout.
		// A plain task body holds the pool active until it returns (Ready set
		// before decrActive), exactly like the source controller's YieldSlot.
		srcID := src.Named()
		tasks.Go(context.Background(), "fake-git-fetch", func(context.Context) {
			time.Sleep(200 * time.Millisecond)
			s.SetArtifact(srcID, &store.SourceArtifact{
				Kind: manifest.KindGitRepository, URL: "file://" + root, LocalPath: root,
			})
			s.UpdateStatus(srcID, store.StatusReady, "ready")
		})

		ks := &manifest.Kustomization{
			Name: "apps", Namespace: "flux-system",
			KustomizationSpec: kustomizev1.KustomizationSpec{
				Path:    "./apps",
				Timeout: &metav1.Duration{Duration: 50 * time.Millisecond},
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository, Name: "apps-src", Namespace: "flux-system",
				},
			},
			SourceKind: manifest.KindGitRepository, SourceName: "apps-src", SourceNamespace: "flux-system",
			Contents: map[string]any{},
		}
		s.AddObject(ks)

		// Fix → parks until the source is Ready (200ms) → renders → Ready.
		// Broken → rode the 50ms clock → resolveSource saw no artifact → Failed,
		// and this WaitForStatus(Ready) would time out.
		info := testutil.WaitForStatus(t, s, ks.Named(), store.StatusReady)
		if info.Status != store.StatusReady {
			t.Fatalf("iter %d: KS status = %v (%q); want Ready (parked for the in-flight sourceRef fetch)", i, info.Status, info.Message)
		}

		c.Close()
		tasks.BlockTillDone()
	}
}
