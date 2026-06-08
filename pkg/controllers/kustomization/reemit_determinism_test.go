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

// TestReconcile_ParentReemitChurn_DependentStaysReady is the integration
// regression for the re-emission-churn determinism bug. A dependent that
// waits (quiescence-bound) on a parent Kustomization must reach Ready even
// while that parent is being re-emitted by churn (a stamped, no-op
// re-AddObject — exactly what a structural grandparent's dedup-skip replay
// did before this fix).
//
// Pre-fix: the parent's coalesced re-run transiently flipped Ready→Pending
// at the top of reconcile (controller.go:142), and the dependent's
// quiescence-bound depwait, re-reading at a transient pool drain, gave up
// with "parent Kustomization not ready" — nondeterministically dropping the
// dependent's rendered resources. Post-fix (A: a no-op re-run of an
// already-Ready KS doesn't downgrade; B: the dedup-skip path no longer
// re-publishes children, so the churn stops) the parent stays Ready and the
// dependent renders on every iteration.
func TestReconcile_ParentReemitChurn_DependentStaysReady(t *testing.T) {
	for i := range 30 {
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
		bootstrap := &manifest.GitRepository{
			Name: "flux-system", Namespace: "flux-system",
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + root},
		}
		s.AddObject(bootstrap)
		s.SetArtifact(bootstrap.Named(), &store.SourceArtifact{
			Kind: manifest.KindGitRepository, URL: "file://" + root, LocalPath: root,
		})
		s.UpdateStatus(bootstrap.Named(), store.StatusReady, "ready")

		tasks := task.New()
		c := New(s, tasks, kustomize.NewTreeCache(), true)
		c.Configure(Options{Renders: tasksRenderInflight{tasks}})
		c.Start(context.Background())

		mkKS := func(name string, deps []manifest.DependencyRef) *manifest.Kustomization {
			return &manifest.Kustomization{
				Name: name, Namespace: "flux-system",
				KustomizationSpec: kustomizev1.KustomizationSpec{
					Path:    "./apps",
					Timeout: &metav1.Duration{Duration: 50 * time.Millisecond},
					SourceRef: kustomizev1.CrossNamespaceSourceReference{
						Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
					},
				},
				SourceKind: manifest.KindGitRepository, SourceName: "flux-system", SourceNamespace: "flux-system",
				DependsOn: deps,
				Contents:  map[string]any{},
			}
		}

		parent := mkKS("parent", nil)
		s.AddObject(parent)
		testutil.WaitForStatus(t, s, parent.Named(), store.StatusReady)

		// Churn the settled parent with stamped, no-op re-emits (the
		// AddObject DeepEqual gate fails on the label delta, re-firing a
		// coalesced re-run that dedup-skips) while a dependent waits on it
		// with a short timeout. The dependent's wait is quiescence-bound
		// (Renders wired above), the same path the production HR/KS gate uses.
		dep := mkKS("dep", []manifest.DependencyRef{{NamedResource: parent.Named()}})
		for j := range 3 {
			stamped := parent.Clone()
			stamped.Labels = map[string]string{
				"kustomize.toolkit.fluxcd.io/name":      "grandparent",
				"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
				"churn":                                 string(rune('a' + j)),
			}
			s.AddObject(stamped)
		}
		s.AddObject(dep)

		info := testutil.WaitForStatus(t, s, dep.Named(), store.StatusReady)
		if info.Status != store.StatusReady {
			t.Fatalf("iter %d: dependent KS = %v (%q); want Ready (parent stays Ready across re-emit churn)", i, info.Status, info.Message)
		}

		c.Close()
		tasks.BlockTillDone()
	}
}
