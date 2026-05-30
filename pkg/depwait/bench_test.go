package depwait

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// BenchmarkWatchReady_ManyDeps measures Watch + WaitAll against a
// 50-dep set where every dep is already Ready in the store. This is
// the fast path the orchestrator hits when re-emitted parents drive
// children whose deps already landed; the wait should never block.
func BenchmarkWatchReady_ManyDeps(b *testing.B) {
	s := store.New()
	ids := make([]manifest.NamedResource, 0, 50)
	for i := range 50 {
		id := manifest.NamedResource{
			Kind: manifest.KindGitRepository, Namespace: "ns",
			Name: fmt.Sprintf("src-%d", i),
		}
		s.AddObject(&manifest.GitRepository{Name: id.Name, Namespace: id.Namespace})
		s.UpdateStatus(id, store.StatusReady, "")
		ids = append(ids, id)
	}
	deps := testutil.DepRefs(ids...)
	w := &Waiter{Store: s, Timeout: 5 * time.Second}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sum := WaitAll(w.Watch(ctx, deps))
		if sum.AnyFailed() {
			b.Fatalf("expected all ready: %+v", sum)
		}
	}
}

// BenchmarkCEL_Evaluate measures evaluateReadyExpr after the program
// is in the celCache — the per-fire cost the depwait CEL path pays on
// every store event. Uses a representative Flux-idiom expression that
// reads dep.status.conditions and dep.metadata.labels.
func BenchmarkCEL_Evaluate(b *testing.B) {
	s := store.New()
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "consumer"}
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "producer"}
	// Seed the dep with a Ready=True condition + a label so the CEL
	// has substrate to evaluate.
	s.AddObject(&manifest.Kustomization{Name: dep.Name, Namespace: dep.Namespace, Labels: map[string]string{
		"app.kubernetes.io/component": "cache",
	}})
	s.UpdateStatus(dep, store.StatusReady, "")
	expr := `dep.metadata.labels['app.kubernetes.io/component'] == 'cache' && ` +
		`dep.status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')`

	// Pre-compile so the bench measures the eval path, not compile.
	if _, err := compileReadyExpr(expr); err != nil {
		b.Fatalf("compile: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		ok, err := evaluateReadyExpr(expr, s, parent, dep)
		if err != nil {
			b.Fatalf("evaluate: %v", err)
		}
		if !ok {
			b.Fatalf("expected true")
		}
	}
}
