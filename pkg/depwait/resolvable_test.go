package depwait

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// AllPresentReconcilable gates base.Await's YieldSlot-vs-YieldQuiescent choice:
// true only when every dep is a present, CEL-free Kustomization/HelmRelease (a
// reconcilable, multi-hop producer — the transient-drain victim that must hold
// active). A source-kind dep (single-hop producer), an absent dep, or a
// ReadyExpr dep must keep the quiescence give-up.
func TestAllPresentReconcilable(t *testing.T) {
	s := store.New()

	presentHR := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "hr"}
	s.UpdateStatus(presentHR, store.StatusPending, "rendering")
	presentKS := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "ks"}
	s.UpdateStatus(presentKS, store.StatusPending, "rendering")

	absent := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "ghost"}

	// A present source-kind dep: single-hop producer → excluded (stays on the
	// quiescence give-up so the post-Failed fast-fail short-circuit survives).
	presentSrc := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "src"}
	s.UpdateStatus(presentSrc, store.StatusPending, "fetching")

	w := &Waiter{Store: s}
	ref := func(id manifest.NamedResource) manifest.DependencyRef {
		return manifest.DependencyRef{NamedResource: id}
	}

	cases := []struct {
		name string
		deps []manifest.DependencyRef
		want bool
	}{
		{"present HR", []manifest.DependencyRef{ref(presentHR)}, true},
		{"present KS", []manifest.DependencyRef{ref(presentKS)}, true},
		{"present HR + KS", []manifest.DependencyRef{ref(presentHR), ref(presentKS)}, true},
		{"present source kind", []manifest.DependencyRef{ref(presentSrc)}, false},
		{"absent", []manifest.DependencyRef{ref(absent)}, false},
		{"present HR with ReadyExpr", []manifest.DependencyRef{{NamedResource: presentHR, ReadyExpr: "true"}}, false},
		{"mixed present HR + absent", []manifest.DependencyRef{ref(presentHR), ref(absent)}, false},
		{"mixed present HR + source", []manifest.DependencyRef{ref(presentHR), ref(presentSrc)}, false},
		{"empty", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := w.AllPresentReconcilable(tc.deps); got != tc.want {
				t.Errorf("AllPresentReconcilable = %v, want %v", got, tc.want)
			}
		})
	}
}
