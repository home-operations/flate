package loader

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPromote_AdmissionErrors(t *testing.T) {
	t.Parallel()
	for _, site := range []string{"existing", "child", "final", "cancel"} {
		t.Run(site, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			first := &manifest.ConfigMap{Name: "first", Namespace: "apps"}
			target := &manifest.ConfigMap{Name: "target", Namespace: "apps"}
			last := &manifest.ConfigMap{Name: "last", Namespace: "apps"}
			docs := []store.Child{{Obj: first}, {Obj: target}, {Obj: last}}
			if site == "existing" {
				st.AddObject(target)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cause := fmt.Errorf("%w: exact admission failed", manifest.ErrInput)
			calls := 0
			admit := func(obj manifest.BaseManifest) (bool, error) {
				if obj.Named() != target.Named() {
					return true, nil
				}
				calls++
				if site == "cancel" {
					cancel()
					return true, nil
				}
				if site != "final" || calls == 2 {
					return false, cause
				}
				return true, nil
			}
			got, err := Promote(ctx, st, target.Named(), PromoteOptions{Documents: docs, Admit: admit})
			want := cause
			if site == "cancel" {
				want = context.Canceled
			}
			if got || !errors.Is(err, want) {
				t.Fatalf("Promote = %v, %v, want false, %v", got, err, want)
			}
			if site != "existing" && st.GetObject(first.Named()) == nil {
				t.Fatal("previously admitted independent object was rolled back")
			}
			if site == "child" || site == "cancel" {
				if st.GetObject(target.Named()) != nil || st.GetObject(last.Named()) != nil {
					t.Fatal("failure published the rejected or later sibling")
				}
			}
			if site == "final" && st.GetObject(target.Named()) == nil {
				t.Fatal("final admission error rolled back accepted publication")
			}
		})
	}
}
