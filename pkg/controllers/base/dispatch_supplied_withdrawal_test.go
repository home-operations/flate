package base_test

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDispatch_SuppliedWithdrawal(t *testing.T) {
	t.Parallel()
	s := store.New()
	parent := &manifest.ResourceSet{Name: "parent", Namespace: "apps"}
	reference := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "supplied"}
	repository := reference.Clone()
	repository.Path = "repository"
	s.AddObject(parent)
	s.AddDeclaration(
		store.Declaration{
			Obj:    reference,
			Doc:    testutil.ObjectDocument(reference),
			Origin: manifest.SuppliedDeclaration,
		},
	)
	s.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: repository}}, Complete: true}, nil)
	c := base.New(s, nil)
	run := controllertest.NewExecution(c, repository.Named())
	defer run.Close()
	leaf := &manifest.ConfigMap{Name: "leaf", Namespace: "apps"}
	run.Dispatch(t.Context(), manifest.DrainNone, run,
		func(run *base.Execution, ctx context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
			return run.Emit(
				ctx,
				repository.Named(),
				&store.Emission{Children: []store.Child{{Obj: leaf}}, Complete: true},
				nil,
			)
		})
	queued := controllertest.NewExecution(c, repository.Named())
	defer queued.Close()
	s.RecordEmitted(parent.Named(), &store.Emission{Complete: true}, nil)
	if run.Valid() {
		t.Fatal("withdrawal did not invalidate the repository execution")
	}
	queued.Dispatch(t.Context(), manifest.DrainNone, struct{}{},
		func(struct{}, context.Context, *manifest.Kustomization, manifest.DrainLevel) error {
			t.Error("queued node reconciled the supplied declaration")
			return nil
		})
	if !queued.Valid() || s.Policy(reference.Named()).Reconcile || s.GetObject(leaf.Named()) != nil {
		t.Fatal("supplied declaration retained production visibility")
	}
}
