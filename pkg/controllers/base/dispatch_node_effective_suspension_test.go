package base_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDispatchNode_EffectiveSuspension(t *testing.T) {
	t.Parallel()
	st := store.New()
	tasks := task.NewBounded(2)
	c := base.New(st, tasks)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	input := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "child",
		Namespace: "apps",
	}
	st.AddObjects([]manifest.BaseManifest{parent, input})
	publish := func(obj *manifest.HelmRelease) {
		t.Helper()
		_, _, next := st.GetEmission(parent.Named())
		defer next.Finish()
		next.Children, next.Complete = []store.Child{{Obj: obj}}, true
		st.RecordEmitted(parent.Named(), &next, nil)
	}
	publish(input)
	entered, replaced := make(chan struct{}), make(chan struct{})
	c.Configure(base.Options{PreflightFailure: func(id manifest.NamedResource) (string, bool) {
		if id == input.Named() {
			close(entered)
			tasks.YieldSlot(func() { <-replaced })
		}
		return "", false
	}})
	c.StartLifecycle()
	var bodies atomic.Int64
	tasks.Go(t.Context(), "dispatch", func(ctx context.Context) {
		run := controllertest.NewExecution(c, input.Named())
		run.Dispatch(
			ctx,
			manifest.DrainNone,
			struct{}{},
			func(_ struct{}, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
				bodies.Add(1)
				return nil
			},
		)
	})
	tasks.Go(t.Context(), "replace", func(context.Context) {
		tasks.YieldSlot(func() { <-entered })
		replacement := input.Clone()
		replacement.Suspend = true
		publish(replacement)
		close(replaced)
	})
	tasks.BlockTillDone()
	info, _ := st.GetStatus(input.Named())
	if tasks.Failures() != 0 || bodies.Load() != 0 || info.Outcome() != store.OutcomeSuspended || input.Suspend {
		t.Fatalf("effective suspension: bodies=%d status=%+v", bodies.Load(), info)
	}
}

type blockedFingerprintContent struct {
	tasks    *task.Service
	entered  chan struct{}
	replaced <-chan struct{}
}

func (c blockedFingerprintContent) MarshalJSON() ([]byte, error) {
	close(c.entered)
	c.tasks.YieldSlot(func() { <-c.replaced })
	return []byte(`"A"`), nil
}

func TestFingerprintDedup_CompleteOnly(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"current", "predecessor"} {
		for _, complete := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/complete=%t", path, complete), func(t *testing.T) {
				t.Parallel()
				s := store.New()
				c := base.New(s, nil)
				parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
				child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
				s.AddObject(parent)
				index := &manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}},
				}
				filterLoader := admissiontest.New(
					t,
					admissiontest.Config{
						Changes:   change.NewSet([]string{"parent.yaml"}),
						Index:     index,
						Inventory: s,
						Root:      "",
					},
				)
				admissiontest.Commit(t, filterLoader, nil)
				filter := filterLoader.Options.Filter
				c.Configure(base.Options{Filter: filter, Index: index, Loader: filterLoader})
				fingerprint := manifest.Fingerprint("same")
				_, _, next := s.GetEmission(parent.Named())
				defer next.Finish()
				next.Children, next.Fingerprint, next.Complete = []store.Child{{Obj: child}}, fingerprint, complete
				if path == "current" {
					s.RecordEmitted(parent.Named(), &next, nil)
				} else {
					s.RecordEmitted(parent.Named(), &next, nil)
					assert.FatalIf(t, !complete &&
						s.GetArtifact(
							parent.Named(),
							store.ArtifactContent,
						).(*store.Emission).Fingerprint != "", "%v", "incomplete successor retained a successful fingerprint")
					changed := parent.Clone()
					changed.Labels = map[string]string{"changed": "yes"}
					s.AddObject(changed)
					assert.FatalIf(
						t,
						s.GetArtifact(parent.Named(), store.ArtifactContent) != nil,
						"%v",
						"fixture did not invalidate the current emission",
					)
				}
				assert.FatalIf(
					t,
					filter.ShouldReconcile(child.Named()),
					"%v",
					"fixture child already selected before replay",
				)
				fingerprintDedupResult1, fingerprintDedupResult1Err := controllertest.NewExecution(c, parent.Named()).
					FingerprintDedup(t.Context(), parent.Named(), "same", &store.Emission{})
				fp, handled, err := fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused, fingerprintDedupResult1Err
				assert.FatalIf(
					t,
					fp != fingerprint || handled != complete || err != nil,
					"replay key=%q handled=%t err=%v, want %q,%t,nil",
					fp,
					handled,
					err,
					fingerprint,
					complete,
				)
				assert.FatalIf(
					t,
					filter.ShouldReconcile(child.Named()) != complete,
					"%v",
					"complete replay did not extend changed-only selection through Emit",
				)
			})
		}
	}
}
