package orchestrator

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/controllers/resourceset"
	"github.com/home-operations/flate/internal/schedule"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDAGDispatch_NeverEmittedCoverageWithdrawal(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			t.Parallel()

			s := store.New()
			tasks := task.NewBounded(workers)
			p, k := testutil.KustomizationID(
				"flux-system",
				"publisher",
			), testutil.KustomizationID(
				"flux-system",
				"never-emitted",
			)
			l := &manifest.ResourceSet{
				Name:      "static",
				Namespace: "apps",
				Inputs:    []fluxopv1.ResourceSetInput{{}},
				ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: withdrawn-output}
data: {value: rendered}
`,
			}
			provider := &manifest.ResourceSetInputProvider{
				Name:      "dynamic",
				Namespace: l.Namespace,
				Type:      fluxopv1.InputProviderGitHubBranch,
			}
			l.InputsFrom = []fluxopv1.InputProviderReference{{Name: provider.Name}}
			s.AddObjects(
				[]manifest.BaseManifest{&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace}, l, provider},
			)
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				l.Named(): {Productions: []manifest.Production{{Owner: k, Source: manifest.BootstrapSourceID}}},
			}})
			s.RecordEmitted(
				p,
				&store.Emission{
					Children: []store.Child{{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}},
					Complete: true,
				},
				nil,
			)
			s.SetArtifact(l.Named(), &store.SourceArtifact{Key: "static-l"})
			s.UpdateStatus(l.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			assert.FatalIf(
				t,
				s.GetObject(l.Named()) != l || s.GetArtifact(l.Named(), store.ArtifactContent) == nil,
				"%v",
				"fixture L is not live with an artifact",
			)
			{
				info, ok := s.GetStatus(l.Named())
				assert.FatalIf(t, !ok || info.Status != store.StatusReady, "%v", "fixture L is not Ready")
			}
			assert.FatalIf(
				t,
				s.GetArtifact(k, store.ArtifactContent) != nil,
				"%v",
				"fixture K unexpectedly emitted",
			)
			var publications atomic.Int64
			c := resourceset.New(s, tasks, nil)
			c.Configure(base.Options{})
			c.StartLifecycle()
			d := dagDispatcher{&Orchestrator{store: s, rsc: c, controllers: base.Controllers{ResourceSet: c}}}
			unsub := s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
				if id == l.Named() {
					publications.Add(1)
				}
			}, false)
			t.Cleanup(unsub)
			withdrawn := make(chan struct{})
			tasks.Go(t.Context(), "withdrawal", func(context.Context) {
				s.RecordEmitted(p, &store.Emission{Complete: true}, nil)
				close(withdrawn)
			})
			tasks.Go(t.Context(), "dispatch-withdrawn", func(ctx context.Context) {
				tasks.YieldSlot(func() { <-withdrawn })
				result := d.Dispatch(ctx, l.Named(), manifest.DrainNone, schedule.Result{})
				defer result.Assessment.Close()
				assert.ErrorIf(
					t,
					result.Assessment.Outcome() == schedule.Blocked,
					"withdrawn dispatch blocked: %+v",
					result.Assessment,
				)
			})
			tasks.BlockTillDone()
			output := manifest.NamedResource{
				Kind:      manifest.KindConfigMap,
				Namespace: l.Namespace,
				Name:      "withdrawn-output",
			}
			assert.FatalIf(t, tasks.Failures() != 0 || publications.Load() != 0 || s.GetObject(l.Named()) != nil ||
				s.GetArtifact(l.Named(), store.ArtifactContent) != nil ||
				s.GetObject(output) != nil,
				"withdrawn body/artifact leaked: publications=%d", publications.Load())

			s.AddObject(l)
			tasks.Go(t.Context(), "dispatch-restored", func(ctx context.Context) {
				d.Dispatch(ctx, l.Named(), manifest.DrainNone, schedule.Result{})
			})
			tasks.BlockTillDone()
			assert.FatalIf(
				t,
				tasks.Failures() != 0 || publications.Load() != 1 || s.GetObject(output) == nil,
				"%v",
				"restored control did not execute the real render body",
			)
		})
	}
}
