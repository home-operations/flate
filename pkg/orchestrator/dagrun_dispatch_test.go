package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDAGDispatch_ResourceSetDedup(t *testing.T) {
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, true)
	c.Configure(resourceset.Options{})
	c.StartLifecycle()
	t.Cleanup(c.Close)
	rs := &manifest.ResourceSet{Name: "dedup", Namespace: "ns", ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: value}
`}
	s.AddObject(rs)
	var arrivals atomic.Int64
	unsub := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals.Add(1) }, false)
	t.Cleanup(unsub)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
	out, blocked, _ := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	if out != schedule.OutcomeTerminal || len(blocked) != 0 || arrivals.Load() != 1 {
		t.Fatalf("fresh result=%v blocked=%v arrivals=%d", out, blocked, arrivals.Load())
	}
	before := s.GetArtifact(rs.Named())
	out, blocked, _ = d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	if out != schedule.OutcomeTerminalNoop || len(blocked) != 0 || arrivals.Load() != 1 || s.GetArtifact(rs.Named()) != before {
		t.Fatalf("dedup result=%v blocked=%v arrivals=%d artifact changed=%v", out, blocked, arrivals.Load(), s.GetArtifact(rs.Named()) != before)
	}
}

func TestDAGDispatch_ResourceSetClassification(t *testing.T) {
	for _, name := range []string{"dedup", "fresh fingerprint identical docs", "missing artifact", "empty fingerprint", "blocked gate", "failed gate", "preflight failure", "filter skip", "vanished", "render failure", "suspended"} {
		for _, selector := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/selector_%v", name, selector), func(t *testing.T) {
				s := store.New()
				tasks := task.NewBounded(2)
				c := resourceset.New(s, tasks, true)
				c.Configure(resourceset.Options{})
				c.StartLifecycle()
				rs := &manifest.ResourceSet{Name: "matrix", Namespace: "ns", Inputs: []fluxopv1.ResourceSetInput{{}}, ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: fixed}
`}
				if selector {
					rs.InputsFrom = []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}}}}
				}
				s.AddObject(rs)
				d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
				if out, blocked, rerun := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone); rerun != selector || out != schedule.OutcomeTerminal || len(blocked) != 0 {
					t.Fatalf("fresh setup failed: %v %v", out, blocked)
				}
				before := s.GetArtifact(rs.Named()).(*store.ResourceSetArtifact)
				c.Close()
				opts := resourceset.Options{}
				want := schedule.OutcomeTerminal
				fresh := *rs
				switch name {
				case "dedup":
					want = schedule.OutcomeTerminalNoop
				case "fresh fingerprint identical docs":
					fresh.Inputs = []fluxopv1.ResourceSetInput{{"unused": &apiextensionsv1.JSON{Raw: []byte(`"new"`)}}}
					s.AddObject(&fresh)
				case "missing artifact":
					s.DeleteObject(rs.Named())
					s.AddObject(rs)
				case "empty fingerprint":
					s.SetArtifact(rs.Named(), &store.ResourceSetArtifact{Manifests: before.Manifests})
				case "blocked gate", "failed gate":
					fresh.DependsOn = []fluxopv1.Dependency{{Kind: manifest.KindResourceSet, Name: "gate", Namespace: "ns"}}
					s.AddObject(&fresh)
					want = schedule.OutcomeBlocked
					if name == "failed gate" {
						gate := &manifest.ResourceSet{Name: "gate", Namespace: "ns"}
						s.AddObject(gate)
						s.UpdateStatus(gate.Named(), store.StatusFailed, "gate failed")
						want = schedule.OutcomeTerminal
					}
				case "preflight failure":
					opts.PreflightFailure = func(manifest.NamedResource) (string, bool) { return "cycle", true }
				case "filter skip":
					opts.Filter = change.NewFilter(change.NewSet(nil), indexForTest(nil), s)
				case "vanished":
					s.DeleteObject(rs.Named())
					s.SetArtifact(rs.Named(), before)
				case "suspended":
					fresh.Suspend = true
					s.AddObject(&fresh)
				case "render failure":
					fresh.ResourcesTemplate = "<< .missing.field >>"
					s.AddObject(&fresh)
				}
				c = resourceset.New(s, tasks, true)
				c.Configure(opts)
				c.StartLifecycle()
				t.Cleanup(c.Close)
				d.o.rsc = c
				var arrivals atomic.Int64
				unsub := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals.Add(1) }, false)
				t.Cleanup(unsub)
				out, blocked, rerun := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
				if rerun != (selector && name != "vanished" && name != "suspended") || out != want || (len(blocked) > 0) != (want == schedule.OutcomeBlocked) {
					t.Fatalf("outcome=%v blocked=%v; want %v", out, blocked, want)
				}
				if want == schedule.OutcomeTerminalNoop && arrivals.Load() != 0 {
					t.Fatalf("no-op emitted %d arrivals", arrivals.Load())
				}
				if name == "fresh fingerprint identical docs" {
					after := s.GetArtifact(rs.Named()).(*store.ResourceSetArtifact)
					if before == after || before.Fingerprint == after.Fingerprint {
						t.Fatal("fresh fingerprint must replace artifact identity")
					}
					if diff := cmp.Diff(before.Manifests, after.Manifests); diff != "" {
						t.Fatalf("fixture documents must stay identical (-want +got): %s", diff)
					}
				}
				if name == "filter skip" {
					info, ok := s.GetStatus(rs.Named())
					if !ok || info.Status != store.StatusReady || info.Message != store.MsgUnchanged || s.GetArtifact(rs.Named()) != before {
						t.Fatalf("filter must preserve artifact with Ready/unchanged: %+v", info)
					}
				}
			})
		}
	}
}

func TestDAGDispatch_ConcurrentResourceSetUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, true)
	c.Configure(resourceset.Options{})
	c.StartLifecycle()
	t.Cleanup(c.Close)
	rs := &manifest.ResourceSet{Name: "concurrent", Namespace: "ns", Inputs: []fluxopv1.ResourceSetInput{{}}, ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: initial}
`}
	s.AddObject(rs)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
	d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	s.UpdateStatus(rs.Named(), store.StatusPending, "queued")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unsub := s.AddListener(store.EventStatusUpdated, func(nid manifest.NamedResource, _ any) {
		if nid == rs.Named() {
			once.Do(func() {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
	}, false)
	defer unsub()
	fresh := *rs
	fresh.ResourcesTemplate = strings.ReplaceAll(rs.ResourcesTemplate, "initial", "changed")
	s.AddObject(&fresh)
	done := make(chan schedule.Outcome, 1)
	tasks.Go(ctx, "dispatch", func(ctx context.Context) {
		out, _, rerun := d.Dispatch(ctx, rs.Named(), manifest.DrainNone)
		if !rerun {
			t.Error("post-reconcile intent ignored the updated selector")
		}
		done <- out
	})
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	newest := fresh
	newest.ResourcesTemplate = strings.ReplaceAll(fresh.ResourcesTemplate, "changed", "newest")
	newest.InputsFrom = []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}}}}
	tasks.Go(ctx, "update", func(context.Context) {
		s.AddObject(&newest)
		close(release)
	})
	if out := <-done; out != schedule.OutcomeTerminal {
		t.Fatalf("concurrent fresh render classified as %v", out)
	}
	tasks.BlockTillDone()
	if out, _, _ := d.Dispatch(ctx, rs.Named(), manifest.DrainNone); out != schedule.OutcomeTerminal {
		t.Fatalf("newest content must render conservatively: %v", out)
	}
	if out, _, rerun := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone); !rerun || out != schedule.OutcomeTerminalNoop {
		t.Fatalf("settled newest content must dedup: %v", out)
	}
	cm, ok := s.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "output"})
	if !ok || cm.Data["key"] != "newest" {
		t.Fatalf("latest concurrent content was lost: %+v", cm)
	}
}
