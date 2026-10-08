package orchestrator

import (
	"context"
	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/pkg/change"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDAGDispatch_ResourceSetDedup(t *testing.T) {
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, true)
	c.Configure(resourceset.Options{})
	c.Start(t.Context())
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
	out, blocked := d.Dispatch(t.Context(), rs.Named(), schedule.DrainNone)
	if out != schedule.OutcomeTerminal || len(blocked) != 0 || arrivals.Load() != 1 {
		t.Fatalf("fresh result=%v blocked=%v arrivals=%d", out, blocked, arrivals.Load())
	}
	before := s.GetArtifact(rs.Named())
	out, blocked = d.Dispatch(t.Context(), rs.Named(), schedule.DrainNone)
	if out != schedule.OutcomeTerminalNoop || len(blocked) != 0 || arrivals.Load() != 1 || s.GetArtifact(rs.Named()) != before {
		t.Fatalf("dedup result=%v blocked=%v arrivals=%d artifact changed=%v", out, blocked, arrivals.Load(), s.GetArtifact(rs.Named()) != before)
	}
}

func TestDAGDispatch_ResourceSetClassification(t *testing.T) {
	for _, name := range []string{"dedup", "fresh fingerprint identical docs", "missing artifact", "empty fingerprint", "blocked gate", "failed gate", "preflight failure", "filter skip", "vanished", "render failure"} {
		t.Run(name, func(t *testing.T) {
			s := store.New()
			tasks := task.NewBounded(2)
			c := resourceset.New(s, tasks, true)
			c.Configure(resourceset.Options{})
			c.Start(t.Context())
			rs := &manifest.ResourceSet{Name: "matrix", Namespace: "ns", ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: fixed}
`}
			s.AddObject(rs)
			d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
			if out, blocked := d.Dispatch(t.Context(), rs.Named(), schedule.DrainNone); out != schedule.OutcomeTerminal || len(blocked) != 0 {
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
				opts.Filter = change.NewFilter(change.NewSet(nil), nil, t.TempDir(), s)
			case "vanished":
				s.DeleteObject(rs.Named())
				s.SetArtifact(rs.Named(), before)
			case "render failure":
				fresh.ResourcesTemplate = "<< .missing.field >>"
				s.AddObject(&fresh)
			}
			c = resourceset.New(s, tasks, true)
			c.Configure(opts)
			c.Start(t.Context())
			t.Cleanup(c.Close)
			d.o.rsc = c
			var arrivals atomic.Int64
			unsub := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals.Add(1) }, false)
			t.Cleanup(unsub)
			out, blocked := d.Dispatch(t.Context(), rs.Named(), schedule.DrainNone)
			if out != want || (len(blocked) > 0) != (want == schedule.OutcomeBlocked) {
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

func TestDAGDispatch_ConcurrentResourceSetUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, true)
	c.Configure(resourceset.Options{})
	c.Start(t.Context())
	t.Cleanup(c.Close)
	rs := &manifest.ResourceSet{Name: "concurrent", Namespace: "ns", ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: initial}
`}
	s.AddObject(rs)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c}}
	d.Dispatch(t.Context(), rs.Named(), schedule.DrainNone)
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
		out, _ := d.Dispatch(ctx, rs.Named(), schedule.DrainNone)
		done <- out
	})
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	newest := fresh
	newest.ResourcesTemplate = strings.ReplaceAll(fresh.ResourcesTemplate, "changed", "newest")
	tasks.Go(ctx, "update", func(context.Context) {
		s.AddObject(&newest)
		close(release)
	})
	if out := <-done; out != schedule.OutcomeTerminal {
		t.Fatalf("concurrent fresh render classified as %v", out)
	}
	tasks.BlockTillDone()
	if out, _ := d.Dispatch(ctx, rs.Named(), schedule.DrainNone); out != schedule.OutcomeTerminal {
		t.Fatalf("newest content must render conservatively: %v", out)
	}
	if out, _ := d.Dispatch(t.Context(), rs.Named(), schedule.DrainNone); out != schedule.OutcomeTerminalNoop {
		t.Fatalf("settled newest content must dedup: %v", out)
	}
	cm, ok := s.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "output"})
	if !ok || cm.Data["key"] != "newest" {
		t.Fatalf("latest concurrent content was lost: %+v", cm)
	}
}
