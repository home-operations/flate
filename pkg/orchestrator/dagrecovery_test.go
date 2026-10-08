package orchestrator

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func TestDAGDispatcher_RecoveredAndPrimaryFailures(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/consumer.yaml", ksYAML("consumer", "consumer", "ghost"))
	testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer}\ndata: {k: v}\n")
	o, err := New(Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	o.configureControllers()
	d := dagDispatcher{o}
	victim, ghost := ksID("consumer"), ksID("ghost")
	out, deps := d.Dispatch(t.Context(), victim, schedule.DrainCascade)
	if out != schedule.OutcomeDependencyFailed || !slices.Equal(deps, []manifest.NamedResource{ghost}) {
		t.Fatalf("derived failure = %v/%v, want ghost blocker", out, deps)
	}
	o.store.AddObject(&manifest.Kustomization{Name: ghost.Name, Namespace: ghost.Namespace})
	o.store.UpdateStatus(ghost, store.StatusReady, "")
	out, deps = d.Dispatch(t.Context(), victim, schedule.DrainNone)
	info, _ := o.store.GetStatus(victim)
	if out != schedule.OutcomeTerminal || len(deps) != 0 || info.Status != store.StatusReady || len(o.store.BlockedBy(victim)) != 0 || len(o.store.FailedResources()) != 0 {
		t.Fatalf("recovery = %v/%v status=%+v blockers=%v failures=%v", out, deps, info, o.store.BlockedBy(victim), o.store.FailedResources())
	}
	ks, _ := o.store.Get[*manifest.Kustomization](victim)
	ks = ks.Clone()
	ks.Path = "./missing-directory"
	o.store.AddObject(ks)
	o.store.SetBlocked(victim, []manifest.NamedResource{ghost})
	o.store.UpdateStatus(victim, store.StatusFailed, "old derived failure")
	out, deps = d.Dispatch(t.Context(), victim, schedule.DrainNone)
	info, _ = o.store.GetStatus(victim)
	if out != schedule.OutcomeTerminal || len(deps) != 0 || info.Status != store.StatusFailed || len(o.store.BlockedBy(victim)) != 0 {
		t.Fatalf("primary failure misclassified = %v/%v status=%+v blockers=%v", out, deps, info, o.store.BlockedBy(victim))
	}
	o.Stop()
}

func TestDAG_RedispatchErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/app.yaml", ksYAML("app", "app", ""))
	testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app}\ndata: {k: v}\n")
	o, err := New(Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	victim := ksID("app")
	unsub := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if info, ok := payload.(store.StatusInfo); ok && id == victim && info.Status == store.StatusReady {
			o.store.Refire(id)
		}
	}, false)
	defer unsub()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = o.Render(ctx)
	if err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || ctx.Err() != nil {
		t.Fatalf("Render error=%v context=%v, want scheduler redispatch error", err, ctx.Err())
	}
}
