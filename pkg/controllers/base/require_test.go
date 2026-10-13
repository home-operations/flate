package base_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRequire_Classifications(t *testing.T) {
	for _, mode := range []string{"ready", "blocked", "failed", "mixed", "force"} {
		t.Run(mode, func(t *testing.T) {
			st := store.New()
			id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "consumer"}
			ready := manifest.DependencyRef{Kind: manifest.KindKustomization, Namespace: "apps", Name: "ready"}
			failed := manifest.DependencyRef{Kind: manifest.KindKustomization, Namespace: "apps", Name: "failed"}
			pending := manifest.DependencyRef{Kind: manifest.KindKustomization, Namespace: "apps", Name: "pending"}
			st.UpdateStatus(ready.NamedResource, store.StatusReady, "")
			st.UpdateStatus(failed.NamedResource, store.StatusFailed, "render failed")
			st.UpdateStatus(pending.NamedResource, store.StatusPending, "working")
			st.UpdateStatus(id, store.StatusReady, "informative")
			st.AddObject(&manifest.HelmRelease{Name: id.Name, Namespace: id.Namespace, Suspend: mode != "ready"})
			deps := []manifest.DependencyRef{ready}
			level := manifest.DrainNone
			switch mode {
			case "blocked":
				deps = append(deps, pending)
			case "failed":
				deps = append(deps, failed)
			case "mixed", "force":
				deps = append(deps, pending, failed)
				if mode == "force" {
					level = manifest.DrainForce
				}
			}
			_, _, err := base.New(st, nil, "test").RequireRefresh[*manifest.HelmRelease](t.Context(), id, deps, level)
			info, _ := st.GetStatus(id)
			if info.Status != store.StatusReady || info.Message != "informative" {
				t.Fatalf("gate changed caller status: %+v", info)
			}
			switch mode {
			case "ready":
				if err != nil {
					t.Fatal(err)
				}
			case "blocked", "mixed":
				blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
				if !ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{pending.NamedResource}) || errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("blocked gate = %v, want only pending dependency", err)
				}
			case "failed", "force":
				failure, ok := errors.AsType[*manifest.DependencyFailedError](err)
				want := []manifest.NamedResource{failed.NamedResource}
				if mode == "force" {
					want = append(want, pending.NamedResource)
				}
				if !ok || failure.Parent != id || !slices.Equal(failure.Failed, want) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("failed gate = %#v, want sorted dependencies %v", err, want)
				}
				if failure.Reasons[failed.NamedResource] != "render failed" || mode == "force" && failure.Reasons[pending.NamedResource] != "not ready" {
					t.Fatalf("failure reasons = %v", failure.Reasons)
				}
			}
		})
	}
}

func TestRequireRefresh_CanonicalManifest(t *testing.T) {
	for _, found := range []bool{true, false} {
		t.Run(map[bool]string{true: "active", false: "missing"}[found], func(t *testing.T) {
			st := store.New()
			obj := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", Values: map[string]any{"value": "old"}}
			fresh := obj.Clone()
			fresh.Values["value"] = "fresh"
			var want *manifest.HelmRelease
			if found {
				st.AddObject(fresh)
				want = fresh
			}
			c := base.New(st, nil, "test")
			c.SetFilter(change.NewFilter(change.NewSet([]string{"other.yaml"}), &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{obj.Named(): {File: "consumer.yaml"}},
			}, st))
			got, ok, err := c.RequireRefresh[*manifest.HelmRelease](t.Context(), obj.Named(), nil, manifest.DrainNone)
			if err != nil || ok != found || got != want || obj.Values["value"] != "old" {
				t.Fatalf("refresh = %v, %v, %v; want canonical %v, original=%v", got, ok, err, want, obj.Values)
			}
			if info, has := st.GetStatus(obj.Named()); has {
				t.Fatalf("active/missing refresh changed status: %+v", info)
			}
		})
	}
}
