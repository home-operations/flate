package orchestrator_test

import (
	"context"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

// writeCluster lays out a two-app Flux cluster under dir: app-a renders
// ConfigMap cfg-a (data k=aVal), app-b renders ConfigMap cfg-b (data
// k=bVal). The two per-app Kustomizations are independent, so a change to
// one's rendered output must not pull the other into changed-only scope.
func writeCluster(t *testing.T, dir, aVal, bVal string) {
	t.Helper()
	ks := func(name string) string {
		return "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n" +
			"metadata: {name: " + name + ", namespace: flux-system}\n" +
			"spec:\n  path: ./apps/" + name + "\n" +
			"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
	}
	cm := func(name, val string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + "}\ndata: {k: " + val + "}\n"
	}
	testutil.WriteFile(t, dir, "ks-a.yaml", ks("app-a"))
	testutil.WriteFile(t, dir, "ks-b.yaml", ks("app-b"))
	testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", cm("cfg-a", aVal))
	testutil.WriteFile(t, dir, "apps/app-b/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/app-b/cm.yaml", cm("cfg-b", bVal))
}

// configMapNames returns the set of ConfigMap names across every parent's
// rendered manifests in a Result — what each side of the comparison
// actually rendered.
func configMapNames(res *orchestrator.Result) map[string]bool {
	out := map[string]bool{}
	for _, docs := range res.Manifests {
		for _, m := range docs {
			if m["kind"] == "ConfigMap" {
				if meta, ok := m["metadata"].(map[string]any); ok {
					if name, ok := meta["name"].(string); ok {
						out[name] = true
					}
				}
			}
		}
	}
	return out
}

// TestRenderTrees_ChangedOnlyAndDiff renders two trees that differ in just
// app-a's ConfigMap and asserts: both sides render (non-nil Results), the
// unchanged app-b is scoped OUT of both (changed-only mode), and
// diff.Changes over the two outputs reports exactly the cfg-a change.
func TestRenderTrees_ChangedOnlyAndDiff(t *testing.T) {
	baseDir := t.TempDir()
	headDir := t.TempDir()
	writeCluster(t, baseDir, "v1", "same")
	writeCluster(t, headDir, "v2", "same") // only app-a's ConfigMap data differs

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base, head, err := orchestrator.RenderTrees(ctx, baseDir, headDir, orchestrator.Config{
		WipeSecrets: true,
		Concurrency: 4,
	})
	if err != nil {
		t.Fatalf("RenderTrees: %v", err)
	}
	if base.Result == nil || head.Result == nil {
		t.Fatalf("both sides must render: base=%v head=%v", base.Result, head.Result)
	}

	// Changed-only scoping: cfg-a (changed) renders on both sides; cfg-b
	// (unchanged) renders on neither.
	for label, res := range map[string]*orchestrator.Result{"base": base.Result, "head": head.Result} {
		names := configMapNames(res)
		if !names["cfg-a"] {
			t.Errorf("%s: expected cfg-a to render (it changed); got %v", label, names)
		}
		if names["cfg-b"] {
			t.Errorf("%s: cfg-b is unchanged and must be scoped out of changed-only render; got %v", label, names)
		}
	}

	// The structured diff: exactly cfg-a changed.
	changes := diff.Changes(
		diff.DocsFromManifests(base.Result.Manifests, nil),
		diff.DocsFromManifests(head.Result.Manifests, nil),
		diff.Options{},
	)
	if len(changes) != 1 {
		t.Fatalf("want exactly 1 change (cfg-a), got %d: %+v", len(changes), changes)
	}
	c := changes[0]
	if c.Name != "cfg-a" || c.Kind != "ConfigMap" || c.Status != diff.StatusChanged {
		t.Errorf("change = %+v; want ConfigMap cfg-a changed", c)
	}
}

// TestRenderTrees_StopsButStoreReadable confirms the contract the CLI and
// SDK consumers rely on: both orchestrators are Stopped on return, yet
// their Stores stay readable for post-render doc gathering.
func TestRenderTrees_StopsButStoreReadable(t *testing.T) {
	baseDir := t.TempDir()
	headDir := t.TempDir()
	writeCluster(t, baseDir, "v1", "same")
	writeCluster(t, headDir, "v2", "same")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base, head, err := orchestrator.RenderTrees(ctx, baseDir, headDir, orchestrator.Config{WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("RenderTrees: %v", err)
	}
	// Store still answers after the internal Stop.
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "app-a"}
	if base.Store().GetObject(id) == nil {
		t.Errorf("base Store unreadable after RenderTrees Stop: %s missing", id)
	}
	if head.Store().GetObject(id) == nil {
		t.Errorf("head Store unreadable after RenderTrees Stop: %s missing", id)
	}
}
