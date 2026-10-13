package diff

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestPair_DistinguishesByParent locks the parent-aware pairing: two
// HelmReleases each rendering a same-named Deployment are tracked
// independently, so a change to HR/a's copy doesn't leak into a phantom
// pair against HR/b's copy. Without the parent in the key the two would
// collapse into one entry.
func TestPair_DistinguishesByParent(t *testing.T) {
	dep := func(hr, spec string) Doc {
		return Doc{
			Manifest: map[string]any{"kind": "Deployment", "metadata": map[string]any{"name": "x", "namespace": "ns"}, "spec": spec},
			Parent:   Parent{Kind: "HelmRelease", Namespace: "ns", Name: hr},
		}
	}
	left := []Doc{dep("a", "old"), dep("b", "old")}
	right := []Doc{dep("a", "new"), dep("b", "old")}

	pairs := pair(left, right)
	if len(pairs) != 2 {
		t.Fatalf("expected 2 paired resources (one per parent), got %d: %+v", len(pairs), pairs)
	}
	for _, p := range pairs {
		changed := p.a["spec"] != p.b["spec"]
		switch p.parent.Name {
		case "a":
			if !changed {
				t.Errorf("HR a's Deployment should show a spec change: %+v", p)
			}
		case "b":
			if changed {
				t.Errorf("HR b's Deployment should be unchanged: %+v", p)
			}
		default:
			t.Errorf("unexpected parent %q", p.parent.Name)
		}
	}
}

func TestPair_PathIsPresentation(t *testing.T) {
	leftParent := Parent{Kind: "Kustomization", Namespace: "flux-system", Name: "apps", Path: "old/apps"}
	for _, tt := range []struct {
		name, path, resource, parent string
	}{
		{"moved", "new/apps", "ConfigMap apps/shared (new/apps)", "Kustomization flux-system/apps (new/apps)"},
		{"cleared", "", "ConfigMap apps/shared", "Kustomization flux-system/apps"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rightParent := leftParent
			rightParent.Path = tt.path
			left := []Doc{cmDoc(leftParent, "shared", "apps", "old", "")}
			right := []Doc{cmDoc(rightParent, "shared", "apps", "old", "")}
			if got := Changes(left, right, Options{}); len(got) != 0 {
				t.Fatalf("path-only move produced changes: %+v", got)
			}
			for _, format := range []Format{FormatDiff, FormatHTML} {
				out, err := RenderDocs(left, right, Options{Format: format})
				if err != nil || len(out) != 0 {
					t.Fatalf("%s path-only move produced output %q: %v", format, out, err)
				}
			}
			right[0].Manifest["data"] = map[string]any{"k": "new"}
			changes := Changes(left, right, Options{})
			if len(changes) != 1 || changes[0].Status != StatusChanged || changes[0].Parent != rightParent {
				t.Fatalf("content change must use current parent: %+v", changes)
			}
			for _, format := range []Format{FormatDiff, FormatHTML} {
				out, err := RenderDocs(left, right, Options{Format: format})
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(out), "old/apps") {
					t.Errorf("%s heading contains old path: %s", format, out)
				}
				if format == FormatDiff {
					want := "--- " + tt.resource + "\n+++ " + tt.resource + "\n"
					if !strings.HasPrefix(string(out), want) {
						t.Errorf("unified headers want %q, got %s", want, out)
					}
				} else if want := `<div class="rparent">` + tt.parent + `</div>`; !strings.Contains(string(out), want) {
					t.Errorf("HTML parent heading missing %q", want)
				}
			}
			removed := Changes(left, nil, Options{})
			if len(removed) != 1 || removed[0].Parent != leftParent {
				t.Errorf("removal must use original parent: %+v", removed)
			}
			want := "--- ConfigMap apps/shared (old/apps)\n+++ ConfigMap apps/shared (old/apps)\n"
			if out := unified(t, left, nil); !strings.HasPrefix(out, want) {
				t.Errorf("removal headers want %q, got %s", want, out)
			}
		})
	}
}

func TestPair_DuplicateDocumentsWarnAndLastWins(t *testing.T) {
	parent := Parent{Kind: "Kustomization", Namespace: "flux-system", Name: "apps", Path: "./apps"}
	for _, side := range []string{"left", "right"} {
		t.Run(side, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })

			first := cmDoc(parent, "shared", "ns", "shadowed", "")
			last := cmDoc(parent, "shared", "ns", "retained", "")
			last.Parent.Path = "new/apps"
			other := cmDoc(parent, "shared", "ns", "other-side", "")
			left, right := []Doc{first, last}, []Doc{other}
			if side == "right" {
				left, right = right, left
			}
			pairs := pair(left, right)
			if len(pairs) != 1 {
				t.Fatalf("expected one paired resource, got %d", len(pairs))
			}
			retained, counterpart := pairs[0].a, pairs[0].b
			if side == "right" {
				retained, counterpart = counterpart, retained
			}
			if retained["data"].(map[string]any)["k"] != "retained" || counterpart["data"].(map[string]any)["k"] != "other-side" {
				t.Fatalf("expected last document paired with other side, got %+v", pairs[0])
			}
			if strings.Count(logs.String(), "level=WARN") != 1 {
				t.Fatalf("expected one duplicate warning, got %q", logs.String())
			}
			for _, field := range []string{"diff: duplicate document", "side=" + side, "parentKind=Kustomization", "parentNamespace=flux-system", "parentName=apps", "parentPath=new/apps", "apiVersion=v1", "kind=ConfigMap", "namespace=ns", "name=shared"} {
				if !strings.Contains(logs.String(), field) {
					t.Errorf("warning missing %q: %s", field, logs.String())
				}
			}
		})
	}
}
