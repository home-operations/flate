package diff

import (
	"reflect"
	"testing"
)

func cmDoc(parent Parent, name, ns, dataVal, chart string) Doc {
	meta := map[string]any{"name": name}
	if ns != "" {
		meta["namespace"] = ns
	}
	if chart != "" {
		meta["labels"] = map[string]any{"helm.sh/chart": chart}
	}
	return Doc{
		Parent: parent,
		Manifest: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": meta,
			"data":     map[string]any{"k": dataVal},
		},
	}
}

// TestChanges_Classification locks added / changed / removed and the
// drop of byte-identical resources.
func TestChanges_Classification(t *testing.T) {
	p := Parent{Kind: "Kustomization", Namespace: "flux-system", Name: "apps"}
	left := []Doc{
		cmDoc(p, "keep", "ns", "same", ""),
		cmDoc(p, "edit", "ns", "old", ""),
		cmDoc(p, "gone", "ns", "x", ""),
	}
	right := []Doc{
		cmDoc(p, "keep", "ns", "same", ""),
		cmDoc(p, "edit", "ns", "new", ""),
		cmDoc(p, "fresh", "ns", "y", ""),
	}

	got := map[string]ChangeStatus{}
	for _, c := range Changes(left, right, Options{}) {
		got[c.Name] = c.Status
	}
	want := map[string]ChangeStatus{"edit": StatusChanged, "gone": StatusRemoved, "fresh": StatusAdded}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("statuses = %v; want %v (byte-identical 'keep' must be dropped)", got, want)
	}
}

// TestChanges_StripDropsNoiseOnlyChange confirms a change confined to a
// stripped attribute (a chart-label bump with identical content) is not
// reported — the diff stays churn-free.
func TestChanges_StripDropsNoiseOnlyChange(t *testing.T) {
	p := Parent{Kind: "HelmRelease", Namespace: "ns", Name: "app"}
	left := []Doc{cmDoc(p, "x", "ns", "same", "app-1.0.0")}
	right := []Doc{cmDoc(p, "x", "ns", "same", "app-2.0.0")}
	if got := Changes(left, right, Options{StripAttrs: DefaultStripAttrs}); len(got) != 0 {
		t.Fatalf("a change only in a stripped attr should be dropped; got %+v", got)
	}
}

// TestChanges_CapturesChartBeforeStrip locks that OldChart/NewChart read
// helm.sh/chart from the pre-strip manifest while Old/New are stripped.
func TestChanges_CapturesChartBeforeStrip(t *testing.T) {
	p := Parent{Kind: "HelmRelease", Namespace: "ns", Name: "app"}
	left := []Doc{cmDoc(p, "x", "ns", "old", "app-1.0.0")}
	right := []Doc{cmDoc(p, "x", "ns", "new", "app-2.0.0")}

	got := Changes(left, right, Options{StripAttrs: DefaultStripAttrs})
	if len(got) != 1 {
		t.Fatalf("want 1 change, got %d: %+v", len(got), got)
	}
	c := got[0]
	if c.OldChart != "app-1.0.0" || c.NewChart != "app-2.0.0" {
		t.Errorf("chart not captured pre-strip: Old=%q New=%q", c.OldChart, c.NewChart)
	}
	meta, _ := c.New["metadata"].(map[string]any)
	if labels, ok := meta["labels"].(map[string]any); ok {
		if _, present := labels["helm.sh/chart"]; present {
			t.Errorf("helm.sh/chart must be stripped from New; labels=%v", labels)
		}
	}
}

// TestChanges_NormalizeHookSuppressesChange is the konflate use case: a
// render-random Secret value reads as a change by default, but a redaction
// hook constant-izes it so it no longer does.
func TestChanges_NormalizeHookSuppressesChange(t *testing.T) {
	p := Parent{Kind: "Kustomization", Namespace: "ns", Name: "apps"}
	secret := func(val string) Doc {
		return Doc{
			Parent: p,
			Manifest: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"metadata": map[string]any{"name": "s", "namespace": "ns"},
				"data":     map[string]any{"token": val},
			},
		}
	}
	left := []Doc{secret("AAAA")}
	right := []Doc{secret("BBBB")}

	if got := Changes(left, right, Options{}); len(got) != 1 {
		t.Fatalf("without redaction the value change must show; got %d", len(got))
	}
	redact := func(m map[string]any) {
		if m["kind"] != "Secret" {
			return
		}
		if d, ok := m["data"].(map[string]any); ok {
			for k := range d {
				d[k] = "<redacted>"
			}
		}
	}
	if got := Changes(left, right, Options{Normalize: redact}); len(got) != 0 {
		t.Fatalf("redaction hook should suppress the render-random change; got %+v", got)
	}
}

// TestChanges_Sorted locks deterministic ordering by parent then identity.
func TestChanges_Sorted(t *testing.T) {
	pa := Parent{Kind: "Kustomization", Namespace: "ns", Name: "a"}
	pb := Parent{Kind: "Kustomization", Namespace: "ns", Name: "b"}
	left := []Doc{cmDoc(pb, "z", "ns", "1", ""), cmDoc(pa, "y", "ns", "1", "")}
	right := []Doc{cmDoc(pb, "z", "ns", "2", ""), cmDoc(pa, "y", "ns", "2", "")}
	got := Changes(left, right, Options{})
	if len(got) != 2 || got[0].Parent.Name != "a" || got[1].Parent.Name != "b" {
		t.Fatalf("changes not sorted by parent: %+v", got)
	}
}
