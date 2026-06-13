package report

import (
	"bytes"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func ks(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
}

func failed(ids ...manifest.NamedResource) map[manifest.NamedResource]store.StatusInfo {
	m := map[manifest.NamedResource]store.StatusInfo{}
	for _, id := range ids {
		m[id] = store.StatusInfo{Status: store.StatusFailed, Message: id.Name + " boom"}
	}
	return m
}

// TestBuild_GroupsCascadeUnderRoots pins the core collapse: a real (primary)
// failure and a missing dependency each surface once, with the resources that
// cascaded from them folded into Blocks / RequiredBy — never on their own.
func TestBuild_GroupsCascadeUnderRoots(t *testing.T) {
	root, mid, leaf := ks("root"), ks("mid"), ks("leaf") // leaf → mid → root (primary)
	missing := ks("missing")
	dependent := ks("dependent") // dependent → missing (not found)

	f := failed(root, mid, leaf, dependent) // missing is absent from the store
	blocked := map[manifest.NamedResource][]manifest.NamedResource{
		mid:       {root},
		leaf:      {mid},
		dependent: {missing},
	}

	m := Build(f, blocked, nil, nil)

	if len(m.Primary) != 1 || m.Primary[0].ID != root {
		t.Fatalf("Primary = %+v, want only root", m.Primary)
	}
	// root's cascade is mid and leaf (transitively), folded under it and sorted.
	if got := m.Primary[0].Blocks; len(got) != 2 || got[0] != leaf || got[1] != mid {
		t.Errorf("root.Blocks = %+v, want sorted [leaf mid]", got)
	}
	if len(m.Missing) != 1 || m.Missing[0].ID != missing {
		t.Fatalf("Missing = %+v, want only 'missing'", m.Missing)
	}
	if got := m.Missing[0].RequiredBy; len(got) != 1 || got[0] != dependent {
		t.Errorf("missing.RequiredBy = %+v, want [dependent]", got)
	}

	var b bytes.Buffer
	if err := m.Write(&b, false, 0); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"root", "blocks", "missing", "not found", "required by", "2 failed", "3 blocked"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered report missing %q:\n%s", want, out)
		}
	}
	// Cascaded resources never get their own failure line.
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "✗") && (strings.Contains(line, "leaf") || strings.Contains(line, "dependent")) {
			t.Errorf("cascaded resource must not get its own failure line: %q", line)
		}
	}
}

// TestBuild_PrimaryOnly covers a lone real failure with nothing depending on it.
func TestBuild_PrimaryOnly(t *testing.T) {
	a := ks("a")
	m := Build(failed(a), nil, nil, nil)
	if len(m.Primary) != 1 || len(m.Missing) != 0 || len(m.Primary[0].Blocks) != 0 {
		t.Fatalf("want one primary with no blocks, got %+v", m)
	}
	if m.Empty() {
		t.Error("a model with a failure is not empty")
	}
}

// TestBuild_NotesOnly covers a clean run that only carries deferred log notes.
func TestBuild_NotesOnly(t *testing.T) {
	m := Build(nil, nil, nil, []Note{{Text: "resource orphaned id=x", Count: 3}})
	if len(m.Primary) != 0 || len(m.Missing) != 0 || m.Empty() {
		t.Fatalf("notes-only model should be non-empty with no failures: %+v", m)
	}
	var b bytes.Buffer
	_ = m.Write(&b, false, 0)
	if out := b.String(); !strings.Contains(out, "notes (3)") || !strings.Contains(out, "×3") {
		t.Errorf("notes footer missing or not collapsed:\n%s", out)
	}
}

// TestBuild_Empty covers a clean run with nothing to show.
func TestBuild_Empty(t *testing.T) {
	if !Build(nil, nil, nil, nil).Empty() {
		t.Error("a clean run must produce an empty model")
	}
}

// TestWrite_WarningsSection: a clean run carrying only render advisories renders
// a "warnings" section (attributed + detail) and NO red "✗ 0 failed" verdict.
func TestWrite_WarningsSection(t *testing.T) {
	hr := manifest.NamedResource{Kind: "HelmRelease", Namespace: "default", Name: "app"}
	warns := []manifest.Warning{
		{Resource: hr, Category: manifest.WarnStaleValues, Message: "values not defined by the chart schema", Detail: []string{"image.tagg", "oldKey"}},
		{Category: manifest.WarnEmptyScan, Message: "no Flux objects found"}, // global
	}
	m := Build(nil, nil, warns, nil)
	if m.Empty() {
		t.Fatal("a model with warnings is not empty")
	}
	var b bytes.Buffer
	if err := m.Write(&b, false, 0); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"warnings (2)", "HelmRelease default/app: values not defined", "image.tagg, oldKey", "no Flux objects found"} {
		if !strings.Contains(out, want) {
			t.Errorf("warnings section missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "failed") {
		t.Errorf("a warnings-only run must not render a failure verdict:\n%s", out)
	}
}

// TestWrite_WarningCount: a deduped warning renders its repeat count.
func TestWrite_WarningCount(t *testing.T) {
	warns := []manifest.Warning{{Category: manifest.WarnPathConfig, Message: "same root", Count: 4}}
	var b bytes.Buffer
	_ = Build(nil, nil, warns, nil).Write(&b, false, 0)
	if out := b.String(); !strings.Contains(out, "×4") {
		t.Errorf("warning repeat count not rendered:\n%s", out)
	}
}

// TestWrite_PrimaryMessageIsMultiLine pins that a multi-line primary message (a
// kustomize duplicate-id diagnostic) renders every line — the producer
// attribution must survive intact rather than being truncated to the first
// line, which is exactly the detail that tells the user what to fix.
func TestWrite_PrimaryMessageIsMultiLine(t *testing.T) {
	a := ks("a")
	f := map[manifest.NamedResource]store.StatusInfo{
		a: {Status: store.StatusFailed, Message: "may not add resource with an already registered id: X\n" +
			"duplicate resource(s) produced by multiple accumulated paths:\n  - ./one\n  - ./two"},
	}
	var b bytes.Buffer
	if err := Build(f, nil, nil, nil).Write(&b, false, 0); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"already registered id: X", "produced by multiple accumulated paths", "- ./one", "- ./two"} {
		if !strings.Contains(out, want) {
			t.Errorf("multi-line primary message missing %q:\n%s", want, out)
		}
	}
}

// TestBuild_BlockedCountsDistinctResources pins that a resource blocked by two
// independent roots (a failed parent AND a missing dependency) counts once in
// the verdict, even though it folds under both roots' lists.
func TestBuild_BlockedCountsDistinctResources(t *testing.T) {
	root, missing, dual := ks("root"), ks("missing"), ks("dual")
	f := failed(root, dual) // missing is absent from the store
	blocked := map[manifest.NamedResource][]manifest.NamedResource{
		dual: {root, missing}, // blocked by a failed parent AND a missing dep
	}

	m := Build(f, blocked, nil, nil)
	if m.Blocked != 1 {
		t.Errorf("Blocked = %d, want 1 distinct blocked resource", m.Blocked)
	}
	if len(m.Primary) != 1 || len(m.Primary[0].Blocks) != 1 {
		t.Errorf("root should fold dual once: %+v", m.Primary)
	}
	if len(m.Missing) != 1 || len(m.Missing[0].RequiredBy) != 1 {
		t.Errorf("missing should fold dual once: %+v", m.Missing)
	}

	var b bytes.Buffer
	_ = m.Write(&b, false, 0)
	if out := b.String(); !strings.Contains(out, "1 blocked") {
		t.Errorf("verdict should count the dual-rooted resource once:\n%s", out)
	}
}

// TestRoots_WalksTransitiveChainToTrueRoot pins the shared grouping the test
// runner reuses: a leaf folds under the primary at the top of its blocker chain.
func TestRoots_WalksTransitiveChainToTrueRoot(t *testing.T) {
	root, mid, leaf := ks("root"), ks("mid"), ks("leaf")
	byRoot := Roots(map[manifest.NamedResource][]manifest.NamedResource{
		mid:  {root},
		leaf: {mid},
	})
	if got := byRoot[root]; len(got) != 2 {
		t.Fatalf("root should gather mid and leaf, got %+v", got)
	}
}
