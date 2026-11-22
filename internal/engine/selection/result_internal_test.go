package selection

import (
	"errors"
	"hash/maphash"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResult_WarningsPreserveStoredEvidence(t *testing.T) {
	t.Parallel()
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	s.AddWarning(manifest.Warning{Resource: id, Category: manifest.WarnInputParseFailed, Detail: []string{"token"}})
	before := s.Warnings()
	result := &Result{Warnings: before}
	assert.Equal(t, len(result.Warnings), 1)
	result.Warnings = nil
	assert.Diff(t, s.Warnings(), before)
}

type externalSourceFacts struct{ solverFacts }

func (*externalSourceFacts) SourceExternal(manifest.NamedResource, *store.ReadSet) bool { return true }

func TestPrepareExternal_EntryMustOwnReaderAndGenerator(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                      string
		ownsReader, ownsGenerator bool
	}{
		{name: "only reader", ownsReader: true},
		{name: "only generator", ownsGenerator: true},
		{name: "both", ownsReader: true, ownsGenerator: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			entry := &manifest.Kustomization{Name: "entry", Namespace: "apps"}
			sibling := &manifest.Kustomization{Name: "sibling", Namespace: "apps"}
			generator := &manifest.RawObject{
				APIVersion: manifest.SourceExtensionsDomain + "/v1beta1",
				Kind:       manifest.KindArtifactGenerator, Name: "generator", Namespace: "apps",
			}
			input := &manifest.ExternalArtifact{Name: "artifact", Namespace: "apps"}
			reader := &manifest.Kustomization{
				Name:      "reader",
				Namespace: "apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindExternalArtifact,
					Name: input.Name,
				},
			}
			parent := &manifest.ResourceSet{Name: "parent", Namespace: "apps"}
			st.AddObject(parent)
			st.AddObject(sibling)
			st.RecordEmitted(parent.Named(), &store.Emission{Complete: true, Children: []store.Child{
				{Obj: entry, Generated: true, VisibleGeneration: true},
			}}, nil)
			var owned, other []store.Child
			for _, child := range []struct {
				obj   manifest.BaseManifest
				owned bool
			}{{obj: reader, owned: tc.ownsReader}, {obj: generator, owned: tc.ownsGenerator}} {
				if child.owned {
					owned = append(owned, store.Child{Obj: child.obj})
				} else {
					other = append(other, store.Child{Obj: child.obj})
				}
			}
			st.RecordEmitted(entry.Named(), &store.Emission{Complete: true, Children: owned}, nil)
			st.RecordEmitted(sibling.Named(), &store.Emission{Complete: true, Children: other}, nil)
			st.RecordEmitted(
				generator.Named(),
				&store.Emission{Complete: true, Children: []store.Child{{Obj: input}}},
				nil,
			)
			st.SetArtifact(input.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
			st.UpdateStatus(reader.Named(), store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged})
			side := selectionSide{
				run: Snapshot{
					Store: st,
					Facts: &externalSourceFacts{},
					Entry: EntryIdentity{Producer: entry.Named()},
				},
				result: &Result{},
			}
			side.reads.Init(st, manifest.NamedResource{}, nil)
			defer side.reads.Retire()
			assert.Equal(t, side.reads.GeneratedDocument(entry.Named()), true)
			side.prepareExternal(reader.Named(), reader)
			if tc.ownsReader && tc.ownsGenerator {
				assert.Equal(t, len(side.result.External), 0)
				return
			}
			assert.FatalIf(t, len(side.result.External) != 1, "warnings = %v, want one", side.result.External)
			assert.Equal(t, side.result.External[0].Category, manifest.WarnExternalSource)
		})
	}
}

func TestPrepareFailures_RequiredNamedLookup(t *testing.T) {
	t.Parallel()
	owner := (&manifest.Kustomization{Name: "package", Namespace: "apps"}).Named()
	unrelated := (&manifest.Kustomization{Name: "unrelated", Namespace: "apps"}).Named()
	request := (&manifest.HelmRelease{Name: "release", Namespace: "apps"}).Named()
	failure := store.StatusInfo{Status: store.StatusFailed}
	for _, tc := range []struct {
		name    string
		matched bool
		roots   []manifest.NamedResource
	}{
		{name: "missing"},
		{name: "matched", matched: true, roots: []manifest.NamedResource{request}},
		{name: "matched without output roots", matched: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(&Admission{
				Needed: []manifest.NamedResource{owner}, lookups: []manifest.NamedResource{owner},
			}, true)
			side := &selectionSide{
				run: Snapshot{
					Store:    store.New(),
					Failed:   map[manifest.NamedResource]store.StatusInfo{owner: failure, unrelated: failure},
					Decision: decision,
				},
				roots:  tc.roots,
				result: &Result{LookupMatched: tc.matched},
			}
			side.prepareFailures([]manifest.NamedResource{request}, nil)
			_, fatal := side.result.Failed[owner]
			assert.Equal(t, fatal, !tc.matched)
			_, fatal = side.result.Failed[unrelated]
			assert.Equal(t, fatal, false)
		})
	}
}

func TestProject_LookupFailureSupersedesOtherDiagnostics(t *testing.T) {
	t.Parallel()
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "candidate", manifest.NamedResource{})
	pkg := manifest.DiagnosticFor(owner.Named(), owner)
	failure := store.FailureStatus(&manifest.DependencyFailedError{
		Parent: pkg.ID, Failed: []manifest.NamedResource{manifest.BootstrapSourceID},
	})
	target := (&manifest.HelmRelease{Name: "target"}).Named()
	request := Request{Roots: []manifest.NamedResource{target}}
	lookups := []manifest.NamedResource{pkg.ID}
	for _, tc := range []struct {
		name            string
		requiredLookups []manifest.NamedResource
	}{
		{name: "unrelated failure"},
		{name: "blocked lookup owner", requiredLookups: lookups},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(&Admission{
				Needed: lookups, lookups: tc.requiredLookups,
			}, true)
			runs := []Snapshot{{
				Store: store.New(), Complete: true, HasResult: true, Decision: decision,
				Failed:         map[manifest.NamedResource]store.StatusInfo{pkg.ID: failure},
				Blocked:        map[manifest.NamedResource][]manifest.NamedResource{pkg.ID: failure.BlockedBy()},
				LookupEvidence: []LookupFailure{{Package: pkg, Path: "/entry", Cause: errors.New("bad candidate")}},
			}}
			results, errs := Project(runs, nil, request)
			assert.FatalIf(t, errs.Selected == nil, "missing projected failure")
			assert.Equal(t, strings.Count(errs.Selected.Error(), "cannot establish membership"), 1)
			assert.Equal(t, strings.Count(errs.Selected.Error(), failure.Message), 1)
			assert.Diff(t, results[0].Causes, map[manifest.NamedResource][]manifest.NamedResource{pkg.ID: nil})
			assert.Equal(t, len(results[0].Blocked), 0)
			assert.Equal(t, len(results[0].Unrelated), 0)
		})
	}
}
