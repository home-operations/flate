package loader

import (
	"fmt"
	"path/filepath"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestEachIndexedKustomization_UnavailableSource(t *testing.T) {
	t.Parallel()
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			ks := &manifest.Kustomization{
				Name: "owner", Namespace: "apps", Path: "./",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "remote"},
			}
			id := ks.Named()
			l.Store.AddObject(&manifest.OCIRepository{Name: "remote", Namespace: "apps"})
			record := manifest.DiscoveryRecord{File: "owner.yaml", Path: "owner.yaml", Authored: id, Declaration: ks}
			if prepared {
				record.Builder = "kustomization.yaml"
			}
			l.Index.Records[id] = record
			l.candidates = map[manifest.NamedResource]store.Child{id: {Obj: ks}}
			l.locations = map[manifest.NamedResource]authoredOccurrence{id: {
				Identity: id, Authored: id, object: ks, requiresBuild: true,
				facts: &occurrenceFacts{structural: ks},
			}}
			var objects []manifest.BaseManifest
			l.eachIndexedKustomization(func(child store.Child) { objects = append(objects, child.Obj) })
			assert.Equal(t, len(objects), 1)
			if len(objects) == 1 {
				assert.Equal(t, objects[0], manifest.BaseManifest(ks))
			}
			assert.Equal(t, l.Store.GetObject(id), manifest.BaseManifest(nil))
			assert.Equal(t, l.Index.Records[id].Declaration, manifest.BaseManifest(ks))
		})
	}
}

func TestPackageClaims_UnavailableSource(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	ks := &manifest.Kustomization{
		Name: "owner", Namespace: "apps", Path: "./apps/b",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "remote"},
	}
	file := filepath.Join(l.SourceRoot, "owner.yaml")
	l.documents.cache[file] = document{eligible: selection.RootEligible}
	claims := l.packageClaims([]selection.OccurrenceFacts{{
		Identity: ks.Named(), Authored: ks.Named(), Object: ks,
		Declaration: selection.OccurrenceID{Source: l.SourceRoot, File: "owner.yaml"},
	}})
	assert.Diff(t, claims, []string(nil))
}

func TestFilterClaimedPackageRoots_UnavailableSource(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	ks := &manifest.Kustomization{Name: "owner", Path: "./apps/b"}
	roots := map[string]bool{"apps/b": false}
	view := sourceScope{external: true}
	l.filterClaimedPackageRoots("", roots, ks, &view)
	assert.Diff(t, roots, map[string]bool{"apps/b": false})
	assert.Equal(t, l.documents.cache["apps/b"].buildExcluded, false)
}

func TestVisitKustomizationScope_UnavailableSource(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.ensureScopes()
	ks := &manifest.Kustomization{Name: "owner", Path: "./apps/b"}
	l.scopes.inputs = []scopeInput{{owner: ks}}
	l.scopes.sources = []sourceScope{{external: true}}
	l.visitKustomizationScope("", ks, 0)
	assert.Equal(t, len(l.scopes.contributions), 0)
	assert.Equal(t, len(l.documents.cache), 0)
}
