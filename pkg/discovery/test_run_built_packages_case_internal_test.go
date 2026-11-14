package discovery

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/source/cache"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testRunBuiltPackagesCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()
	persistent := cache.RenderPersistenceDisabledReason() == ""

	const root = "/virtual/prepared-root"
	input := fstest.MapFS{
		"a/kustomization.yaml": {
			Data: []byte(
				"namespace: apps\nresources: [cm.yaml, driver.yaml, source.yaml]\nnameSuffix: -dev\npatches: [{path: patch.yaml}]\n",
			),
		},
		"a/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {A: base}\n"),
		},
		"a/patch.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {B: patched}\n"),
		},
		"a/driver.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"driver",
					"apps",
					"  path: ./output\n  sourceRef: {kind: GitRepository, name: cluster-dev}\n",
				),
			),
		},
		"a/source.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://fixture.invalid/self}\n",
			),
		},
		"b/kustomization.yaml": {
			Data: []byte("configMapGenerator: [{name: generated, literals: [C=second]}]\n"),
		},
	}
	if (*testCaseInput) == "." {
		input["kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [a, b]\n")}
	}
	cacheDir := t.TempDir()
	var previousDocs []map[string]any
	var previousIndex *manifest.DiscoveryIndex
	var previousHistory map[manifest.NamedResource]manifest.DiscoveryRecord
	var previousWarnings []manifest.Warning
	wantValue := "base"
	for _, lifetime := range []string{"uncached", "cold", "warm-1", "warm-2", "changed", "changed-uncached"} {
		t.Logf("cache lifetime: %s", lifetime)
		if lifetime == "changed" {
			wantValue = "edited"
			input["a/cm.yaml"].Data = []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {A: edited}\n",
			)
		}
		dir := cacheDir
		if lifetime == "uncached" || lifetime == "changed-uncached" {
			dir = ""
		}
		testReceiptTreesResult1 := testReceiptTrees(t, dir, persistent)
		trees, closeCache := testReceiptTreesResult1.trees, testReceiptTreesResult1.close
		st := store.New()
		source := &store.SourceArtifact{LocalPath: root, FS: input}
		st.SetArtifact(manifest.BootstrapSourceID, source)
		d := newDiscoverer(
			&Config{
				Input:    source,
				Path:     filepath.Join(root, (*testCaseInput)),
				Store:    st,
				Trees:    trees,
				SelfURLs: []string{"https://fixture.invalid/self"},
			},
		)
		_, err := d.resolveTestSources(t.Context(), false)
		assert.NoError(t, err)
		owner := manifest.NewEntryKustomization(
			manifest.BootstrapSourceID,
			(*testCaseInput),
			manifest.NamedResource{},
		)
		req := preparedRequest(t, &d.cfg, owner, source)
		if lifetime == "changed" {
			assert.FatalIf(
				t,
				previousPackageHistory(t, trees, &req).Records != nil,
				"%v",
				"changed consumed resource retained stale accepted history",
			)
		}
		if strings.HasPrefix(lifetime, "warm-") {
			assert.Diff(t, packageHistory(t, trees, &req, persistent), previousHistory)
		}
		assert.NoError(t, d.loadManifestsTest(t.Context(), root, true))
		assert.Equal(t, d.binding.Producer, owner.Named())
		assertPreparedPackageOutputsResult2 := d.assertPreparedPackageOutputs(t, testCaseInput, owner, wantValue)
		docs, wantHistory := assertPreparedPackageOutputsResult2.documents, assertPreparedPackageOutputsResult2.history
		assert.NoError(t, d.loader.FinalizeIndex(t.Context(), root, true))
		assert.NoError(t, d.acceptPrepared(t.Context()))
		expectedHistory := wantHistory
		if !persistent || dir == "" {
			expectedHistory = nil
		}
		assert.Diff(t, packageHistory(t, trees, &req, persistent && dir != ""), expectedHistory)
		d.assertPreparedPackageReaders(t, testCaseInput)
		if lifetime == "uncached" || lifetime == "changed" {
			previousDocs, previousIndex, previousWarnings = docs, d.loader.Index, st.Warnings()
		} else {
			assert.Diff(t, docs, previousDocs)
			{
				diff := cmp.Diff(previousIndex, d.loader.Index, cmp.Comparer(manifest.Equal))
				assert.ErrorIf(t, diff != "", "discovery index mismatch (-want +got):\n%s", diff)
			}
			assert.Diff(t, st.Warnings(), previousWarnings)
		}
		previousHistory = expectedHistory
		closeCache()
	}
}

type testRunEntryOwnerEligibilityCaseTailInputs struct {
	testCaseInput **string
	ready         **manifest.Kustomization
	d             **Session
	owner         **manifest.Kustomization
	st            **store.Store
	root          string
}

type packageOutputs struct {
	documents []map[string]any
	history   map[manifest.NamedResource]manifest.DiscoveryRecord
}

func (d *Session) assertPreparedPackageOutputs(
	t *testing.T,
	testCaseInput *string,
	owner *manifest.Kustomization,
	wantValue string,
) packageOutputs {
	t.Helper()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings-dev"}
	batch := d.loader.PromotionDocuments(id)
	wantCount := 4
	if (*testCaseInput) == "a" {
		wantCount = 3
	}
	assert.Equal(t, len(batch), wantCount)
	wantHistory := map[manifest.NamedResource]manifest.DiscoveryRecord{}
	var docs []map[string]any
	if len(batch) != 0 {
		docs = make([]map[string]any, 0, len(batch))
	}
	childValues := batch
	for childIndex := range childValues {
		child := childValues[childIndex]
		docs = append(docs, child.Doc)
		record, ok := d.loader.Index.Lookup(child.Obj.Named())
		assert.FatalIf(t, !ok || record.Parent != d.binding.Producer ||
			!slices.ContainsFunc(
				record.Productions,
				func(p manifest.Production) bool { return p.Owner == d.binding.Producer },
			), "unowned prepared output: %+v", record)
		accepted := manifest.DiscoveryRecord{
			Productions: []manifest.Production{{Owner: owner.Named(), Source: d.binding.Source}},
		}
		if child.Obj.Named() == testutil.KustomizationID("apps", "driver-dev") {
			accepted.Application = `{"apiVersion":"kustomize.toolkit.fluxcd.io/v1","kind":"Kustomization","metadata":{"name":"driver-dev","namespace":"apps"},"spec":{"path":"./output","sourceRef":{"kind":"GitRepository","name":"cluster-dev"}}}`
			source := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "cluster-dev",
			}
			accepted.Refs = []manifest.NamedResource{source}
			accepted.ContentRoots = map[manifest.NamedResource][]string{source: {"output"}}
		}
		wantHistory[child.Obj.Named()] = accepted
		if child.Obj.Named() == id {
			cm := child.Obj.(*manifest.ConfigMap)
			assert.Diff(t, cm.Data, map[string]any{"A": wantValue, "B": "patched"})
		}
	}
	return packageOutputs{documents: docs, history: wantHistory}
}

func (d *Session) assertPreparedPackageReaders(t *testing.T, testCaseInput *string) {
	t.Helper()
	files := []string{"a/kustomization.yaml", "a/cm.yaml", "a/patch.yaml", "a/driver.yaml", "a/source.yaml"}
	if (*testCaseInput) == "." {
		files = append(files, "b/kustomization.yaml")
	}
	for _, file := range files {
		assert.ErrorIf(t, !slices.ContainsFunc(
			d.loader.Index.Readers[file],
			func(r manifest.FileReader) bool { return r.Reader == d.binding.Producer },
		), "root missing actual reader for %s", file)
	}
}
