package discovery

import (
	"io/fs"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPlan_AggregateBootstrapProbe(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"apps/kustomization.yaml": {Data: []byte("resources: [owner.yaml]\n")},
		"apps/owner.yaml": {Data: []byte(testutil.KustomizationYAML(
			"app", "flux-system", "  path: ./missing\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		))},
	}
	view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: files})
	assert.NoError(t, err)
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, LocalRoot: root, FS: view}
	entries, err := Plan(t.Context(), Config{Input: input, Path: root})
	assert.NoError(t, err)
	assert.FatalIf(t, len(entries) != 1 || !entries[0].aggregate, "expected a prepared aggregate: %v", entries)
	entry := &entries[0]
	bootstrap := entry.prepared.sources[manifest.BootstrapSourceID]
	assert.FatalIf(
		t, !bootstrap.synthetic || bootstrap.input == entry.Input,
		"expected a bootstrap binding from planning",
	)

	replacement := &store.SourceArtifact{
		Provider:  store.ProviderInput,
		LocalPath: root,
		LocalRoot: root,
		FS: fstest.MapFS{
			"source.yaml": {Data: []byte(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: 'https://fixture.invalid/repo', ref: {branch: main}}
`)},
		},
	}
	for _, tc := range []struct {
		name      string
		input     *store.SourceArtifact
		synthetic bool
	}{
		{name: "prepared aggregate", input: entry.Input, synthetic: true},
		{name: "replacement input", input: replacement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var work manifest.WorkRecorder
			session, err := Begin(t.Context(), Config{
				Store: store.New(), Path: entry.Path, Input: tc.input, Entry: entry, Work: work.Scope(),
			})
			assert.NoError(t, err)
			t.Cleanup(session.Close)
			var files uint64
			for _, record := range work.Scope().Snapshot(0) {
				files += record.Counts.FileEntries
			}
			assert.Equal(t, files == 0, tc.synthetic)
			resolution := session.resolvedSources.snapshot()[manifest.BootstrapSourceID]
			assert.Equal(t, resolution.synthetic, tc.synthetic)
			assert.Equal(t, resolution.input, tc.input)
		})
	}
}

func TestPlan_ParsedInputHandoff(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{}
	reads := map[string]*atomic.Int64{}
	for _, side := range []string{"left", "right"} {
		files[side+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [bootstrap.yaml, owner.yaml]\n")}
		files[side+"/bootstrap.yaml"] = &fstest.MapFile{Data: []byte(
			"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: bootstrap, namespace: apps}\nspec:\n  path: ./" + side + "\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		)}
		files[side+"/owner.yaml"] = &fstest.MapFile{
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: apps}\nspec:\n  path: ./" + side + "/leaf\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			),
		}
		files[side+"/leaf/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\n")}
		files[side+"/leaf/cm.yaml"] = &fstest.MapFile{
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {side: " + side + "}\n",
			),
		}
		reads[side+"/owner.yaml"] = &atomic.Int64{}
		reads[side+"/leaf/cm.yaml"] = &atomic.Int64{}
	}
	view, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: entryReadFS{FS: files, reads: reads}},
	)
	assert.NoError(t, err)
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: view}
	var recorder manifest.WorkRecorder
	entries, err := Plan(t.Context(), Config{Input: input, Path: root, Work: recorder.Scope()})
	var counts manifest.WorkCounts
	for _, record := range recorder.Scope().Snapshot(0) {
		counts.Add(&record.Counts)
	}
	assert.Equal(t, counts.OwnerPrepares, uint64(5))
	assert.Equal(t, counts.FileEntries, uint64(43))
	assert.Equal(t, counts.ReaderEdges, uint64(24))
	assert.NoError(t, err)
	// Request Ruling 1 requires explicit bootstrap roots for these isolated handoffs.
	assert.FatalIf(t, len(entries) != 2 || len(entries[0].prepared.aliasViews) == 0,
		"expected two prepared alias-backed entries: %+v", entries)

	before := map[string]int64{}
	for name, count := range reads {
		before[name] = count.Load()
	}
	t.Cleanup(func() {
		for name, count := range reads {
			assert.ErrorIf(t, count.Load() != before[name],
				"%s content reads = %d, want %d", name, count.Load(), before[name])
		}
	})
	t.Run("different content", func(t *testing.T) {
		t.Parallel()
		changed := *input
		changed.Content = "next-generation"
		d := newDiscoverer(&Config{Input: &changed, Entry: &entries[0], Path: entries[0].Path, Store: store.New()})
		assert.Equal(t, d.loader.Options.ParsedInputs == nil, true)
	})
	t.Run("native admissions", func(t *testing.T) {
		t.Parallel()
		testPlanParsedInputHandoffCase1(t, &entries, &input, &root)
	})
}

func testPlanParsedInputHandoffCase1(t *testing.T, entries *[]Entry, input **store.SourceArtifact, root *string) {
	t.Helper()

	for _, entry := range *entries {
		t.Run(entry.Path, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			d := newDiscoverer(&Config{Input: (*input), Entry: &entry, Path: entry.Path, Store: st})
			assert.FatalIf(
				t,
				d.loader.Options.ParsedInputs == nil,
				"%s",
				"matching input did not borrow its parsed evidence",
			)

			_, err := d.resolveTestSources(t.Context(), false)
			assert.NoError(t, err)
			assert.NoError(t, d.loadManifestsTest(t.Context(), (*root), false))
			owner := d.loader.CatalogObject(manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "owner"}).(*manifest.Kustomization)
			side := strings.Split(owner.Path, "/")[1]
			// The owner ruling restores #1379's owner-path and sibling-isolation expectations.
			assert.Equal(t, entry.Path, filepath.Join(*root, side))
			_, found := d.loader.Index.Lookup(
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "payload"},
			)
			assert.Equal(t, found, true)
			for i := range d.loader.Index.Records {
				record := d.loader.Index.Records[i]
				assert.FatalIf(t, record.File != "" && !strings.Contains(record.File, side+"/"),
					"entry admitted sibling evidence: %+v", record)
			}
			for key, view := range entry.prepared.aliasViews {
				assert.Equal(t, d.aliasViews[key], view)
				delete(d.aliasViews, key)
				assert.FatalIf(
					t,
					entry.prepared.aliasViews[key].artifact == nil,
					"%s",
					"native admission mutated the prepared alias map",
				)
			}
		})
	}
}

func TestRun_PreparedInputAdmission(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [owner.yaml, secret.yaml]\n")},
		"owner.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: planned, namespace: apps}\nspec: {path: ./leaf}\n",
			),
		},
		"secret.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: Secret\nmetadata: {name: secret, namespace: apps}\nstringData: {KEY: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n",
			),
		},
	}
	view, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: files},
	)
	assert.NoError(t, err)
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, LocalRoot: root, FS: view}
	entries, err := Plan(t.Context(), Config{Input: input})
	assert.NoError(t, err)
	for _, tc := range []struct {
		name     string
		input    *store.SourceArtifact
		wantName string
	}{
		{name: "matching admission", input: input, wantName: "planned"},
		{name: "different source", wantName: "authoritative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.input == nil {
				other, _, err := gittree.New(
					t.Context(),
					nil,
					plumbing.ZeroHash,
					gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
						"kustomization.yaml": {Data: []byte("resources: [owner.yaml]\n")},
						"owner.yaml": {
							Data: []byte(
								"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: authoritative, namespace: apps}\nspec: {path: ./other}\n",
							),
						},
					}},
				)
				assert.NoError(t, err)
				tc.input = &store.SourceArtifact{
					Provider:  store.ProviderInput,
					LocalPath: root,
					LocalRoot: root,
					FS:        other,
				}
			}
			st := store.New()
			result, err := Run(t.Context(), testConfig(t, Config{Input: tc.input, Entry: &entries[0], Store: st}))
			assert.NoError(t, err)
			id := manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Namespace: "apps",
				Name:      tc.wantName,
			}
			if record, ok := result.Index.Lookup(
				id,
			); !ok || record.Parent != result.Binding.Producer ||
				st.GetObject(id) != nil {
				t.Fatalf("authoritative input ownership lost: %+v", result.Index.Records)
			}
			if tc.name != "different source" {
				secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "secret"}
				secret, ok := result.Index.Records[secretID].Declaration.(*manifest.Secret)
				assert.FatalIf(t, !ok || st.GetObject(secretID) != nil,
					"prepared secret = %+v, published=%v", secret, st.GetObject(secretID))

				assert.Equal(t, secret.HasKeys(), true)
				value, lookupErr := secret.Value("KEY")
				assert.NoError(t, lookupErr)
				assert.Equal(t, value.Found && value.Encrypted, true)
				assert.Equal(t, value.Value, "")
			}
		})
	}
}

type entryReadFS struct {
	fs.FS

	reads map[string]*atomic.Int64
}

func (f entryReadFS) Open(name string) (fs.File, error) {
	if count := f.reads[name]; count != nil {
		count.Add(1)
	}
	return f.FS.Open(name)
}

func (f entryReadFS) Stat(name string) (fs.FileInfo, error) { return fs.Stat(f.FS, name) }

func (f entryReadFS) ReadDir(name string) ([]fs.DirEntry, error) { return fs.ReadDir(f.FS, name) }
