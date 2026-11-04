package discovery

import (
	"io/fs"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestPlan_ParsedInputHandoff(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{}
	reads := map[string]*atomic.Int64{}
	for _, side := range []string{"left", "right"} {
		files[side+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [owner.yaml]\n")}
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
	input := &store.SourceArtifact{LocalPath: root, FS: view}
	entries, err := Plan(t.Context(), Config{Input: input, Path: root, WipeSecrets: true})
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(entries) != 1 || len(entries[0].prepared.aliasViews) == 0,
		"expected one literal entry with prepared aliases: %+v",
		entries,
	)
	before := map[string]int64{}
	for name, count := range reads {
		before[name] = count.Load()
	}
	t.Cleanup(func() {
		for name, count := range reads {
			if count.Load() != before[name] {
				t.Errorf("%s content reads = %d, want %d", name, count.Load(), before[name])
			}
		}
	})
	t.Run("different content", func(t *testing.T) {
		t.Parallel()
		testPlanParsedInputHandoffDifferentContent(
			t,
			testPlanParsedInputHandoffDifferentContentInputs{input: &input, entries: &entries},
		)
	},
	)
	t.Run("native admissions", func(t *testing.T) {
		t.Parallel()
		testPlanParsedInputHandoffNativeAdmissions(
			t,
			testPlanParsedInputHandoffNativeAdmissionsInputs{root: &root, input: &input, entries: &entries},
		)
	},
	)
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
	input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: view}
	entries, err := Plan(t.Context(), Config{Input: input, WipeSecrets: true})
	assert.NoError(t, err)
	for _, tc := range []struct {
		name     string
		input    *store.SourceArtifact
		wipe     bool
		wantName string
	}{
		{name: "matching admission", input: input, wipe: true, wantName: "planned"},
		{name: "parse policy", input: input, wantName: "planned"},
		{name: "different source", wantName: "authoritative", wipe: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRunPreparedInputAdmissionCase(
				t,
				testRunPreparedInputAdmissionCaseInputs{root: &root, entries: &entries, tc: &tc},
			)
		},
		)
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

type testPlanParsedInputHandoffDifferentContentInputs struct {
	input   **store.SourceArtifact
	entries *[]Entry
}

func testPlanParsedInputHandoffDifferentContent(
	t *testing.T,
	testCaseInput testPlanParsedInputHandoffDifferentContentInputs,
) {
	t.Helper()

	changed := *(*testCaseInput.input)
	changed.Content = "next-generation"
	d := newDiscoverer(
		&Config{
			Input:       &changed,
			Entry:       &(*testCaseInput.entries)[0],
			Path:        (*testCaseInput.entries)[0].Path,
			Store:       store.New(),
			WipeSecrets: true,
		},
	)
	assert.Equal(t, d.loader.Options.ParsedInputs == nil, true)
}

type testPlanParsedInputHandoffNativeAdmissionsInputs struct {
	root    *string
	input   **store.SourceArtifact
	entries *[]Entry
}

func testPlanParsedInputHandoffNativeAdmissions(
	t *testing.T,
	testCaseInput testPlanParsedInputHandoffNativeAdmissionsInputs,
) {
	t.Helper()

	for _, entry := range *testCaseInput.entries {
		t.Run(entry.Path, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			d := newDiscoverer(
				&Config{Input: (*testCaseInput.input), Entry: &entry, Path: entry.Path, Store: st, WipeSecrets: true},
			)
			assert.FatalIf(
				t,
				d.loader.Options.ParsedInputs == nil,
				"%v",
				"matching input did not borrow its parsed evidence",
			)
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.NoError(t, err)
			assert.NoError(t, d.loadManifests(t.Context(), (*testCaseInput.root), false))
			assert.Equal(t, entry.Path, (*testCaseInput.root))
			for _, side := range []string{"left", "right"} {
				{
					_, ok := d.loader.Index.Lookup(
						manifest.NamedResource{
							Kind:      manifest.KindConfigMap,
							Namespace: "apps",
							Name:      "payload",
						},
					)
					assert.FatalIf(t, !ok, "%s leaf evidence missing", side)
				}
			}
			for key, view := range entry.prepared.aliasViews {
				assert.Equal(t, d.aliasViews[key], view)
				delete(d.aliasViews, key)
				assert.FatalIf(
					t,
					entry.prepared.aliasViews[key] == nil,
					"%v",
					"native admission mutated the prepared alias map",
				)
			}
		})
	}
}

type testRunPreparedInputAdmissionCaseInputs struct {
	root    *string
	entries *[]Entry
	tc      *struct {
		name string

		input    *store.SourceArtifact
		wipe     bool
		wantName string
	}
}

func testRunPreparedInputAdmissionCase(t *testing.T, testCaseInput testRunPreparedInputAdmissionCaseInputs) {
	t.Helper()

	if testCaseInput.tc.input == nil {
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
		testCaseInput.tc.input = &store.SourceArtifact{
			LocalPath: (*testCaseInput.root),
			LocalRoot: (*testCaseInput.root),
			FS:        other,
		}
	}
	st := store.New()
	result, err := Run(
		t.Context(),
		Config{
			Input:       testCaseInput.tc.input,
			Entry:       &(*testCaseInput.entries)[0],
			Store:       st,
			Trees:       testTrees(t),
			WipeSecrets: testCaseInput.tc.wipe,
		},
	)
	assert.NoError(t, err)
	id := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      testCaseInput.tc.wantName,
	}
	if record, ok := result.Index.Lookup(
		id,
	); !ok || record.Parent != result.Binding.Producer ||
		st.GetObject(id) != nil {
		t.Fatalf("authoritative input ownership lost: %+v", result.Index.Records)
	}
	if testCaseInput.tc.name != "different source" {
		secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "secret"}
		secret, ok := result.Index.Records[secretID].Declaration.(*manifest.Secret)
		if !ok || st.GetObject(secretID) != nil {
			t.Fatalf("prepared secret = %+v, published=%v", secret, st.GetObject(secretID))
		}
		assert.Equal(t, secret.HasPlaceholder(), testCaseInput.tc.wipe)
	}
}
