package loader

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestOccurrenceClaims_CapturedSubsetAndIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	pin := filepath.Join(root, ".artifacts/pin")
	st := store.New()
	input := fstest.MapFS{}
	source := &manifest.GitRepository{Name: "pin", Namespace: "sources"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: pin, LocalRoot: root, Key: "committed", Content: "committed", FS: input})
	st.AddObject(&manifest.Kustomization{Name: "stored", Namespace: "apps", Path: "unrelated"})
	for _, side := range []string{"one", "two"} {
		input[side+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: " + side + "\nnamePrefix: " + side + "-\nresources: [owners.yaml]\n")}
		paths := []string{"captured-a", "captured-b"}
		if side == "two" {
			paths = []string{"captured-c", "excluded"}
		}
		body := testutil.KustomizationYAML("owner", "authored", "  path: ./"+paths[0]+"\n  sourceRef: {kind: GitRepository, name: pin, namespace: sources}\n") + "---\n" + testutil.KustomizationYAML("owner", "authored", "  path: ./"+paths[1]+"\n  sourceRef: {kind: GitRepository, name: pin, namespace: sources}\n")
		input[side+"/owners.yaml"] = &fstest.MapFile{Data: []byte(body)}
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input})
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	one, err := l.Occurrences(t.Context(), filepath.Join(root, "one"), "")
	assert.NoError(t, err)
	two, err := l.Occurrences(t.Context(), filepath.Join(root, "two"), "")
	assert.NoError(t, err)
	assert.Equal(t, len(one), 2)
	assert.Equal(t, len(two), 2)
	if len(one) != 2 || len(two) != 2 {
		t.Fatal("same-identity declarations lost occurrence evidence")
	}
	repeated, err := l.Occurrences(t.Context(), filepath.Join(root, "one"), "")
	assert.NoError(t, err)
	assert.Equal(t, slices.Equal(repeated, one), true)
	repeated[0].Identity.Name = "caller-owned"
	assert.Equal(t, one[0].Identity.Name, "one-owner")
	assert.Equal(t, len(l.scopes.projections), 2)
	_, err = l.Load(t.Context(), filepath.Join(pin, "one"))
	assert.NoError(t, err)
	mounted, err := l.Occurrences(t.Context(), filepath.Join(pin, "one"), "")
	assert.NoError(t, err)
	assert.Equal(t, len(mounted), 2)
	for i, o := range mounted {
		assert.Equal(t, o.Declaration, OccurrenceID{Source: pin, File: "one/owners.yaml", Document: i})
		assert.Equal(t, o.Identity, one[i].Identity)
		assert.Equal(t, o.Authored, one[i].Authored)
		assert.Equal(t, o.Object.Named(), o.Authored)
	}
	assert.Equal(t, len(l.scopes.projections), 2)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = l.Occurrences(ctx, filepath.Join(root, "one"), "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("warm occurrence cancellation: %v", err)
	}
	captured := append(slices.Clone(one), two[0])
	before := slices.Clone(captured)
	objects := st.ListObjects("", "")
	claims := l.OccurrenceClaims(root, captured)
	var want []manifest.KSClaim
	for _, prefix := range []string{".artifacts/pin/", ""} {
		for i, path := range []string{"captured-a", "captured-b", "captured-c"} {
			want = append(want, manifest.KSClaim{ID: captured[i].Identity, Prefix: prefix + path + "/"})
		}
	}
	assert.Diff(t, claims, want)
	assert.Equal(t, len(l.OccurrenceClaims(root, nil)), 0)
	assert.Equal(t, len(st.ListObjects("", "")), len(objects))
	for _, obj := range objects {
		assert.Equal(t, st.GetObject(obj.Named()), obj)
	}
	for i, occurrence := range captured {
		assert.Equal(t, occurrence.Declaration, before[i].Declaration)
		assert.Equal(t, occurrence.Identity, before[i].Identity)
		assert.Equal(t, occurrence.Authored, testutil.KustomizationID("authored", "owner"))
		assert.Equal(t, occurrence.Object, before[i].Object)
		assert.Equal(t, occurrence.Object.Named(), occurrence.Authored)
	}
	assert.Equal(t, captured[0].Identity, testutil.KustomizationID("one", "one-owner"))
	assert.Equal(t, captured[1].Identity, captured[0].Identity)
	assert.Equal(t, captured[2].Identity, testutil.KustomizationID("two", "two-owner"))
	assert.Diff(t, []OccurrenceID{captured[0].Declaration, captured[1].Declaration, captured[2].Declaration}, []OccurrenceID{
		{Source: root, File: "one/owners.yaml", Document: 0},
		{Source: root, File: "one/owners.yaml", Document: 1},
		{Source: root, File: "two/owners.yaml", Document: 0},
	})

	t.Run("retained failure before successful declaration", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		input := fstest.MapFS{
			"overlay/kustomization.yaml": {Data: []byte("namespace: selected\nnamePrefix: pre-\nnameSuffix: -post\nresources: [owners.yaml]\n")},
			"overlay/owners.yaml": {Data: []byte(testutil.KustomizationYAML("failed", "authored", "  path: []\n") + "---\n" +
				testutil.KustomizationYAML("owner", "authored", "  path: ./captured\n"))},
		}
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
		l := New(st)
		l.SourceRoot = root
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
		file := filepath.Join(root, "overlay/owners.yaml")
		children := l.documents.cache[file].objects
		if len(children) != 2 || children[0].Obj != nil || !errors.Is(children[0].outcome.Err, manifest.ErrInput) {
			t.Fatalf("failed document was not retained before its successful sibling: %+v", children)
		}
		failure := children[0].outcome.Err
		assert.Equal(t, children[0].outcome.Ordinal, 0)
		assert.Equal(t, children[1].outcome.Ordinal, 1)
		want := testutil.KustomizationID("selected", "pre-owner-post")
		var first []Occurrence
		for range 2 {
			got, err := l.Occurrences(t.Context(), filepath.Join(root, "overlay"), "")
			assert.NoError(t, err)
			if len(got) != 1 {
				t.Fatalf("successful declaration count: got %d, want 1", len(got))
			}
			assert.Equal(t, got[0].Declaration, OccurrenceID{Source: root, File: "overlay/owners.yaml", Document: 1})
			assert.Equal(t, got[0].Identity, want)
			assert.Equal(t, got[0].Authored, testutil.KustomizationID("authored", "owner"))
			assert.Equal(t, got[0].Object.Named(), got[0].Authored)
			assert.Equal(t, len(l.scopes.projections), 1)
			if first == nil {
				first = got
			} else {
				assert.Equal(t, slices.Equal(got, first), true)
			}
		}
		assert.Diff(t, l.OccurrenceClaims(root, first), []manifest.KSClaim{{ID: want, Prefix: "captured/"}})
		assert.NoError(t, l.FinalizeIndex(t.Context(), root))
		failed := testutil.KustomizationID("authored", "failed")
		for _, record := range l.Index.Files() {
			assert.Equal(t, record.Authored == failed, false)
			for _, production := range record.Productions {
				assert.Equal(t, production.Authored == failed, false)
			}
		}
		retained := l.documents.cache[file].objects
		assert.Equal(t, len(retained), 2)
		assert.Equal(t, retained[0].Obj, nil)
		assert.Equal(t, retained[0].outcome.Ordinal, 0)
		assert.Equal(t, retained[0].outcome.Err, failure)
	})
}

func TestPackages_ScanProvenance(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := fstest.MapFS{}
	for _, dir := range []string{"one/pkg", "two/pkg"} {
		input[dir+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\n")}
		input[dir+"/cm.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values}\n")}
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	packages, err := l.Packages(t.Context())
	assert.NoError(t, err)
	if len(packages) != 2 {
		t.Fatalf("package evidence: %+v", packages)
	}
	for i, dir := range []string{"one", "two"} {
		assert.Equal(t, packages[i].Path, filepath.ToSlash(filepath.Join(dir, "pkg")))
		assert.Diff(t, packages[i].Scans, []string{".", dir, dir + "/pkg"})
	}
}

func TestProjectIdentity_NamespaceOnly(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		view          graphVisit
		object        manifest.BaseManifest
		want          manifest.NamedResource
		noAllocation  bool
	}{
		{name: "untransformed", version: "v1", object: &manifest.ConfigMap{Name: "values"}, want: manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "values"}, noAllocation: true},
		{name: "namespaced", view: graphVisit{namespace: "selected"}, version: "v1", object: &manifest.ConfigMap{Name: "values", Namespace: "authored"}, want: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "selected", Name: "values"}, noAllocation: true},
		{name: "foreign Namespace", view: graphVisit{namespace: "selected"}, version: "foreign.example/v2", object: &manifest.RawObject{APIVersion: "foreign.example/v2", Kind: "Namespace", Name: "_"}, want: manifest.NamedResource{Group: "foreign.example", Kind: "Namespace", Namespace: "selected", Name: "_"}, noAllocation: true},
		{name: "core Namespace", view: graphVisit{namespace: "selected"}, version: "v1", object: &manifest.RawObject{APIVersion: "v1", Kind: "Namespace", Name: "_"}, want: manifest.NamedResource{Kind: "Namespace", Name: "selected"}},
		{name: "core Namespace exact version", view: graphVisit{namespace: "selected"}, version: "v2", object: &manifest.RawObject{APIVersion: "v2", Kind: "Namespace", Name: "_"}, want: manifest.NamedResource{Kind: "Namespace", Namespace: "selected", Name: "_"}, noAllocation: true},
		{name: "cluster scoped", view: graphVisit{namespace: "selected"}, version: "rbac.authorization.k8s.io/v1", object: &manifest.RawObject{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "policy"}, want: manifest.NamedResource{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "policy"}},
		{name: "prefix and suffix", view: graphVisit{namePrefix: "pre-", nameSuffix: "-post"}, version: "v1", object: &manifest.ConfigMap{Name: "values", Namespace: "authored"}, want: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "authored", Name: "pre-values-post"}},
		{name: "core Namespace exclusions", view: graphVisit{namespace: "selected", namePrefix: "pre-", nameSuffix: "-post"}, version: "v1", object: &manifest.RawObject{APIVersion: "v1", Kind: "Namespace", Name: "_"}, want: manifest.NamedResource{Kind: "Namespace", Name: "selected"}},
		{name: "foreign Namespace exclusions", view: graphVisit{namespace: "selected", namePrefix: "pre-", nameSuffix: "-post"}, version: "foreign.example/v2", object: &manifest.RawObject{APIVersion: "foreign.example/v2", Kind: "Namespace", Name: "_"}, want: manifest.NamedResource{Group: "foreign.example", Kind: "Namespace", Namespace: "selected", Name: "_"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &Loader{}
			view := tc.view
			got, err := l.projectIdentity(tc.object, view, tc.version)
			assert.NoError(t, err)
			assert.Equal(t, got, tc.want)
			if tc.noAllocation {
				assert.Equal(t, l.scopes, nil)
			} else {
				assert.Equal(t, len(l.scopes.projections), 1)
			}
			allocations := testing.AllocsPerRun(1, func() {
				got, err = l.projectIdentity(tc.object, view, tc.version)
			})
			assert.NoError(t, err)
			assert.Equal(t, got, tc.want)
			assert.Equal(t, allocations, 0)
			if tc.noAllocation {
				l.ensureScopes()
				got, err = l.projectIdentity(tc.object, view, tc.version)
				assert.NoError(t, err)
				assert.Equal(t, got, tc.want)
				if l.scopes.projections != nil {
					t.Fatal("cheap projection initialized the memo")
				}
			}
		})
	}
}

func TestProjectIdentity_CompleteInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		object  manifest.RawObject
		view    graphVisit
		version string
		want    manifest.NamedResource
		entries int
	}{
		{name: "group", object: manifest.RawObject{APIVersion: "other.test/v1", Kind: "Widget", Name: "item", Namespace: "authored"}, view: graphVisit{fallback: "fallback", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "other.test", Kind: "Widget", Namespace: "authored", Name: "pre-item-post"}, entries: 2},
		{name: "kind", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Other", Name: "item", Namespace: "authored"}, view: graphVisit{fallback: "fallback", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Other", Namespace: "authored", Name: "pre-item-post"}, entries: 2},
		{name: "name", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "other", Namespace: "authored"}, view: graphVisit{fallback: "fallback", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "authored", Name: "pre-other-post"}, entries: 2},
		{name: "resolved namespace", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "item", Namespace: "other"}, view: graphVisit{fallback: "fallback", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "other", Name: "pre-item-post"}, entries: 2},
		{name: "exact version equal result", object: manifest.RawObject{APIVersion: "example.test/v2", Kind: "Widget", Name: "item", Namespace: "authored"}, view: graphVisit{fallback: "fallback", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v2", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "authored", Name: "pre-item-post"}, entries: 2},
		{name: "forced namespace", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "item", Namespace: "authored"}, view: graphVisit{namespace: "selected", fallback: "fallback", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "selected", Name: "pre-item-post"}, entries: 2},
		{name: "prefix", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "item", Namespace: "authored"}, view: graphVisit{fallback: "fallback", namePrefix: "other-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "authored", Name: "other-item-post"}, entries: 2},
		{name: "suffix", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "item", Namespace: "authored"}, view: graphVisit{fallback: "fallback", namePrefix: "pre-", nameSuffix: "-other"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "authored", Name: "pre-item-other"}, entries: 2},
		{name: "fallback shadowed by explicit namespace", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "item", Namespace: "authored"}, view: graphVisit{fallback: "other", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "authored", Name: "pre-item-post"}, entries: 1},
		{name: "fallback resolves to explicit namespace", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "item"}, view: graphVisit{fallback: "authored", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "authored", Name: "pre-item-post"}, entries: 1},
		{name: "different fallback namespace", object: manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "item"}, view: graphVisit{fallback: "other", namePrefix: "pre-", nameSuffix: "-post"}, version: "example.test/v1", want: manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "other", Name: "pre-item-post"}, entries: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := &Loader{}
			obj := &manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "item", Namespace: "authored"}
			view := graphVisit{fallback: "fallback", namePrefix: "pre-", nameSuffix: "-post"}
			first, err := l.projectIdentity(obj, view, obj.APIVersion)
			assert.NoError(t, err)
			assert.Equal(t, first, manifest.NamedResource{Group: "example.test", Kind: "Widget", Namespace: "authored", Name: "pre-item-post"})
			authored := tc.object.Named()
			for range 2 {
				got, err := l.projectIdentity(&tc.object, tc.view, tc.version)
				assert.NoError(t, err)
				assert.Equal(t, got, tc.want)
				assert.Equal(t, len(l.scopes.projections), tc.entries)
			}
			assert.Equal(t, tc.object.Named(), authored)
		})
	}
	t.Run("visit namespace remains separate from resolved namespace", func(t *testing.T) {
		t.Parallel()
		l := &Loader{}
		obj := &manifest.RawObject{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "policy"}
		for _, namespace := range []string{"one", "two"} {
			got, err := l.projectIdentity(obj, graphVisit{namespace: namespace}, obj.APIVersion)
			assert.NoError(t, err)
			assert.Equal(t, got, obj.Named())
		}
		assert.Equal(t, len(l.scopes.projections), 2)
	})

	t.Run("equal resolved namespace with distinct visit namespaces", func(t *testing.T) {
		t.Parallel()
		l := &Loader{}
		obj := &manifest.ConfigMap{Name: "values"}
		for _, view := range []graphVisit{
			{namespace: "selected", fallback: "ignored", namePrefix: "pre-"},
			{fallback: "selected", namePrefix: "pre-"},
		} {
			got, err := l.projectIdentity(obj, view, "v1")
			assert.NoError(t, err)
			assert.Equal(t, got, manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "selected", Name: "pre-values"})
		}
		assert.Equal(t, len(l.scopes.projections), 2)
	})
	t.Run("excluded transforms still distinguish exact inputs", func(t *testing.T) {
		t.Parallel()
		l := &Loader{}
		obj := &manifest.RawObject{APIVersion: "v1", Kind: "Namespace", Name: "_"}
		for i, view := range []graphVisit{
			{namespace: "selected"},
			{namespace: "selected", namePrefix: "pre-"},
			{namespace: "selected", nameSuffix: "-post"},
		} {
			got, err := l.projectIdentity(obj, view, obj.APIVersion)
			assert.NoError(t, err)
			assert.Equal(t, got, manifest.NamedResource{Kind: "Namespace", Name: "selected"})
			assert.Equal(t, len(l.scopes.projections), i+1)
		}
	})
}
