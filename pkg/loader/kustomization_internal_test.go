package loader

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestResolve_File(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	base, absolute := filepath.Join(tmp, "cluster", "apps", "foo"), filepath.Join(tmp, "etc", "values.yaml")
	root := filepath.VolumeName(tmp) + string(filepath.Separator)
	cases := []struct {
		name    string
		base    string
		rel     string
		wantAbs string
		wantOK  bool
	}{
		{"relative under base", base, "data/values.yaml", filepath.Join(base, "data", "values.yaml"), true},
		{"traversal parent", base, "../escape.yaml", "", false},
		{"traversal deep", base, "../../etc/passwd", "", false},
		{"traversal mixed", base, "sub/../../escape", "", false},
		{"sibling prefix escape", base, "../foobar/values.yaml", "", false},
		{"absolute passes through", base, absolute, absolute, true},
		{
			"absolute cleaned",
			base,
			filepath.Dir(absolute) + string(filepath.Separator) + "sub/../values.yaml",
			absolute,
			true,
		},
		{"empty rejected", base, "", "", false},
		{"dot", base, ".", base, true},
		{"dot prefix", base, "..foo", filepath.Join(base, "..foo"), true},
		{"url lexical", base, "https://example.com/x", filepath.Join(base, "https:", "example.com", "x"), true},
		{"relative base", "apps", "child/file", filepath.FromSlash("apps/child/file"), true},
		{"dot base child", ".", "child", "", false},
		{"empty base child", "", "child", "", false},
		{"dot base itself", ".", ".", ".", true},
		{"empty base itself", "", ".", ".", true},
		{"root base child", root, "child", "", false},
		{"root base itself", root, ".", root, true},
		{"root base absolute", root, absolute, absolute, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			abs, ok := kustpath.Resolve(tc.base, tc.rel, kustpath.File)
			assert.FatalIf(
				t,
				ok != tc.wantOK,
				"kustpath.Resolve(%q, %q) ok = %v, want %v",
				tc.base,
				tc.rel,
				ok,
				tc.wantOK,
			)
			assert.ErrorIf(
				t,
				ok && abs != tc.wantAbs,
				"kustpath.Resolve(%q, %q) = %q, want %q",
				tc.base,
				tc.rel,
				abs,
				tc.wantAbs,
			)
		})
	}
}

func TestResolve_Directory(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	base := filepath.Join(tmp, "cluster", "apps", "foo")
	cases := []struct {
		name    string
		base    string
		rel     string
		wantAbs string
		wantOK  bool
	}{
		{"relative under base", base, "sub/dir", filepath.Join(base, "sub", "dir"), true},
		{"traversal parent resolves", base, "../bar", filepath.Join(base, "..", "bar"), true},
		{"traversal deep resolves", base, "../../../deploy/x", filepath.Join(tmp, "deploy", "x"), true},
		{"absolute rejected", base, filepath.Join(tmp, "etc", "x"), "", false},
		{"url rejected", base, "https://example.com/x", "", false},
		{"empty rejected", base, "", "", false},
		{"dot", base, ".", base, true},
		{"dot prefix", base, "..foo", filepath.Join(base, "..foo"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			abs, ok := kustpath.Resolve(tc.base, tc.rel, kustpath.Directory)
			assert.FatalIf(
				t,
				ok != tc.wantOK,
				"kustpath.Resolve(%q, %q) ok = %v, want %v",
				tc.base,
				tc.rel,
				ok,
				tc.wantOK,
			)
			assert.ErrorIf(
				t,
				ok && abs != tc.wantAbs,
				"kustpath.Resolve(%q, %q) = %q, want %q",
				tc.base,
				tc.rel,
				abs,
				tc.wantAbs,
			)
		})
	}
}

func TestFinalizeIndex_ResourceMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, resources string
		builder, want   bool
	}{
		{"omitted", "resources: [cm.yaml]\n", true, false},
		{"listed", "resources: [ks.yaml, cm.yaml]\n", true, true},
		{"builder-free", "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexResourceMembershipCase(t, &tc)
		},
		)
	}
	for _, version := range []string{"", "/", "example.test/", "example.test/v1/extra"} {
		t.Run("unlisted ResourceSet/"+version, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexResourceMembershipCase2(
				t,
				&version,
			)
		},
		)
	}
}

func TestLoader_KustomizationFilenames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		loaded int
	}{
		{"kustomization.yaml", 1},
		{"kustomization.yml", 1},
		{"Kustomization", 1},
		{"kustomization.json", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, tc.name, "resources: [included.yaml]\nunknownDirective: ignored\n")
			for _, file := range []string{"included.yaml", "unlisted.yaml"} {
				testutil.WriteFile(
					t,
					root,
					file,
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: "+file+"}\nspec: {url: https://example.test/repo.git}\n",
				)
			}
			{
				loaded, err := New(store.New()).Load(t.Context(), root)
				assert.FatalIf(
					t,
					err != nil || loaded != tc.loaded,
					"Load = %d, %v; want %d objects",
					loaded,
					err,
					tc.loaded,
				)
			}
		})
	}
}

func TestDocumentReader_SourceViewsAndReuse(t *testing.T) {
	t.Parallel()
	r := documentReader{cache: map[string]document{}}
	working := filepath.Join(t.TempDir(), "apps")
	committed := filepath.Join(t.TempDir(), "apps")
	testutil.WriteFile(t, working, "Kustomization", "namespace: working\n")
	testutil.WriteFile(t, committed, "Kustomization", "namespace: host\n")
	testutil.WriteFile(t, committed, "host.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: host}\n")
	{
		name := new(r).input(filepath.VolumeName(working) + string(filepath.Separator)).name
		assert.FatalIf(t, name != ".", "filesystem root name = %q, want dot", name)
	}
	r.views = map[string]fs.FS{working: os.DirFS(working), committed: fstest.MapFS{
		"Kustomization": {Data: []byte("namespace: committed\n")},
	}}
	builder2 := r.read(working)
	first, file := builder2.body, builder2.file
	other := r.read(committed).body
	assert.FatalIf(
		t,
		first == nil || other == nil || first.Namespace != "working" || other.Namespace != "committed",
		"source views were mixed: working=%v committed=%v",
		first,
		other,
	)
	host := filepath.Join(committed, "host.yaml")
	objects, err := r.parseFile(t.Context(), host, manifest.ParseDocOptions{DefaultNamespace: ""})
	assert.NoError(t, err)
	assert.Equal(t, len(objects), 0)
	{
		failed := r.cache[host].objects
		assert.FatalIf(t, len(failed) != 1 || !errors.Is(failed[0].outcome.Err, fs.ErrNotExist) ||
			failed[0].Obj != nil ||
			failed[0].Doc != nil, "absent committed file fell through to disk: %+v", failed)
	}
	testutil.WriteFile(t, working, "Kustomization", "namespace: changed\n")
	{
		builder4 := r.read(working)
		got, gotFile := builder4.body, builder4.file
		assert.FatalIf(t, got != first || gotFile != file, "%s", "positive result was re-read")
	}
	missing := t.TempDir()
	{
		got := r.read(missing).body
		assert.FatalIf(t, got != nil, "%s", "missing kustomization was discovered")
	}
	testutil.WriteFile(t, missing, "kustomization.yaml", "namespace: late\n")
	{
		got := r.read(missing).body
		assert.FatalIf(t, got != nil, "%s", "negative result was re-read")
	}
	t.Run("retained objects through skips and errors", func(t *testing.T) {
		t.Parallel()
		testDocumentReaderSourceViewsAndReuseRetainedObjectsThroughSkipsAndErrors(t)
	},
	)

	t.Run("load missing mounted child", func(t *testing.T) {
		t.Parallel()
		testDocumentReaderSourceViewsAndReuseLoadMissingMountedChild(
			t,
			&committed,
		)
	},
	)

	t.Run("unavailable source", func(t *testing.T) {
		t.Parallel()
		testDocumentReaderSourceViewsAndReuseUnavailableSource(
			t,
			&testDocumentReaderSourceViewsAndReuseUnavailableSourceInputs{r: &r, committed: &committed},
		)
	},
	)

	t.Run("artifact membership and invalidation", func(t *testing.T) {
		t.Parallel()
		testDocumentReaderSourceViewsAndReuseArtifactMembershipAndInvalidation(t)
	},
	)
	t.Run(
		"native_reader_ownership",
		func(t *testing.T) { t.Parallel(); testDocumentReaderSourceViewsAndReuseNativeReaderOwnership(t) },
	)
	t.Run("replaced source", func(t *testing.T) {
		t.Parallel()
		testDocumentReaderSourceViewsAndReuseReplacedSource(
			t,
			&committed,
		)
	},
	)
	t.Run(
		"late_nested_source",
		func(t *testing.T) { t.Parallel(); testDocumentReaderSourceViewsAndReuseLateNestedSource(t) },
	)
}

func testFinalizeIndexResourceMembershipCase(
	t *testing.T,
	testCaseInput *struct {
		name      string
		resources string
		builder   bool
		want      bool
	},
) {
	t.Helper()

	root := t.TempDir()
	if testCaseInput.builder {
		testutil.WriteFile(t, root, "app/kustomization.yaml", testCaseInput.resources)
	}
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n")
	testutil.WriteFile(t, root, "app/ks.yaml", testutil.KustomizationYAML("stray", "apps", "  path: ./leaf\n"))
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	if got := l.AdmitsRoot(
		l.IndexedObject(
			testutil.KustomizationID("apps", "stray"),
			l.Index.Records[testutil.KustomizationID("apps", "stray")],
		).Obj,
		filepath.Join(root, "app", "ks.yaml"),
	); got != testCaseInput.want {
		t.Fatalf("admission=%t want %t", got, testCaseInput.want)
	}
	var owners []manifest.NamedResource
	if testCaseInput.want {
		owners = []manifest.NamedResource{testutil.KustomizationID("apps", "stray")}
	}
	assert.Diff(t, l.Index.Claims["leaf/"], owners)
}

func testFinalizeIndexResourceMembershipCase2(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [selected.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"selected.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: selected, namespace: apps}\nspec: {url: https://example.test/repo.git}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"excluded.yaml",
		"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: excluded}\nspec:\n  dependsOn: [{apiVersion: '"+(*testCaseInput)+"', kind: Kustomization, name: missing}]\n",
	)
	l := New(store.New())
	_, err := loadForTest(t.Context(), t, l, root)
	assert.NoError(t, err)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	assert.FatalIf(t, len(retainedErrorsForTest(l)) != 0 ||
		l.Store.GetObject(
			manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "selected",
			},
		) == nil,
		"unlisted input affected selected source: errors=%v", retainedErrorsForTest(l))
}

func testDocumentReaderSourceViewsAndReuseRetainedObjectsThroughSkipsAndErrors(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	reader := documentReader{views: map[string]fs.FS{root: os.DirFS(root)}}
	testutil.WriteFile(t, root, "mixed.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: retained}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: source}}
---
plain: data
---
{}
---
# empty document
---
apiVersion: v1
kind: ConfigMap
metadata: 7
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: '${TEMPLATE}'}
---
apiVersion: example.test/v1
kind: Widget
metadata: {name: ignored}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: values}
data: {VALUE: keep}
---
apiVersion: example.test/v1/extra
kind: Widget
metadata: {name: invalid}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: pending}
`)
	var objects []manifest.BaseManifest
	opts := manifest.ParseDocOptions{DefaultNamespace: "apps"}
	var ordinals []int
	var failures []manifest.DocOccurrence
	err := reader.decodeFileObjects(filepath.Join(root, "mixed.yaml"), opts, false, func(child parsedObject) {
		if child.outcome.Err != nil {
			failures = append(failures, child.outcome)
			return
		}
		ordinal := child.outcome.Ordinal
		obj := child.Obj
		objects = append(objects, obj)
		ordinals = append(ordinals, ordinal)
	})
	assert.Diff(t, ordinals, []int{0, 7, 9})
	assert.FatalIf(
		t,
		err != nil || len(failures) != 2 || len(objects) != 3 || !errors.Is(failures[0].Err, manifest.ErrInput),
		"objects=%v err=%v",
		objects,
		err,
	)
	k := objects[0].(*manifest.Kustomization)
	c := objects[1].(*manifest.ConfigMap)
	before, err := k.RenderDocument()
	assert.NoError(t, err)
	testutil.WriteFile(
		t,
		root,
		"next.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next}\ndata: {VALUE: replaced}\n",
	)
	_, err = reader.parseFile(t.Context(), filepath.Join(root, "next.yaml"), opts)
	assert.NoError(t, err)
	assert.Equal(t, k.Namespace, "apps")
	after, err := k.RenderDocument()
	assert.NoError(t, err)
	assert.Diff(t, after, before)
	valueResult, err := c.Value("VALUE")
	value, found := valueResult.Value, valueResult.Found
	assert.NoError(t, err)
	assert.Equal(t, found, true)
	assert.Equal(t, value, "keep")
}

func testDocumentReaderSourceViewsAndReuseLoadMissingMountedChild(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	child := filepath.Join((*testCaseInput), "child")
	testutil.WriteFile(t, child, "ks.yaml", testutil.KustomizationYAML("decoy", "", "  {path: ./apps}\n"))
	st := store.New()
	input := &store.SourceArtifact{LocalPath: (*testCaseInput), FS: fstest.MapFS{
		"Kustomization": {Data: []byte("namespace: committed\n")},
	}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := New(st)
	{
		_, err := loadForTest(t.Context(), t, l, child)
		assert.FatalIf(
			t,
			!errors.Is(err, fs.ErrNotExist),
			"missing mounted child error = %v, want absent source content",
			err,
		)
	}
	assert.FatalIf(t, len(st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)) != 0 ||
		len(l.Index.Records) != 0 ||
		len(retainedErrorsForTest(l)) != 0, "%s", "physical decoy supplied an object or indexed evidence")
	assert.FatalIf(t, st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != input ||
		new(l.documents).mountedRoot(
			child,
		) != (*testCaseInput), "%s", "missing child replaced the authoritative mounted view")
	{
		k := l.documents.read((*testCaseInput)).body
		assert.FatalIf(
			t,
			k == nil || k.Namespace != "committed",
			"%s",
			"virtual view lost authority over physical content",
		)
	}
}

type testDocumentReaderSourceViewsAndReuseUnavailableSourceInputs struct {
	r         *documentReader
	committed *string
}

func testDocumentReaderSourceViewsAndReuseUnavailableSource(
	t *testing.T,
	testCaseInput *testDocumentReaderSourceViewsAndReuseUnavailableSourceInputs,
) {
	t.Helper()

	st := store.New()
	repo := &manifest.GitRepository{Name: "released", Namespace: "n"}
	st.AddObject(repo)
	st.SetArtifact(repo.Named(), &store.SourceArtifact{LocalPath: (*testCaseInput.committed), Key: "pin"})
	testCaseInput.r.mountSources(st)
	{
		_, err := testCaseInput.r.parseFile(
			t.Context(),
			filepath.Join((*testCaseInput.committed), "host.yaml"),
			manifest.ParseDocOptions{DefaultNamespace: ""},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "released pin read host content through its coordinate: %v", err)
	}
}

func testDocumentReaderSourceViewsAndReuseArtifactMembershipAndInvalidation(t *testing.T) {
	t.Helper()

	st := store.New()
	reader := documentReader{cache: map[string]document{}}
	root := t.TempDir()
	git := &manifest.GitRepository{Name: "git"}
	oci := &manifest.OCIRepository{Name: "oci"}
	bucket := &manifest.Bucket{Name: "bucket"}
	var mounted []string
	for _, tc := range []struct {
		obj   manifest.BaseManifest
		mount bool
	}{
		{git, true},
		{oci, true},
		{bucket, true},
		{&manifest.ExternalArtifact{Name: "external"}, true},
		{&manifest.HelmRepository{Name: "repository"}, false},
		{&manifest.HelmChartSource{Name: "chart"}, false},
		{&manifest.RawObject{APIVersion: "foreign.example/v1", Kind: manifest.KindGitRepository, Name: "foreign"}, false},
	} {
		id := tc.obj.Named()
		path := filepath.Join(root, id.Name)
		testutil.WriteFile(t, path, "Kustomization", "namespace: host\n")
		st.AddObject(tc.obj)
		st.SetArtifact(id, &store.SourceArtifact{LocalRoot: root, LocalPath: path, FS: fstest.MapFS{
			"Kustomization": {Data: []byte("namespace: " + id.Name + "\n")},
		}})
		if tc.mount {
			mounted = append(mounted, path)
		}
	}
	alias := &manifest.GitRepository{Name: "alias"}
	st.AddObject(alias)
	st.SetArtifact(alias.Named(), st.GetArtifact(git.Named(), store.ArtifactContent).(*store.SourceArtifact))
	absentPath := &manifest.GitRepository{Name: "absent-path"}
	st.AddObject(absentPath)
	st.SetArtifact(absentPath.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
	wrongArtifact := &manifest.Bucket{Name: "wrong-artifact"}
	st.AddObject(wrongArtifact)
	st.RecordEmitted(wrongArtifact.Named(), &store.Emission{}, nil)
	bootstrapPath := filepath.Join(root, "bootstrap")
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: bootstrapPath, FS: fstest.MapFS{
		"Kustomization": {Data: []byte("namespace: bootstrap\n")},
	}})
	assert.Equal(t, st.GetObject(manifest.BootstrapSourceID) == nil, true)
	mounted = append(mounted, bootstrapPath)
	slices.Sort(mounted)
	assert.Diff(t, reader.mountSources(st), mounted)
	assert.Equal(t, len(reader.views), len(mounted))
	for _, path := range mounted {
		body := reader.read(path).body
		assert.FatalIf(
			t,
			body == nil || body.Namespace != filepath.Base(path),
			"mounted content = %v for %s",
			body,
			path,
		)
		{
			again := reader.read(path).body
			assert.FatalIf(t, again != body, "%s", "unchanged mounted document was re-parsed")
		}
	}
	assert.Equal(t, len(reader.mountSources(st)), 0)
	ociPath, bucketPath := filepath.Join(root, "oci"), filepath.Join(root, "bucket")
	late := filepath.Join(ociPath, "late")
	{
		body := reader.read(late).body
		assert.FatalIf(t, body != nil, "%s", "unexpected document before replacement")
	}
	st.SetArtifact(oci.Named(), &store.SourceArtifact{LocalPath: ociPath, FS: fstest.MapFS{
		"Kustomization":      {Data: []byte("namespace: replaced\n")},
		"late/Kustomization": {Data: []byte("namespace: late\n")},
	}})
	st.DeleteObject(bucket.Named())
	assert.Diff(t, reader.mountSources(st), []string{bucketPath, ociPath})
	{
		body := reader.read(ociPath).body
		assert.FatalIf(
			t,
			body == nil || body.Namespace != "replaced",
			"replacement retained stale document: %v",
			body,
		)
	}
	{
		body := reader.read(late).body
		assert.FatalIf(t, body == nil || body.Namespace != "late", "replacement retained negative cache: %v", body)
	}
	{
		body := reader.read(bucketPath).body
		assert.FatalIf(
			t,
			body != nil || new(reader).mountedRoot(bucketPath) != "",
			"%s",
			"removed source retained a mount or read the physical decoy",
		)
	}
}

func testDocumentReaderSourceViewsAndReuseNativeReaderOwnership(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "namespace: native\n")
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	artifact, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, artifact)
	r := documentReader{}
	r.mountSources(st)
	assert.FatalIf(t, r.views[root] != artifact.FS, "%s", "temporary reader constructed a private operation view")
	l := New(st)
	l.mountSources()
	assert.FatalIf(t, l.documents.views[root] == artifact.FS ||
		st.GetArtifact(
			manifest.BootstrapSourceID,
			store.ArtifactContent,
		) != artifact, "%s", "loader did not own its private view independently of the artifact")
}
