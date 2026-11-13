package loader

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDiscovery_SkipsNonDiscoveryKinds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cases := []struct {
		kind, api, spec string
		admitted        bool
	}{
		{
			manifest.KindKustomization,
			manifest.FluxKustomizeDomain + "/v1",
			"spec: {interval: 10m, path: ./apps, sourceRef: {kind: GitRepository, name: fixture}}",
			true,
		},
		{manifest.KindResourceSet, manifest.FluxOperatorDomain + "/v1", "spec: {resources: []}", true},
		{
			manifest.KindResourceSetInputProvider,
			manifest.FluxOperatorDomain + "/v1",
			"spec: {type: Static, defaultValues: {key: value}}",
			true,
		},
		{manifest.KindGitRepository, manifest.SourceDomain + "/v1", "spec: {url: https://example.test/repo.git}", true},
		{
			manifest.KindHelmRelease,
			manifest.HelmReleaseDomain + "/v2",
			"spec: {chart: {spec: {chart: podinfo, sourceRef: {kind: HelmRepository, name: podinfo}}}}",
			false,
		},
		{manifest.KindConfigMap, "v1", "data: {FOO: bar}", false},
	}
	for _, tc := range cases {
		testutil.WriteFile(
			t,
			dir,
			tc.kind+".yaml",
			fmt.Sprintf(
				"apiVersion: %s\nkind: %s\nmetadata: {name: fixture, namespace: flux-system}\n%s\n",
				tc.api,
				tc.kind,
				tc.spec,
			),
		)
	}
	st := store.New()
	l := New(st)
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	{
		_, err := l.Load(t.Context(), dir, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
	}
	for id := range l.Index.Files() {
		assert.FatalIf(t, st.GetObject(id) != nil, "evidence became runnable before admission: %s", id)
	}
	assert.NoError(t, l.Admit(t.Context(), dir))
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			group, _, found := strings.Cut(tc.api, "/")
			if !found {
				group = ""
			}
			id := manifest.NamedResource{Group: group, Kind: tc.kind, Name: "fixture", Namespace: "flux-system"}
			assert.Equal(t, st.GetObject(id) != nil, tc.admitted)
			_, indexed := l.Index.Lookup(id)
			assert.Equal(t, indexed, true)
		})
	}
	cached := l.documents.cache[filepath.Join(dir, manifest.KindGitRepository+".yaml")].objects
	assert.Equal(
		t,
		slices.ContainsFunc(
			cached,
			func(child parsedObject) bool { return child.Obj.Named().Kind == manifest.KindGitRepository },
		),
		true,
	)
}

// TestDiscovery_PromoteMaterializesFromIndex covers the
// lazy-promotion contract: when depwait hits a missing dep that the
// Existence index knows about, Promote re-parses the file and
// AddObjects it into the Store so the wait can clear.
func TestDiscovery_PromoteMaterializesFromIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "cm.yaml", `
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-cm
  namespace: default
data:
  KEY: value
`)

	st := store.New()
	admitForTest(st, dir)
	l := loaderFixture(st, nil)
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}

	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "my-cm"}
	assert.FatalIf(t, st.GetObject(id) != nil, "precondition: CM should not be in store yet")
	{
		promoted, err := promoteForTest(t.Context(), t,
			l.Store,
			id,
			PromoteOptions{
				Record: productionForTest(new(l.Index.Records[id])),
			},
		)
		assert.FatalIf(t, err != nil ||
			!promoted, "Promote should return true on a known id")
	}
	assert.ErrorIf(t, st.GetObject(id) == nil, "Promote did not materialize CM into Store")
}

// TestPromote_UnknownIDReturnsFalse covers the path
// where depwait's missing-dep fallback asks for a CM that never
// reached file-load: existence index has no entry, so Promote bails
// out cleanly and the depwait surfaces "dependency not found" as
// intended.
func TestPromote_UnknownIDReturnsFalse(t *testing.T) {
	t.Parallel()
	idx := loaderFixture(store.New(), nil)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "ghost"}
	{
		got, err := promoteForTest(t.Context(), t,
			idx.Store,
			id,
			PromoteOptions{
				Record: productionForTest(new(idx.Index.Records[id])),
			},
		)
		assert.ErrorIf(t, err != nil ||
			got, "Promote on unknown id should return false, got true")
	}
}

func TestPromote_FileRemovedBetweenRecordAndPromote(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "cm.yaml", `
apiVersion: v1
kind: ConfigMap
metadata:
  name: ephemeral
  namespace: default
data:
  KEY: value
`)
	st := store.New()
	admitForTest(st, dir)
	l := loaderFixture(st, nil)
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	// Remove the file behind the index.
	{
		err := os.Remove(filepath.Join(dir, "cm.yaml"))
		assert.FatalIf(t, err != nil, "remove: %v", err)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "ephemeral"}
	{
		got, err := promoteForTest(t.Context(), t,
			l.Store,
			id,
			PromoteOptions{
				Record: productionForTest(new(l.Index.Records[id])),
			},
		)
		assert.ErrorIf(t, err != nil ||
			got, "Promote on a removed file should return false, got true")
	}
	assert.ErrorIf(t, st.GetObject(id) != nil, "removed file should not have produced a Store entry")
}

func TestPromote_MultiDocLoadsSiblings(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"file", "built"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			const bundle = `apiVersion: v1
kind: ConfigMap
metadata: {name: app-config, namespace: apps}
data: {KEY: value}
---
apiVersion: v1
kind: Secret
metadata: {name: app-secret, namespace: apps}
stringData: {TOKEN: "ENC[AES256_GCM,data:fixture,iv:x,tag:y,type:str]"}
sops: {encrypted_regex: "^(data|stringData)$", version: 3.9.0}
`
			testutil.WriteFile(t, dir, "bundle.yaml", bundle)
			st := store.New()
			admitForTest(st, dir)
			cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "app-config"}
			secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "app-secret"}
			record := manifest.Production{Authored: cmID, Path: filepath.Join(dir, "bundle.yaml")}
			opts := PromoteOptions{}
			if input == "built" {
				record = manifest.Production{}
				opts.Documents = builtObjectsForTest(t, bundle)
			}
			opts.Record = record
			{
				promoted, err := promoteForTest(t.Context(), t, st, cmID, opts)
				assert.FatalIf(t, err != nil || !promoted, "%s", "ConfigMap promotion failed")
			}
			for _, id := range []manifest.NamedResource{cmID, secretID} {
				assert.ErrorIf(t, st.GetObject(id) == nil, "admitted sibling %s was not promoted", id)
			}
			secret := st.GetObject(secretID).(*manifest.Secret)
			assert.Diff(t, secret.StringData, map[string]any{})
		})
	}
}

func TestPromote_AdmissionAndPreferExisting(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"file", "built"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			testPromoteAdmissionAndPreferExistingCase(t, &input)
		},
		)
	}
}

// Sources must remain admitted so bootstrap aliasing can resolve their URLs
// without redirecting unrelated source references to the working tree.
func TestDiscovery_SourcesStayFileLoaded(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "src.yaml", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: upstream
  namespace: flux-system
spec:
  url: https://example.test/upstream.git
---
apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: OCIRepository
metadata:
  name: charts
  namespace: flux-system
spec:
  url: oci://example.test/charts
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: podinfo
  namespace: default
spec:
  url: https://example.test/charts
`)

	st := store.New()
	l := loaderFixture(st, nil)
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}

	for _, want := range []manifest.NamedResource{
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "upstream"},
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "charts"},
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindHelmRepository, Namespace: "default", Name: "podinfo"},
	} {
		assert.ErrorIf(t, st.GetObject(want) == nil,
			"source %s must stay in Store during discovery; got nil", want.String())
	}
}

func TestPromote_AuthoredDocumentProjection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, authored, namespace, version string
		evidence                           bool
	}{
		{"bare projected", "", "apps", "1.0.0", true},
		{"authored wins over foreign exact identity", "authored", "other", "1.0.0", true},
		{"no evidence exact identity", "authored", "other", "2.0.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testPromoteAuthoredDocumentProjectionCase(t, &tc)
		},
		)
	}
}

func TestPromote_SourceView(t *testing.T) {
	t.Parallel()
	for _, present := range []bool{true, false} {
		t.Run(fmt.Sprintf("present=%v", present), func(t *testing.T) {
			t.Parallel()
			testPromoteSourceViewCase(t, &present)
		},
		)
	}
}

type promotionBarrier struct {
	fs.FS

	opened  chan struct{}
	release chan struct{}
}

func (b promotionBarrier) Open(name string) (fs.File, error) {
	close(b.opened)
	<-b.release
	return b.FS.Open(name)
}

func TestPromote_CapturedSourceReplacement(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(source)
	root := t.TempDir()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "selected", Namespace: "apps"}
	makeFS := func(value string) fstest.MapFS {
		return fstest.MapFS{
			"values.yaml": &fstest.MapFile{
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected, namespace: apps}\ndata: {value: " + value + "}\n",
				),
			},
		}
	}
	barrier := promotionBarrier{FS: makeFS("old"), opened: make(chan struct{}), release: make(chan struct{})}
	st.SetArtifact(source.Named(), &store.SourceArtifact{Key: "old", LocalPath: root, FS: barrier})
	var first store.ReadSet
	first.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
	options := PromoteOptions{
		Record: manifest.Production{Source: source.Named(), Authored: id, Path: filepath.Join(root, "values.yaml")},
		Reads:  &first,
	}
	var arrivals int
	st.AddListener(store.EventObjectAdded, func(changed manifest.NamedResource, _ any) {
		if changed == id {
			arrivals++
		}
	}, false)
	done := make(chan bool, 1)
	go func() {
		promoted, err := promoteForTest(t.Context(), t, st, id, options)
		assert.ErrorIf(t, err != nil, "%v", err)
		done <- promoted
	}()
	<-barrier.opened
	st.SetArtifact(source.Named(), &store.SourceArtifact{Key: "new", LocalPath: root, FS: makeFS("new")})
	close(barrier.release)
	assert.FatalIf(
		t,
		<-done || st.GetObject(id) != nil || first.Valid() || arrivals != 0,
		"%s",
		"changed source admitted stale raw bytes",
	)
	first.Retire()
	var second store.ReadSet
	second.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
	options.Reads = &second
	{
		promoted, err := promoteForTest(t.Context(), t, st, id, options)
		assert.FatalIf(t, err != nil || !promoted, "%s", "current captured source did not admit")
	}
	if got, _ := second.Get[*manifest.ConfigMap](id); got == nil || got.Data["value"] != "new" || !second.Valid() {
		t.Fatalf("current promotion = %+v, valid=%t", got, second.Valid())
	}
	st.SetArtifact(source.Named(), &store.SourceArtifact{Key: "third", LocalPath: root, FS: makeFS("third")})
	assert.FatalIf(
		t,
		st.GetObject(id) != nil || second.Valid(),
		"%s",
		"source-bound raw admission survived changed source",
	)
	var third store.ReadSet
	third.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
	options.Reads = &third
	{
		promoted, err := promoteForTest(t.Context(), t, st, id, options)
		assert.FatalIf(t, err != nil || !promoted, "%s", "replacement source could not replace raw admission")
	}
	if got, _ := third.Get[*manifest.ConfigMap](id); got == nil || got.Data["value"] != "third" || !third.Valid() {
		t.Fatalf("replacement promotion = %+v, valid=%t", got, third.Valid())
	}
	second.Retire()
	third.Retire()
	for range st.Sweep() {
	}
}

func testPromoteAdmissionAndPreferExistingCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	for _, tc := range []struct {
		name     string
		secret   bool
		existing bool
	}{
		{name: "admitted request"},
		{name: "rejected request", secret: true},
		{name: "rendered object retained", existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			const bundle = `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: apps}
data: {source: raw}
---
apiVersion: v1
kind: Secret
metadata: {name: excluded, namespace: apps}
stringData: {value: fixture}
`
			testutil.WriteFile(t, dir, "bundle.yaml", bundle)
			cm := &manifest.ConfigMap{
				Name:      "selected",
				Namespace: "apps",
				Data:      map[string]any{"source": "rendered"},
			}
			secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "excluded"}
			st := store.New()
			if tc.existing {
				st.AddObject(cm)
			}
			admitForTest(st, dir)
			id := cm.Named()
			if tc.secret {
				id = secret
			}
			record := manifest.Production{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}
			opts := PromoteOptions{}
			if (*testCaseInput) == "built" {
				record = manifest.Production{}
				opts.Documents = builtObjectsForTest(t, bundle)
			}
			opts.Record = record
			{
				got, err := promoteForTest(t.Context(), t, st, id, opts, cm.Named())
				assert.ErrorIf(t, err != nil || got == tc.secret, "Promote returned %v, want %v", got, !tc.secret)
			}
			assert.ErrorIf(
				t,
				st.GetObject(cm.Named()) == nil || st.GetObject(secret) != nil,
				"%s",
				"admission must apply separately to requested and sibling objects",
			)
			assert.ErrorIf(
				t,
				tc.existing && st.GetObject(cm.Named()) != cm,
				"%s",
				"existing rendered object must take precedence",
			)
		})
	}
}

func testPromoteAuthoredDocumentProjectionCase(
	t *testing.T,
	testCaseInput *struct {
		name      string
		authored  string
		namespace string
		version   string
		evidence  bool
	},
) {
	t.Helper()
	const doc = "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: podinfo, namespace: %q}\nspec:\n  interval: 1h\n  chart: {spec: {chart: podinfo, version: %s, sourceRef: {kind: HelmRepository, name: podinfo}}}\n"

	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"bundle.yaml",
		strings.Replace(
			fmt.Sprintf(doc, testCaseInput.authored, "9.0.0"),
			manifest.HelmReleaseDomain,
			"foreign.example",
			1,
		)+"---\n"+fmt.Sprintf(
			doc,
			"other",
			"2.0.0",
		)+"---\n"+fmt.Sprintf(
			doc, testCaseInput.authored, "1.0.0",
		)+"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: note}\n",
	)
	st := store.New()
	admitForTest(st, dir)
	id := testutil.HelmReleaseID(testCaseInput.namespace, "podinfo")
	record := manifest.DiscoveryRecord{Path: filepath.Join(dir, "bundle.yaml")}
	if testCaseInput.evidence {
		record.Authored = id
		record.Authored.Namespace = testCaseInput.authored
	}
	if got, err := promoteForTest(t.Context(), t,
		st,
		id,
		PromoteOptions{
			Record: productionForTest(new(record)),
		},
	); err != nil ||
		got != (testCaseInput.version != "") {
		t.Fatalf("promoteForTest(t.Context(), %s) = %v, want %v", id, got, testCaseInput.version != "")
	}
	if got := st.GetObject(id).(*manifest.HelmRelease).Chart.Version; got != testCaseInput.version {
		t.Errorf("%s chart version = %s, want %s", id, got, testCaseInput.version)
	}
	foreign := id
	foreign.Group = "foreign.example"
	{
		promoted, err := promoteForTest(t.Context(), t,
			st,
			foreign,
			PromoteOptions{
				Record: productionForTest(new(record)),
			},
		)
		assert.FatalIf(t, err != nil ||
			promoted, "%s", "foreign promotion borrowed the real HelmRelease")
	}
	sibling := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "note"}
	assert.ErrorIf(t, st.GetObject(sibling) == nil, "%s", "admitted sibling must retain its authored identity")
	sibling.Namespace = testCaseInput.namespace
	assert.ErrorIf(t, st.GetObject(sibling) != nil, "sibling %s acquired the requested namespace", sibling)
}

func testPromoteSourceViewCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()
	const doc = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected, namespace: authored}\ndata: {source: %s}\n"

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "values.yaml", fmt.Sprintf(doc, "working"))
	input := fstest.MapFS{}
	if *testCaseInput {
		input["values.yaml"] = &fstest.MapFile{Data: []byte(fmt.Sprintf(doc, "committed"))}
	}
	st := store.New()
	source := &manifest.GitRepository{Name: "source"}
	st.AddObject(source)
	repoRoot := t.TempDir()
	st.SetArtifact(
		source.Named(),
		&store.SourceArtifact{LocalRoot: repoRoot, LocalPath: dir, Key: "pinned", Content: "pinned", FS: input},
	)
	foreign := &manifest.RawObject{
		APIVersion: "foreign.example/v1",
		Kind:       manifest.KindGitRepository,
		Name:       source.Name,
	}
	st.AddObject(foreign)
	st.SetArtifact(
		foreign.Named(),
		&store.SourceArtifact{
			LocalPath: dir,
			FS:        fstest.MapFS{"values.yaml": &fstest.MapFile{Data: []byte(fmt.Sprintf(doc, "foreign"))}},
		},
	)
	for _, version := range []string{"", manifest.SourceDomain + "/v1", "v1", "foreign.example/v1", "missing.example/v1"} {
		ks := &manifest.Kustomization{
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				APIVersion: version,
				Kind:       manifest.KindGitRepository,
				Name:       source.Name,
			},
		}
		location := SourceView(st, ks.SourceID(), repoRoot)
		root, pinned, external := location.Path, location.Pinned, location.External
		admitted := version == "" || version == manifest.SourceDomain+"/v1"
		wantRoot := repoRoot
		if admitted {
			wantRoot = dir
		}
		assert.FatalIf(
			t,
			root != wantRoot || pinned != admitted || external != (version == "foreign.example/v1"),
			"SourceView(%q) = %s, %t, %t",
			version,
			root,
			pinned,
			external,
		)
		assert.FatalIf(t, !admitted && st.GetArtifact(ks.SourceID(), store.ArtifactContent) != nil &&
			version != "foreign.example/v1",
			"missing source %s borrowed the real artifact", ks.SourceID())
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	record := manifest.DiscoveryRecord{
		Authored:    id,
		Path:        filepath.Join(dir, "values.yaml"),
		Productions: []manifest.Production{{Source: source.Named()}},
	}
	record.Authored.Namespace = "authored"
	got, err := promoteForTest(t.Context(), t,
		st,
		id,
		PromoteOptions{
			Record: productionForTest(new(record)),
		},
	)
	assert.FatalIf(
		t,
		got != (*testCaseInput) || err != nil,
		"Promote = %v, %v, want availability %v",
		got,
		err,
		(*testCaseInput),
	)
	assert.FatalIf(
		t,
		(*testCaseInput) && st.GetObject(id).(*manifest.ConfigMap).Data["source"] != "committed",
		"%s",
		"promotion read the working file instead of the mounted view",
	)
}
