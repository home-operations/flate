package discovery

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/gitrepo"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestLoadManifests_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	var work manifest.WorkRecorder
	d, err := Begin(t.Context(), Config{
		Path: filepath.Join(root, "flux"), Store: st, Work: work.Scope(),
		Input: &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
			"flux/owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps",
				"  path: ./work\n  sourceRef: {kind: OCIRepository, name: missing}\n"))},
			"work/kustomization.yaml": {Data: []byte("resources: []\n")},
		}},
	})
	assert.NoError(t, err)
	t.Cleanup(d.Close)
	assert.NoError(t, constructTestSources(t.Context(), d))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	published := false
	unsubscribe := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Kind == manifest.KindOCIRepository && id.Name == "missing" {
			assert.NoError(t, constructAdmissionTest(t.Context(), d.admission(), id))
			published = true
			cancel()
		}
	}, false)
	defer unsubscribe()
	err = d.loadManifests(ctx, root, false)
	assert.Equal(t, published, true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, err, ctx.Err())
	var attempts uint64
	for _, record := range work.Scope().Snapshot(0) {
		if record.Key.Source.Kind == manifest.KindOCIRepository && record.Key.Source.Name == "missing" {
			attempts += record.Counts.Attempts
		}
	}
	assert.Equal(t, attempts, uint64(0))
}

func TestDiscardPinnedWorkingTreeFiles_PreservesSources(t *testing.T) {
	t.Parallel()
	t.Run("runtime alias and overlapping claims", testDiscardPinnedWorkingTreeFilesPreservesSourcesRuntimeAlias)
	t.Run(
		"projected owner",
		func(t *testing.T) { t.Parallel(); testDiscardPinnedWorkingTreeFilesPreservesSourcesProjectedOwner(t) },
	)
	for _, tc := range []struct {
		name     string
		resolved bool
	}{
		{name: "resolved", resolved: true},
		{name: "referenced"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testDiscardPinnedWorkingTreeFilesPreservesSourcesCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestLoadAt_IgnoreFileAndMissingDirectory(t *testing.T) {
	t.Parallel()
	t.Run(
		"mounted symlink",
		func(t *testing.T) { t.Parallel(); testLoadAtIgnoreFileAndMissingDirectoryMountedSymlink(t) },
	)

	t.Run("source omitted directory", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app}\n")
		testutil.WriteFile(t, root, "override.krmignore", "")
		assert.NoError(t, os.Symlink("apps", filepath.Join(root, "link")))
		location, err := gitrepo.Locate(t.Context(), root, root)
		assert.NoError(t, err)
		directory, err := gittree.OpenDirectory(t.Context(), location)
		assert.NoError(t, err)
		defer func() { _ = directory.Close() }()
		input := directory.Artifact()
		view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Source, Current: input.FS})
		assert.NoError(t, err)
		source := &manifest.GitRepository{Name: "source"}
		st := store.New()
		st.AddObject(source)
		st.SetArtifact(source.Named(), &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: view})
		d := Session{loader: loader.New(st)}
		d.loader.IgnoreFile = filepath.Join(root, "override.krmignore")
		total := 0
		loaded, err := d.loadAt(t.Context(), filepath.Join(root, "link"), &total, &sourceLoad{})
		assert.NoError(t, err)
		assert.Equal(t, loaded, 0)
		assert.Equal(t, total, 0)
	})

	for _, backing := range []string{"disk", "memory"} {
		for _, scenario := range []string{"missing-directory", "file", "missing-ignore"} {
			t.Run(backing+"/"+scenario, func(t *testing.T) {
				t.Parallel()
				testLoadAtIgnoreFileAndMissingDirectoryCase(
					t,
					testLoadAtIgnoreFileAndMissingDirectoryCaseInputs{backing: &backing, scenario: &scenario},
				)
			},
			)
		}
	}
}

func TestRun_SelectedEntryAdmission(t *testing.T) {
	t.Parallel()
	for _, ownership := range []string{"internal", "authored", "alias", "alias ignored", "alias sparse", "pinned", "fragment", "label collision", "deferred", "optional"} {
		t.Run(ownership, func(t *testing.T) {
			t.Parallel()
			testRunSelectedEntryAdmissionCase(t, &ownership)
		},
		)
	}
}

//nolint:gocritic // Test configuration MUST share its cache owner with its renderer.
func testConfig(t *testing.T, cfg Config) Config {
	t.Helper()
	cfg.PrepareDocuments = base.PrepareDocuments
	cfg.Cache = cachetest.New(t, "", cache.New)
	cfg.Trees = kustomize.NewTreeCache(cfg.Cache, nil, task.NewBounded(2))

	return cfg
}

func testTrees(t *testing.T) *kustomize.TreeCache {
	t.Helper()
	return testConfig(t, Config{}).Trees
}

type receiptTrees struct {
	owner *cache.Cache
	trees *kustomize.TreeCache
	close func()
}

func testReceiptTrees(t *testing.T, cacheDir string, persistent bool) receiptTrees {
	t.Helper()
	owner, err := cache.New(t.Context(), cacheDir)
	assert.NoError(t, err)
	trees := kustomize.NewTreeCache(owner, nil, task.NewBounded(2))
	closeCache := func() {
		t.Helper()

		assert.NoError(t, owner.Close())
	}
	t.Cleanup(closeCache)
	assert.Equal(t, owner.CanPersistRenders(), cacheDir != "" && persistent)
	return receiptTrees{owner: owner, trees: trees, close: closeCache}
}

func packageHistory(t *testing.T, trees *kustomize.TreeCache,
	req *kustomize.RenderRequest, persistent bool,
) map[manifest.NamedResource]manifest.DiscoveryRecord {
	t.Helper()
	docs, receipt, available := trees.PreviousRender(t.Context(), *req)
	base.PrepareDocuments(req.Spec, docs)
	records, complete := trees.RenderHistory(req.Spec.Named(), receipt, kustomize.PrepareShape(req, docs).Content())
	assert.Equal(t, available, persistent)
	assert.Equal(t, complete, persistent)
	assert.Equal(t, records != nil, persistent)
	if !persistent {
		return nil
	}
	return maps.Collect(records)
}

func preparedRequest(
	t *testing.T,
	cfg *Config,
	ks *manifest.Kustomization,
	source *store.SourceArtifact,
) kustomize.RenderRequest {
	t.Helper()
	prepared, err := kustomize.Prepare(ks, nil)
	assert.NoError(t, err)
	return kustomize.RenderRequest{
		Source: source,

		SubPath:             prepared.Path,
		Spec:                prepared,
		StrictSubstitutions: cfg.StrictSubstitutions,
		SkipKinds:           cfg.SkipKinds,
	}
}

func TestRun_BuiltEntryFixedPoint(t *testing.T) {
	t.Parallel()
	const root = "/virtual/built-fixed-point"
	input := fstest.MapFS{
		"app/kustomization.yaml": {
			Data: []byte(
				"resources: [child.yaml, source.yaml]\nnameSuffix: -dev\npatches:\n- target: {kind: Kustomization, name: child}\n  patch: '[{\"op\":\"replace\",\"path\":\"/spec/path\",\"value\":\"./leaf\"}]'\n",
			),
		},
		"app/child.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"child",
					"apps",
					"  path: ./untransformed\n  sourceRef: {kind: GitRepository, name: nested-dev}\n",
				),
			),
		},
		"app/source.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: nested, namespace: apps}\nspec: {url: https://fixture.invalid/self}\n",
			),
		},
		"leaf/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"leaf/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n")},
	}
	st := store.New()
	result, err := Run(
		t.Context(),
		testConfig(t, Config{
			Input:    &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: input},
			Path:     root + "/app",
			Store:    st,
			SelfURLs: []string{"https://fixture.invalid/self"},
		}),
	)
	assert.NoError(t, err)
	child := testutil.KustomizationID("apps", "child-dev")
	record, ok := result.Index.Lookup(child)
	assert.FatalIf(t, !ok || record.Parent != result.Binding.Producer ||
		st.GetObject(testutil.KustomizationID("apps", "child")) != nil, "effective declaration not bound: %+v", record)
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "nested-dev",
	}
	assert.FatalIf(t, st.GetArtifact(source, store.ArtifactContent) == nil, "%v", "effective self source not admitted")
	leaf := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "leaf"}
	record, ok = result.Index.Lookup(leaf)
	assert.FatalIf(
		t,
		!ok || !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return p.Owner == child }),
		"effective path not expanded: %+v",
		record,
	)
}

func testDiscardPinnedWorkingTreeFilesPreservesSourcesProjectedOwner(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "namespace: control\nresources: [owner.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/owner.yaml",
		testutil.KustomizationYAML(
			"owner",
			"",
			"  path: ./target\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "target/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "target/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dirty}\n")
	st := store.New()
	repository := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	artifact := &store.SourceArtifact{
		Provider:  store.ProviderCommit,
		LocalRoot: root,
		LocalPath: t.TempDir(),
		Key:       "pinned",
		Content:   "pinned",
	}
	artifact.FS = os.DirFS(artifact.LocalPath)
	st.AddObject(repository)
	st.SetArtifact(repository.Named(), artifact)
	d := Session{cfg: Config{Store: st}, loader: loader.New(st)}
	d.loader.SourceRoot = root
	{
		_, err := d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assert.NoError(t, d.discardPinnedWorkingTreeFiles(t.Context(), root))
	owner := testutil.KustomizationID("control", "owner")
	{
		_, ok := d.loader.Index.Lookup(owner)
		assert.FatalIf(t, !ok, "%v", "admission lost the projected pinned owner")
	}
	{
		_, ok := d.loader.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "dirty"})
		assert.FatalIf(t, ok, "%v", "working-tree object survived the projected pinned claim")
	}
	assert.FatalIf(t, st.GetObject(repository.Named()) != repository ||
		st.GetArtifact(
			repository.Named(),
			store.ArtifactContent,
		) != artifact, "%v", "projected pinned claim replaced its resolved source")
}

func testDiscardPinnedWorkingTreeFilesPreservesSourcesCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		resolved bool
	},
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	repository := &manifest.GitRepository{
		Name: "cluster", Namespace: "flux-system", URL: "git://fixture.invalid/cluster",
		Reference: &sourcev1.GitRepositoryRef{Tag: "pinned"},
	}
	other := &manifest.GitRepository{Name: "other", Namespace: "flux-system"}
	ks := &manifest.Kustomization{
		Name:      "root",
		Namespace: "flux-system",
		Path:      "./",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      repository.Name,
			Namespace: repository.Namespace,
		},
	}
	cm := &manifest.ConfigMap{Name: "dirty", Namespace: "flux-system"}
	artifact := &store.SourceArtifact{
		Provider:  store.ProviderCommit,
		Kind:      manifest.KindGitRepository,
		LocalRoot: root,
		LocalPath: t.TempDir(),
		Key:       "pinned",
		Content:   "pinned",
	}
	artifact.FS = os.DirFS(artifact.LocalPath)
	d := Session{
		cfg:    Config{Store: st},
		loader: loader.New(st),
		resolvedSources: &entrySourcePublisher{
			mutable: map[manifest.NamedResource]entryResolution{other.Named(): {object: other}},
		},
	}
	d.loader.SourceRoot = root
	if testCaseInput.resolved {
		d.resolvedSources.record(repository.Named(), &entryResolution{object: repository})
	}
	for _, obj := range []manifest.BaseManifest{repository, other, ks, cm} {
		st.AddObject(obj)
		file := obj.Named().Name + ".yaml"
		d.loader.Index.Records[obj.Named()] = manifest.DiscoveryRecord{
			File: file,
			Path: filepath.Join(root, file),
		}
	}
	r := d.loader.Index.Records[ks.Named()]
	r.Refs = []manifest.NamedResource{repository.Named()}
	d.loader.Index.Records[ks.Named()] = r
	for _, repo := range []*manifest.GitRepository{repository, other} {
		st.SetArtifact(repo.Named(), artifact)
		st.UpdateStatus(
			repo.Named(),
			store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "pinned"},
		)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.Equal(t, d.discardPinnedWorkingTreeFiles(ctx, root), ctx.Err())
	for _, obj := range []manifest.BaseManifest{repository, other, ks, cm} {
		assert.Equal(t, st.GetObject(obj.Named()), obj)
		record, indexed := d.loader.Index.Lookup(obj.Named())
		assert.Equal(t, indexed, true)
		assert.Equal(t, record.Path, filepath.Join(root, obj.Named().Name+".yaml"))
		assert.Equal(t, record.File, obj.Named().Name+".yaml")
	}

	assert.NoError(t, d.discardPinnedWorkingTreeFiles(t.Context(), root))
	for _, repo := range []*manifest.GitRepository{repository, other} {
		id := repo.Named()
		status, ok := st.GetStatus(id)
		assert.FatalIf(
			t,
			st.GetObject(id) != repo || st.GetArtifact(id, store.ArtifactContent) != artifact || !ok ||
				status.Status != store.StatusReady,
			"source %s lost its resolved object, artifact or status",
			id,
		)
		{
			_, ok := d.loader.Index.Lookup(id)
			assert.FatalIf(
				t,
				!ok || filesForTest(d.loader.Index)[id] == "",
				"source %s lost discovery metadata",
				id,
			)
		}
	}
	assert.FatalIf(
		t,
		st.GetObject(ks.Named()) != ks || len(d.loader.Index.Records[ks.Named()].Refs) != 1,
		"%v",
		"root Kustomization lost its object or source reference",
	)
	{
		_, ok := d.loader.Index.Lookup(
			cm.Named(),
		)
		assert.FatalIf(t, ok || st.GetObject(cm.Named()) != nil ||
			filesForTest(d.loader.Index)[cm.Named()] != "", "%v", "working-tree object survived the pinned root claim")
	}
	testutil.WriteFile(t, artifact.LocalPath, "source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', ref: {tag: other}}
`)
	d.loader.PreferExisting = true
	{
		_, err := d.loader.Load(t.Context(), artifact.LocalPath, manifest.NamedResource{}, nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assert.FatalIf(t, st.GetObject(repository.Named()) != repository ||
		st.GetArtifact(
			repository.Named(),
			store.ArtifactContent,
		) != artifact, "%v", "committed subtree replaced the resolved source")
}

func testLoadAtIgnoreFileAndMissingDirectoryMountedSymlink(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "host/kustomization.yaml", "resources: []\n")
	{
		err := os.Symlink(filepath.Join(root, "host"), filepath.Join(root, "apps"))
		assert.FatalIf(t, err != nil, "%v", err)
	}
	input := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [ks.yaml]\n")},
		"ks.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"owner",
					"",
					"  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n",
				),
			),
		},
		"apps/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
		"apps/child.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"child",
					"",
					"  {path: ./output, sourceRef: {kind: GitRepository, name: source}}\n",
				),
			),
		},
	}
	st := store.New()
	source := &manifest.GitRepository{Name: "source"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: input})
	st.SetArtifact(
		manifest.BootstrapSourceID,
		st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact),
	)
	d := Session{
		cfg: testConfig(t, Config{
			Path:  root,
			Store: st,
			Input: st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
		}),
		loader: loader.New(st),
	}
	{
		err := d.loadManifestsTest(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	id := testutil.KustomizationID("", "child")
	{
		r, ok := d.loader.Index.Lookup(id)
		assert.FatalIf(
			t,
			!ok || r.File != "apps/child.yaml" || st.GetObject(id) != nil,
			"mounted child redirected by host symlink: %+v present=%t",
			r,
			ok,
		)
	}
}

type testLoadAtIgnoreFileAndMissingDirectoryCaseInputs struct {
	backing  *string
	scenario *string
}

func testLoadAtIgnoreFileAndMissingDirectoryCase(
	t *testing.T,
	testCaseInput testLoadAtIgnoreFileAndMissingDirectoryCaseInputs,
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	d := Session{cfg: Config{Store: st}, loader: loader.New(st)}
	d.loader.IgnoreFile = filepath.Join(root, "override.krmignore")
	target := filepath.Join(root, "missing")
	if (*testCaseInput.scenario) == "missing-ignore" {
		target = root
	} else {
		testutil.WriteFile(t, root, "override.krmignore", "")
	}
	if (*testCaseInput.scenario) == "file" {
		testutil.WriteFile(t, root, "missing", "content")
	}
	if (*testCaseInput.backing) == "memory" {
		input := fstest.MapFS{}
		if (*testCaseInput.scenario) != "missing-ignore" {
			input["override.krmignore"] = &fstest.MapFile{}
		}
		if (*testCaseInput.scenario) == "file" {
			input["missing"] = &fstest.MapFile{Data: []byte("content")}
		}
		st.AddObject(&manifest.GitRepository{Name: "source"})
		st.SetArtifact(
			manifest.NamedResource{
				Group: "source.toolkit.fluxcd.io",
				Kind:  manifest.KindGitRepository,
				Name:  "source",
			},
			&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: input},
		)
	}
	total := 0
	_, err := d.loadAt(t.Context(), target, &total, &sourceLoad{})
	if (*testCaseInput.scenario) == "missing-ignore" {
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "missing explicit ignore error = %v", err)
	} else {
		assert.FatalIf(
			t,
			err != nil || total != 0,
			"missing followed path: loaded=%d, error=%v",
			total,
			err,
		)
	}
}

func testRunSelectedEntryAdmissionCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	const root = "/virtual/entry-admission"
	input := fstest.MapFS{
		"app/kustomization.yaml": {Data: []byte("namespace: apps\nresources: [cm.yaml]\n")},
		"app/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: applied}\ndata: {key: value}\n"),
		},
	}
	st := store.New()
	authored := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./app"}
	switch *testCaseInput {
	case "alias", "alias ignored", "alias sparse", "pinned":
		source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://fixture.invalid/self"}
		artifact := &store.SourceArtifact{
			LocalPath: "/virtual/alias",
			LocalRoot: root,
			Provider:  store.ProviderAlias,
			FS:        input,
		}
		if (*testCaseInput) == "alias ignored" || (*testCaseInput) == "alias sparse" {
			if (*testCaseInput) == "alias ignored" {
				ignore := "app/cm.yaml"
				source.Ignore = &ignore
			} else {
				source.SparseCheckout = []string{"other"}
			}
			artifact.FS = fstest.MapFS{"app/kustomization.yaml": input["app/kustomization.yaml"]}
		}
		st.AddObject(source)
		if (*testCaseInput) == "pinned" {
			artifact.Provider = store.ProviderCommit
			artifact.Key = "pinned"
			artifact.Content = "pinned"
		}
		st.SetArtifact(source.Named(), artifact)
		authored.SourceRef = kustomizev1.CrossNamespaceSourceReference{
			Kind:      source.Named().Kind,
			Name:      source.Name,
			Namespace: source.Namespace,
		}
	case "fragment":
		authored.Path = ""
	case "label collision":
		authored.Name = "entry"
		authored.Path = "./other"
	case "deferred", "optional":
		authored.PostBuild = &kustomizev1.PostBuild{
			SubstituteFrom: []kustomizev1.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "values", Optional: (*testCaseInput) == "optional"},
			},
		}
	}
	if (*testCaseInput) != "internal" {
		st.AddObject(authored)
	}
	if (*testCaseInput) == "authored" {
		input["app/kustomization.yaml"].Data = []byte("namespace: apps\nresources: [cm.yaml, owner.yaml]\n")
		input["app/owner.yaml"] = &fstest.MapFile{
			Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./app\n")),
		}
	}
	cacheDir := ""
	if (*testCaseInput) == "internal" || (*testCaseInput) == "authored" {
		cacheDir = t.TempDir()
	}
	testRunSelectedEntryAdmissionCaseTail(
		t,
		&testRunSelectedEntryAdmissionCaseTailInputs{
			cacheDir:      &cacheDir,
			st:            &st,
			root:          root,
			input:         &input,
			testCaseInput: &testCaseInput,
			authored:      &authored,
		},
	)
}

func testRunSelectedEntryAdmissionCaseTail(t *testing.T, tailCaseInput *testRunSelectedEntryAdmissionCaseTailInputs) {
	t.Helper()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	testReceiptTreesResult1 := testReceiptTrees(t, (*tailCaseInput.cacheDir), persistent)
	cacheOwner, trees, closeCache := testReceiptTreesResult1.owner, testReceiptTreesResult1.trees, testReceiptTreesResult1.close
	cfg := Config{
		Cache: cacheOwner,
		Trees: trees,
		Store: (*tailCaseInput.st),
		Input: &store.SourceArtifact{
			Provider:  store.ProviderInput,
			LocalPath: tailCaseInput.root,
			FS:        (*tailCaseInput.input),
		},
		Path: tailCaseInput.root + "/app",
	}
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	internal := (*(*tailCaseInput.testCaseInput)) == "internal" ||
		(*(*tailCaseInput.testCaseInput)) == "alias ignored" ||
		(*(*tailCaseInput.testCaseInput)) == "alias sparse" ||
		(*(*tailCaseInput.testCaseInput)) ==
			"pinned" ||
		(*(*tailCaseInput.testCaseInput)) ==
			"fragment" ||
		(*(*tailCaseInput.testCaseInput)) ==
			"label collision"
	assert.FatalIf(t, result.Binding.Producer.ReportHidden() != internal,
		"wrong entry representative: %+v",
		result.Binding)
	assert.FatalIf(t, !internal && result.Binding.Producer != (*tailCaseInput.authored).Named(),
		"authored owner not reused: %+v",
		result.Binding)

	assert.FatalIf(t, (*(*tailCaseInput.testCaseInput)) == "label collision" &&
		((*tailCaseInput.authored).Named().ReportHidden() || (*tailCaseInput.st).GetObject((*tailCaseInput.authored).Named()) != (*tailCaseInput.authored)), "%v", "valid authored display name was hidden or replaced")
	{
		record, _ := result.Index.Lookup(result.Binding.Producer)
		assert.FatalIf(t, record.Parent != (manifest.NamedResource{}), "entry is its own child: %+v", record)
	}
	applied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "applied"}
	record, ok := result.Index.Lookup(applied)
	assert.FatalIf(t, !ok ||
		!slices.ContainsFunc(
			record.Productions,
			func(p manifest.Production) bool { return p.Owner == result.Binding.Producer },
		), "root output lacks ordinary ownership: %+v", record)
	assert.FatalIf(
		t,
		((*(*tailCaseInput.testCaseInput)) == "alias ignored" || (*(*tailCaseInput.testCaseInput)) == "alias sparse") &&
			record.Builder != "app/kustomization.yaml",
		"filtered alias changed the prepared literal input: %+v",
		record,
	)
	if _, _, emission := (*tailCaseInput.st).GetEmission(
		result.Binding.Producer,
	); emission.Complete ||
		len(emission.Manifests) != 0 {
		emission.Finish()
		t.Fatal("preparation published output")
	} else {
		emission.Finish()
	}
	if (*(*tailCaseInput.testCaseInput)) == "deferred" || (*(*tailCaseInput.testCaseInput)) == "optional" {
		assert.FatalIf(t, record.Builder != "", "referenced-data entry prepared early: %+v", record)
	}
	if (*tailCaseInput.cacheDir) != "" {
		owner := (*tailCaseInput.st).GetObject(result.Binding.Producer).(*manifest.Kustomization)
		source, err := kustomize.SourceInput((*tailCaseInput.st), owner)
		assert.NoError(t, err)
		req := preparedRequest(t, &cfg, owner, source)
		want := map[manifest.NamedResource]manifest.DiscoveryRecord{
			applied: {
				Productions: []manifest.Production{{Owner: owner.Named(), Source: result.Binding.Source}},
			},
		}
		if (*(*tailCaseInput.testCaseInput)) == "authored" {
			ownRecord, ok := result.Index.Lookup(owner.Named())
			assert.FatalIf(
				t,
				!ok || ownRecord.Parent != (manifest.NamedResource{}),
				"prepared owner declaration became its own child: %+v",
				ownRecord,
			)
			assert.Equal(t, ownRecord.File, "app/owner.yaml")
			assert.Equal(t, ownRecord.Declaration != nil, true)
		}
		if !persistent {
			want = nil
		}
		assert.Diff(t, packageHistory(t, trees, &req, persistent), want)
		closeCache()
		testReceiptTreesResult2 := testReceiptTrees(t, (*tailCaseInput.cacheDir), persistent)
		trees, closeCache = testReceiptTreesResult2.trees, testReceiptTreesResult2.close
		assert.Diff(t, packageHistory(t, trees, &req, persistent), want)
	}
	closeCache()
}

type testRunSelectedEntryAdmissionCaseTailInputs struct {
	cacheDir *string

	st **store.
		Store

	root string

	input *fstest.
		MapFS

	testCaseInput **string

	authored **manifest.
			Kustomization
}
