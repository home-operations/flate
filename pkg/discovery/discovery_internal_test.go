package discovery

import (
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

func TestDiscardPinnedWorkingTreeFiles_PreservesSources(t *testing.T) {
	t.Parallel()
	t.Run("projected owner", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "flux/kustomization.yaml", "namespace: control\nresources: [owner.yaml]\n")
		testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "", "  path: ./target\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"))
		testutil.WriteFile(t, root, "target/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "target/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dirty}\n")
		st := store.New()
		repository := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
		artifact := &store.SourceArtifact{LocalRoot: root, LocalPath: t.TempDir(), Key: "pinned", Content: "pinned"}
		artifact.FS = os.DirFS(artifact.LocalPath)
		st.AddObject(repository)
		st.SetArtifact(repository.Named(), artifact)
		d := discoverer{cfg: Config{Store: st}, loader: loader.New(st)}
		d.loader.SourceRoot = root
		if _, err := d.loader.Load(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		d.discardPinnedWorkingTreeFiles(root)
		owner := testutil.KustomizationID("control", "owner")
		if _, ok := d.loader.Index.Lookup(owner); !ok {
			t.Fatal("admission lost the projected pinned owner")
		}
		if _, ok := d.loader.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "dirty"}); ok {
			t.Fatal("working-tree object survived the projected pinned claim")
		}
		if st.GetObject(repository.Named()) != repository || st.GetArtifact(repository.Named(), store.ArtifactContent) != artifact {
			t.Fatal("projected pinned claim replaced its resolved source")
		}
	})
	for _, tc := range []struct {
		name     string
		resolved bool
	}{
		{name: "resolved", resolved: true},
		{name: "referenced"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			repository := &manifest.GitRepository{
				Name: "cluster", Namespace: "flux-system", URL: "git://fixture.invalid/cluster",
				Reference: &sourcev1.GitRepositoryRef{Tag: "pinned"},
			}
			other := &manifest.GitRepository{Name: "other", Namespace: "flux-system"}
			ks := &manifest.Kustomization{
				Name: "root", Namespace: "flux-system", Path: "./",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: repository.Name, Namespace: repository.Namespace},
			}
			cm := &manifest.ConfigMap{Name: "dirty", Namespace: "flux-system"}
			artifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalRoot: root, LocalPath: t.TempDir(), Key: "pinned", Content: "pinned"}
			artifact.FS = os.DirFS(artifact.LocalPath)
			d := discoverer{
				cfg: Config{Store: st}, loader: loader.New(st),
				resolvedSources: map[manifest.NamedResource]entryResolution{other.Named(): {object: other}},
			}
			d.loader.SourceRoot = root
			if tc.resolved {
				d.resolvedSources[repository.Named()] = entryResolution{object: repository}
			}
			for _, obj := range []manifest.BaseManifest{repository, other, ks, cm} {
				st.AddObject(obj)
				file := obj.Named().Name + ".yaml"
				d.loader.Index.Records[obj.Named()] = manifest.DiscoveryRecord{File: file, Path: filepath.Join(root, file)}
			}
			r := d.loader.Index.Records[ks.Named()]
			r.Refs = []manifest.NamedResource{repository.Named()}
			d.loader.Index.Records[ks.Named()] = r
			for _, repo := range []*manifest.GitRepository{repository, other} {
				st.SetArtifact(repo.Named(), artifact)
				st.UpdateStatus(repo.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "pinned"})
			}
			d.discardPinnedWorkingTreeFiles(root)
			for _, repo := range []*manifest.GitRepository{repository, other} {
				id := repo.Named()
				status, ok := st.GetStatus(id)
				if st.GetObject(id) != repo || st.GetArtifact(id, store.ArtifactContent) != artifact || !ok || status.Status != store.StatusReady {
					t.Fatalf("source %s lost its resolved object, artifact or status", id)
				}
				if _, ok := d.loader.Index.Lookup(id); !ok || filesForTest(d.loader.Index)[id] == "" {
					t.Fatalf("source %s lost discovery metadata", id)
				}
			}
			if st.GetObject(ks.Named()) != ks || len(d.loader.Index.Records[ks.Named()].Refs) != 1 {
				t.Fatal("root Kustomization lost its object or source reference")
			}
			if _, ok := d.loader.Index.Lookup(cm.Named()); ok || st.GetObject(cm.Named()) != nil || filesForTest(d.loader.Index)[cm.Named()] != "" {
				t.Fatal("working-tree object survived the pinned root claim")
			}
			testutil.WriteFile(t, artifact.LocalPath, "source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster', ref: {tag: other}}
`)
			d.loader.PreferExisting = true
			if _, err := d.loader.Load(t.Context(), artifact.LocalPath); err != nil {
				t.Fatal(err)
			}
			if st.GetObject(repository.Named()) != repository || st.GetArtifact(repository.Named(), store.ArtifactContent) != artifact {
				t.Fatal("committed subtree replaced the resolved source")
			}

		})
	}
}

func TestLoadAt_IgnoreFileAndMissingDirectory(t *testing.T) {
	t.Parallel()
	t.Run("mounted symlink", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "host/kustomization.yaml", "resources: []\n")
		if err := os.Symlink(filepath.Join(root, "host"), filepath.Join(root, "apps")); err != nil {
			t.Fatal(err)
		}
		input := fstest.MapFS{
			"kustomization.yaml":      {Data: []byte("resources: [ks.yaml]\n")},
			"ks.yaml":                 {Data: []byte(testutil.KustomizationYAML("owner", "", "  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n"))},
			"apps/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
			"apps/child.yaml":         {Data: []byte(testutil.KustomizationYAML("child", "", "  {path: ./output, sourceRef: {kind: GitRepository, name: source}}\n"))},
		}
		st := store.New()
		source := &manifest.GitRepository{Name: "source"}
		st.AddObject(source)
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, FS: input})
		st.SetArtifact(manifest.BootstrapSourceID, st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact))
		d := discoverer{cfg: Config{Path: root, Store: st, Trees: testTrees(t)}, loader: loader.New(st)}
		if err := d.loadManifests(t.Context(), root, true); err != nil {
			t.Fatal(err)
		}
		id := testutil.KustomizationID("", "child")
		if r, ok := d.loader.Index.Lookup(id); !ok || r.File != "apps/child.yaml" || st.GetObject(id) != nil {
			t.Fatalf("mounted child redirected by host symlink: %+v present=%t", r, ok)
		}
	})

	for _, backing := range []string{"disk", "memory"} {
		for _, scenario := range []string{"missing-directory", "file", "missing-ignore"} {
			t.Run(backing+"/"+scenario, func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				st := store.New()
				d := discoverer{cfg: Config{Store: st}, loader: loader.New(st)}
				d.loader.IgnoreFile = filepath.Join(root, "override.krmignore")
				target := filepath.Join(root, "missing")
				if scenario == "missing-ignore" {
					target = root
				} else {
					testutil.WriteFile(t, root, "override.krmignore", "")
				}
				if scenario == "file" {
					testutil.WriteFile(t, root, "missing", "content")
				}
				if backing == "memory" {
					input := fstest.MapFS{}
					if scenario != "missing-ignore" {
						input["override.krmignore"] = &fstest.MapFile{}
					}
					if scenario == "file" {
						input["missing"] = &fstest.MapFile{Data: []byte("content")}
					}
					st.AddObject(&manifest.GitRepository{Name: "source"})
					st.SetArtifact(manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Name: "source"}, &store.SourceArtifact{LocalPath: root, FS: input})
				}
				total := 0
				err := d.loadAt(t.Context(), target, &total)
				if scenario == "missing-ignore" {
					if !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("missing explicit ignore error = %v", err)
					}
				} else if err != nil || total != 0 {
					t.Fatalf("missing followed path: loaded=%d, error=%v", total, err)
				}
			})
		}
	}
}

func TestRun_SelectedEntryAdmission(t *testing.T) {
	t.Parallel()
	for _, ownership := range []string{"internal", "authored", "alias", "alias ignored", "alias sparse", "pinned", "fragment", "label collision", "deferred", "optional"} {
		t.Run(ownership, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/entry-admission"
			input := fstest.MapFS{
				"app/kustomization.yaml": {Data: []byte("namespace: apps\nresources: [cm.yaml]\n")},
				"app/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: applied}\ndata: {key: value}\n")},
			}
			st := store.New()
			authored := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./app"}
			switch ownership {
			case "alias", "alias ignored", "alias sparse", "pinned":
				source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://fixture.invalid/self"}
				artifact := &store.SourceArtifact{LocalPath: "/virtual/alias", LocalRoot: root, InputAlias: true, FS: input}
				if ownership == "alias ignored" || ownership == "alias sparse" {
					if ownership == "alias ignored" {
						ignore := "app/cm.yaml"
						source.Ignore = &ignore
					} else {
						source.SparseCheckout = []string{"other"}
					}
					artifact.FS = fstest.MapFS{"app/kustomization.yaml": input["app/kustomization.yaml"]}
				}
				st.AddObject(source)
				if ownership == "pinned" {
					artifact.InputAlias = false
					artifact.Key = "pinned"
					artifact.Content = "pinned"
				}
				st.SetArtifact(source.Named(), artifact)
				authored.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name, Namespace: source.Namespace}
			case "fragment":
				authored.Path = ""
			case "label collision":
				authored.Name = "entry"
				authored.Path = "./other"
			case "deferred", "optional":
				authored.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "values", Optional: ownership == "optional"}}}
			}
			if ownership != "internal" {
				st.AddObject(authored)
			}
			if ownership == "authored" {
				input["app/kustomization.yaml"].Data = []byte("namespace: apps\nresources: [cm.yaml, owner.yaml]\n")
				input["app/owner.yaml"] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./app\n"))}
			}
			cacheDir := ""
			if ownership == "internal" || ownership == "authored" {
				cacheDir = t.TempDir()
			}
			trees, closeCache := testReceiptTrees(t, cacheDir)
			cfg := Config{Trees: trees, Store: st, Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root + "/app"}
			result, err := Run(t.Context(), cfg)
			assert.NoError(t, err)
			internal := ownership == "internal" || ownership == "alias ignored" || ownership == "alias sparse" || ownership == "pinned" || ownership == "fragment" || ownership == "label collision"
			if result.Binding.Producer.ReportHidden() != internal {
				t.Fatalf("wrong entry representative: %+v", result.Binding)
			}
			if !internal && result.Binding.Producer != authored.Named() {
				t.Fatalf("authored owner not reused: %+v", result.Binding)
			}
			if ownership == "label collision" && (authored.Named().ReportHidden() || st.GetObject(authored.Named()) != authored) {
				t.Fatal("valid authored display name was hidden or replaced")
			}
			if record, _ := result.Index.Lookup(result.Binding.Producer); record.Parent != (manifest.NamedResource{}) {
				t.Fatalf("entry is its own child: %+v", record)
			}
			applied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "applied"}
			record, ok := result.Index.Lookup(applied)
			if !ok || !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return p.Owner == result.Binding.Producer }) {
				t.Fatalf("root output lacks ordinary ownership: %+v", record)
			}
			if (ownership == "alias ignored" || ownership == "alias sparse") && record.Builder != "app/kustomization.yaml" {
				t.Fatalf("filtered alias changed the prepared literal input: %+v", record)
			}
			if _, _, emission := st.GetEmission(result.Binding.Producer); emission.Complete || len(emission.Manifests) != 0 {
				emission.Finish()
				t.Fatal("preparation published output")
			} else {
				emission.Finish()
			}
			if ownership == "deferred" || ownership == "optional" {
				if record.Builder != "" {
					t.Fatalf("referenced-data entry prepared early: %+v", record)
				}
			}
			if cacheDir != "" {
				owner := st.GetObject(result.Binding.Producer).(*manifest.Kustomization)
				source, _, err := kustomize.SourceInput(st, owner)
				assert.NoError(t, err)
				req := preparedRequest(t, cfg, owner, source)
				want := map[manifest.NamedResource]manifest.DiscoveryRecord{applied: {Productions: []manifest.Production{{Owner: owner.Named(), Source: result.Binding.Source}}}}
				if ownership == "authored" {
					ownRecord, ok := result.Index.Lookup(owner.Named())
					if !ok || ownRecord.Parent != (manifest.NamedResource{}) {
						t.Fatalf("prepared owner declaration became its own child: %+v", ownRecord)
					}
					assert.Equal(t, ownRecord.File, "app/owner.yaml")
					assert.Equal(t, ownRecord.Declaration != nil, true)
				}
				assert.Diff(t, packageHistory(t, trees, req, owner.Named()), want)
				closeCache()
				trees, closeCache = testReceiptTrees(t, cacheDir)
				assert.Diff(t, packageHistory(t, trees, req, owner.Named()), want)
			}
			closeCache()
		})
	}
}

func testTrees(t *testing.T) *kustomize.TreeCache {
	t.Helper()
	trees := kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2))
	t.Cleanup(trees.Close)
	return trees
}

func testReceiptTrees(t *testing.T, cacheDir string) (*kustomize.TreeCache, func()) {
	t.Helper()
	owner, err := cache.New(t.Context(), cacheDir)
	assert.NoError(t, err)
	trees := kustomize.NewTreeCache(owner, nil, task.NewBounded(2))
	closeCache := func() {
		t.Helper()
		trees.Close()
		assert.NoError(t, owner.Close())
	}
	t.Cleanup(closeCache)
	if cacheDir != "" && !owner.CanPersistRenders() {
		t.Skipf("persistent receipt proof requires an eligible build: %s", cache.RenderPersistenceDisabledReason())
	}
	return trees, closeCache
}

func packageHistory(t *testing.T, trees *kustomize.TreeCache, req kustomize.RenderRequest, owner manifest.NamedResource) map[manifest.NamedResource]manifest.DiscoveryRecord {
	t.Helper()
	records, ok := trees.PreviousRender(t.Context(), req, owner)
	if !ok || records == nil {
		t.Fatalf("package %q has no complete accepted history", req.SubPath)
	}
	history := maps.Collect(records)
	return history
}

func preparedRequest(t *testing.T, cfg Config, ks *manifest.Kustomization, source *store.SourceArtifact) kustomize.RenderRequest {
	t.Helper()
	prepared, err := kustomize.Prepare(ks, nil)
	assert.NoError(t, err)
	return kustomize.RenderRequest{
		Source: source, ApplyIgnore: ks.SourceID() == manifest.BootstrapSourceID || source.InputAlias || source.Content == "", SubPath: prepared.Path, Spec: prepared,
		WipeSecrets: cfg.WipeSecrets, StrictSubstitutions: cfg.StrictSubstitutions, SkipKinds: cfg.SkipKinds,
	}
}

func TestRun_BuiltEntryFixedPoint(t *testing.T) {
	t.Parallel()
	const root = "/virtual/built-fixed-point"
	input := fstest.MapFS{
		"app/kustomization.yaml":  {Data: []byte("resources: [child.yaml, source.yaml]\nnameSuffix: -dev\npatches:\n- target: {kind: Kustomization, name: child}\n  patch: '[{\"op\":\"replace\",\"path\":\"/spec/path\",\"value\":\"./leaf\"}]'\n")},
		"app/child.yaml":          {Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./untransformed\n  sourceRef: {kind: GitRepository, name: nested-dev}\n"))},
		"app/source.yaml":         {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: nested, namespace: apps}\nspec: {url: https://fixture.invalid/self}\n")},
		"leaf/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"leaf/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n")},
	}
	st := store.New()
	result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root + "/app", Store: st, Trees: testTrees(t), SelfURLs: []string{"https://fixture.invalid/self"}})
	assert.NoError(t, err)
	child := testutil.KustomizationID("apps", "child-dev")
	record, ok := result.Index.Lookup(child)
	if !ok || record.Parent != result.Binding.Producer || st.GetObject(testutil.KustomizationID("apps", "child")) != nil {
		t.Fatalf("effective declaration not bound: %+v", record)
	}
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "nested-dev"}
	if st.GetArtifact(source, store.ArtifactContent) == nil {
		t.Fatal("effective self source not admitted")
	}
	leaf := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "leaf"}
	record, ok = result.Index.Lookup(leaf)
	if !ok || !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return p.Owner == child }) {
		t.Fatalf("effective path not expanded: %+v", record)
	}
}

func TestRun_EntryOwnerEligibility(t *testing.T) {
	t.Parallel()
	t.Run("independent", func(t *testing.T) {
		t.Parallel()
		const root = "/virtual/independent-entry-owner"
		input := fstest.MapFS{
			"entry/kustomization.yaml": {Data: []byte("resources: [owner.yaml, cm.yaml]\n")},
			"entry/owner.yaml":         {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n  dependsOn: [{name: ready}]\n"))},
			"entry/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\n")},
			"ready/kustomization.yaml": {Data: []byte("resources: []\n")},
		}
		st := store.New()
		st.AddObject(&manifest.Kustomization{Name: "ready", Namespace: "apps", Path: "./ready"})
		result, err := Run(t.Context(), Config{Store: st, Trees: testTrees(t), Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root + "/entry"})
		assert.NoError(t, err)
		assert.Equal(t, result.Binding.Producer, testutil.KustomizationID("apps", "owner"))
	})
	for _, scenario := range []string{"transitive_index_only", "transitive_admitted", "effective_batch", "generated_target", "production_declaration", "owner_production", "foreign_production", "cycle", "self_cycle", "deferred_required", "deferred_optional", "stable_then_changed"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/entry-eligibility"
			input := fstest.MapFS{
				"entry/kustomization.yaml": {Data: []byte("resources: [owner.yaml, sibling.yaml]\n")},
				"entry/owner.yaml":         {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n  dependsOn: [{name: ready}]\n"))},
				"entry/sibling.yaml":       {Data: []byte(testutil.KustomizationYAML("sibling", "apps", "  path: ./leaf\n"))},
				"leaf/kustomization.yaml":  {Data: []byte("resources: []\n")},
			}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
			owner.DependsOn = []kustomizev1.DependencyReference{{Name: "ready"}}
			ready := &manifest.Kustomization{Name: "ready", Namespace: "apps", Path: "./ready"}
			ready.DependsOn = []kustomizev1.DependencyReference{{Name: "sibling"}}
			if scenario == "effective_batch" {
				input["entry/sibling.yaml"].Data = []byte(testutil.KustomizationYAML("before", "other", "  path: ./leaf\n"))
				input["entry/kustomization.yaml"].Data = []byte("namespace: apps\nresources: [sibling.yaml]\npatches:\n- target: {kind: Kustomization, name: before}\n  patch: '[{\"op\":\"replace\",\"path\":\"/metadata/name\",\"value\":\"sibling\"}]'\n")
			}
			if scenario == "generated_target" {
				ready.DependsOn = nil
				ready.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: "generated"}
				input["entry/kustomization.yaml"].Data = []byte("resources: [generator.yaml]\n")
				input["entry/generator.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps}\nspec:\n  artifacts: [{name: generated}]\n")}
			}
			if scenario == "cycle" {
				ready.DependsOn[0].Name = "loop"
			}
			if scenario == "self_cycle" {
				owner.DependsOn[0].Name = owner.Name
			}
			if scenario == "deferred_required" || scenario == "deferred_optional" {
				owner.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "values", Optional: scenario == "deferred_optional"}}}
			}
			st := store.New()
			st.AddObject(owner)
			if scenario == "transitive_admitted" {
				st.AddObject(ready)
			}
			d := newDiscoverer(Config{Store: st, Trees: testTrees(t), Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root + "/entry"})
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.NoError(t, err)
			d.loader.SourceRoot = root
			_, err = d.loader.Load(t.Context(), root+"/entry")
			assert.NoError(t, err)
			d.loader.Index.Records[ready.Named()] = manifest.DiscoveryRecord{Declaration: ready}
			if scenario == "cycle" {
				loop := &manifest.Kustomization{Name: "loop", Namespace: "apps"}
				loop.DependsOn = []kustomizev1.DependencyReference{{Name: ready.Name}}
				d.loader.Index.Records[loop.Named()] = manifest.DiscoveryRecord{Declaration: loop}
			}
			if scenario == "production_declaration" || scenario == "owner_production" || scenario == "foreign_production" {
				ready.DependsOn = nil
				source := manifest.BootstrapSourceID
				if scenario == "foreign_production" {
					source.Name = "foreign"
				}
				d.loader.Index.Records[ready.Named()] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: testutil.KustomizationID("apps", "producer"), Declaration: testutil.KustomizationID("apps", "sibling"), Source: source}}}
				if scenario == "owner_production" {
					d.loader.Index.Records[ready.Named()] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: owner.Named(), Source: source}}}
				}
			}
			if scenario != "transitive_admitted" && st.GetObject(ready.Named()) != nil || len(d.loader.Index.Records[ready.Named()].Refs) != 0 {
				t.Fatal("index-only prerequisite gained store or ordering-ref authority")
			}
			selected := d.bindEntry(t.Context(), root)
			wantOwner := scenario == "cycle" || scenario == "self_cycle" || scenario == "foreign_production"
			assert.Equal(t, selected == owner, wantOwner)
			if !wantOwner && !selected.Named().ReportHidden() {
				t.Fatal("captured prerequisite did not select the literal entry")
			}
			if scenario == "deferred_required" || scenario == "deferred_optional" {
				if _, prepared := d.prepared[owner]; prepared {
					t.Fatal("referenced substitution admitted an incomplete preparation")
				}
			}
			if scenario == "effective_batch" {
				if !slices.ContainsFunc(d.prepared[owner].children, func(c store.Child) bool { return c.Obj.Named() == testutil.KustomizationID("apps", "sibling") }) {
					t.Fatal("eligibility did not inspect the transformed batch")
				}
			}
			if scenario == "stable_then_changed" {
				previous := d.prepared[owner].receipt
				input, _, err := kustomize.SourceInput(st, selected)
				assert.NoError(t, err)
				assert.NoError(t, d.prepareEntry(t.Context(), selected, input))
				declaration := d.loader.Index.Records[owner.Named()].Declaration
				if declaration == nil || !manifest.Equal(declaration, owner) {
					t.Fatal("literal registration erased the original authored declaration")
				}
				assert.Equal(t, d.bindEntry(t.Context(), root), selected)
				assert.Equal(t, d.prepared[owner].receipt, previous)
				independent := *ready
				independent.DependsOn = nil
				d.loader.Index.Records[ready.Named()] = manifest.DiscoveryRecord{Declaration: &independent}
				assert.Equal(t, d.bindEntry(t.Context(), root), owner)
				assert.Equal(t, d.prepared[owner].receipt, previous)
				if len(ready.DependsOn) != 1 {
					t.Fatal("eligibility mutated the borrowed prerequisite")
				}
				projected := *owner
				projected.DependsOn = []kustomizev1.DependencyReference{{Name: "${NEXT}", Namespace: "${NS}"}}
				st.AddObject(&projected)
				vars := func(value string) values.DependencySubstitutions {
					return values.NewDependencySubstitutions([]*manifest.Kustomization{{PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"NEXT": value, "NS": "apps"}}}})
				}
				d.substitutions = vars("sibling")
				if !d.bindEntry(t.Context(), root).Named().ReportHidden() {
					t.Fatal("new owner/snapshot retained the independent decision")
				}
				d.substitutions = vars("ready")
				assert.Equal(t, d.bindEntry(t.Context(), root), &projected)
				before := d.prepared[&projected]
				replacement := *before.source
				replacement.Key = "new-source"
				replacement.Content = "new-content"
				st.SetArtifact(manifest.BootstrapSourceID, &replacement)
				assert.Equal(t, d.bindEntry(t.Context(), root), &projected)
				assert.Equal(t, d.prepared[&projected].source, &replacement)
			}
		})
	}
}

func TestRun_BuiltEntryDoesNotRebindTransformedOwner(t *testing.T) {
	t.Parallel()
	const root = "/virtual/transformed-entry-owner"
	input := fstest.MapFS{
		"app/kustomization.yaml": {Data: []byte("resources: [owner.yaml]\npatches:\n- target: {kind: Kustomization, name: owner}\n  patch: '[{\"op\":\"replace\",\"path\":\"/spec/path\",\"value\":\"./app\"}]'\n")},
		"app/owner.yaml":         {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./other\n"))},
	}
	st := store.New()
	cacheDir := t.TempDir()
	trees, closeCache := testReceiptTrees(t, cacheDir)
	source := &store.SourceArtifact{LocalPath: root, FS: input}
	cfg := Config{Input: source, Path: root + "/app", Store: st, Trees: trees}
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	if !result.Binding.Producer.ReportHidden() {
		t.Fatalf("transformed child replaced the literal entry: %+v", result.Binding)
	}
	owner := testutil.KustomizationID("apps", "owner")
	record, ok := result.Index.Lookup(owner)
	declaration, typed := record.Declaration.(*manifest.Kustomization)
	if !ok || !typed || declaration.Path != "./app" || record.File != "app/kustomization.yaml" || record.Builder != record.File || record.Parent != result.Binding.Producer {
		t.Fatalf("transformed declaration lost its prepared ownership: %+v", record)
	}
	if st.GetObject(owner) != nil {
		t.Fatal("prepared child was published before ordinary runtime emission")
	}
	producer := st.GetObject(result.Binding.Producer).(*manifest.Kustomization)
	req := preparedRequest(t, cfg, producer, source)
	want := map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {
		Productions:  []manifest.Production{{Owner: producer.Named(), Source: result.Binding.Source}},
		ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"app"}},
	}}
	assert.Diff(t, packageHistory(t, trees, req, producer.Named()), want)
	closeCache()
	trees, closeCache = testReceiptTrees(t, cacheDir)
	assert.Diff(t, packageHistory(t, trees, req, producer.Named()), want)
	if _, ok := trees.PreviousRender(t.Context(), req, owner); ok {
		t.Fatal("transformed child replaced the accepted producer")
	}
	closeCache()
}

func TestPrepareEntry_BuildFailurePreservesRawErrors(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"build", "document"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := fstest.MapFS{
				"app/kustomization.yaml": {Data: []byte("resources: [selected.yaml]\npatches: [{path: missing.yaml}]\n")},
				"app/selected.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: selected}
spec:
  dependsOn: [{apiVersion: bad/group/version, kind: ConfigMap, name: dependency}]
`)},
			}
			if failure == "document" {
				input["app/kustomization.yaml"].Data = []byte("resources: [good.yaml, selected.yaml]\n")
				input["app/good.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n")}
			}
			for name, file := range input {
				testutil.WriteFile(t, root, name, string(file.Data))
			}
			cacheDir := t.TempDir()
			cacheOwner := cachetest.New(t, cacheDir, cache.New)
			trees := kustomize.NewTreeCache(cacheOwner, nil, task.NewBounded(2))
			closeCache := trees.Close
			t.Cleanup(closeCache)
			source := &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, LocalRoot: root, FS: os.DirFS(root)}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, source)
			d := discoverer{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
			d.loader.SourceRoot = root
			if _, err := d.loader.Load(t.Context(), d.cfg.Path); err != nil {
				t.Fatal(err)
			}
			rawError := d.loader.EntryFailure(t.Context(), root)
			if !errors.Is(rawError, manifest.ErrInput) {
				t.Fatalf("raw selected error = %v, want ErrInput", rawError)
			}
			owner := &manifest.Kustomization{Name: "entry", Path: "./app"}
			d.binding = Binding{Producer: owner.Named(), Source: manifest.BootstrapSourceID}
			if err := d.prepareEntry(t.Context(), owner, st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)); !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("invalid package build error = %v, want ErrFlux", err)
			}
			if got := d.loader.EntryFailure(t.Context(), root); got == nil || got.Error() != rawError.Error() {
				t.Fatal("failed entry build replaced its raw error")
			}
			_, _, emission := st.GetEmission(owner.Named())
			defer emission.Finish()
			if emission.Complete || len(emission.Manifests) != 0 {
				t.Fatalf("failed package build published documents: %v", emission.Manifests)
			}
			first := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "first"}
			if d.loader.BuiltDocuments(first) != nil || st.GetObject(first) != nil {
				t.Fatal("failed package published a partial batch")
			}
			closeCache()
			assert.NoError(t, cacheOwner.Close())
			trees, closeCache = testReceiptTrees(t, cacheDir)
			req := preparedRequest(t, d.cfg, owner, source)
			if _, ok := trees.PreviousRender(t.Context(), req, owner.Named()); ok {
				t.Fatal("failed package persisted a partial accepted batch")
			}
			testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [selected.yaml]\n")
			const selectedFile = "app/selected.yaml"
			testutil.WriteFile(t, root, selectedFile, strings.ReplaceAll(string(input[selectedFile].Data), "bad/group/version", "v1"))
			st = store.New()
			st.SetArtifact(manifest.BootstrapSourceID, source)
			d = discoverer{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
			d.loader.SourceRoot = root
			_, err := d.loader.Load(t.Context(), d.cfg.Path)
			assert.NoError(t, err)
			d.binding = Binding{Producer: owner.Named(), Source: manifest.BootstrapSourceID}
			assert.NoError(t, d.prepareEntry(t.Context(), owner, source))
			assert.NoError(t, d.acceptEntry(t.Context()))
			assert.Equal(t, len(packageHistory(t, trees, req, owner.Named())), 1)
			closeCache()
			trees, closeCache = testReceiptTrees(t, cacheDir)
			assert.Equal(t, len(packageHistory(t, trees, req, owner.Named())), 1)
			closeCache()

		})
	}
}

func TestPrepareEntry_PublicationFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: accepted}\n")
	cacheDir := t.TempDir()
	trees, closeCache := testReceiptTrees(t, cacheDir)
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, source)
	d := discoverer{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
	d.loader.SourceRoot = root
	_, err := d.loader.Load(t.Context(), d.cfg.Path)
	assert.NoError(t, err)
	owner := d.bindEntry(t.Context(), root)
	req := preparedRequest(t, d.cfg, owner, source)
	assert.NoError(t, os.RemoveAll(cacheDir))
	assert.NoError(t, d.prepareEntry(t.Context(), owner, source))
	err = d.acceptEntry(t.Context())
	if !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, fs.ErrNotExist) || !strings.Contains(err.Error(), `discovery entry "app" publish`) {
		t.Fatalf("publication failure lost classification, cause or entry context: %v", err)
	}
	if len(d.loader.BuiltDocuments(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "accepted"})) != 1 {
		t.Fatal("publication fault preceded successful rendering and registration")
	}
	closeCache()
	trees, closeCache = testReceiptTrees(t, cacheDir)
	if _, ok := trees.PreviousRender(t.Context(), req, owner.Named()); ok {
		t.Fatal("failed publication left durable accepted history")
	}
	closeCache()
}

func TestAdmitEntry_BuildFailurePreservesRawErrors(t *testing.T) {
	t.Parallel()
	const root = "/virtual/failed-entry"
	input := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [cm.yaml]\npatches: [{path: missing.yaml}]\n")},
		"cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n")},
	}
	st := store.New()
	result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Trees: testTrees(t)})
	assert.NoError(t, err)
	if st.GetObject(result.Binding.Producer) == nil {
		t.Fatal("failed preparation lost the ordinary producer")
	}
	_, _, emission := st.GetEmission(result.Binding.Producer)
	defer emission.Finish()
	if emission.Complete || len(emission.Manifests) != 0 {
		t.Fatal("failed preparation published a successful or partial emission")
	}
	record, ok := result.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "first"})
	if !ok || !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return p.Owner == result.Binding.Producer }) {
		t.Fatalf("failed root lost structural ancestry: %+v", record)
	}
}

func TestRun_BuiltPackages(t *testing.T) {
	t.Parallel()
	for _, selected := range []string{".", "a"} {
		t.Run(selected, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/prepared-root"
			input := fstest.MapFS{
				"a/kustomization.yaml": {Data: []byte("namespace: apps\nresources: [cm.yaml, driver.yaml, source.yaml]\nnameSuffix: -dev\npatches: [{path: patch.yaml}]\n")},
				"a/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {A: base}\n")},
				"a/patch.yaml":         {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {B: patched}\n")},
				"a/driver.yaml":        {Data: []byte(testutil.KustomizationYAML("driver", "apps", "  path: ./output\n  sourceRef: {kind: GitRepository, name: cluster-dev}\n"))},
				"a/source.yaml":        {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://fixture.invalid/self}\n")},
				"b/kustomization.yaml": {Data: []byte("configMapGenerator: [{name: generated, literals: [C=second]}]\n")},
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
					input["a/cm.yaml"].Data = []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {A: edited}\n")
				}
				dir := cacheDir
				if lifetime == "uncached" || lifetime == "changed-uncached" {
					dir = ""
				}
				trees, closeCache := testReceiptTrees(t, dir)
				st := store.New()
				source := &store.SourceArtifact{LocalPath: root, FS: input}
				st.SetArtifact(manifest.BootstrapSourceID, source)
				d := newDiscoverer(Config{Input: source, Path: filepath.Join(root, selected), Store: st, Trees: trees, SelfURLs: []string{"https://fixture.invalid/self"}})
				_, err := d.resolveEntrySources(t.Context(), false)
				assert.NoError(t, err)
				owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, selected)
				req := preparedRequest(t, d.cfg, owner, source)
				if lifetime == "changed" {
					if _, ok := trees.PreviousRender(t.Context(), req, owner.Named()); ok {
						t.Fatal("changed consumed resource retained stale accepted history")
					}
				}
				if strings.HasPrefix(lifetime, "warm-") {
					assert.Diff(t, packageHistory(t, trees, req, owner.Named()), previousHistory)
				}
				assert.NoError(t, d.loadManifests(t.Context(), root, true))
				assert.Equal(t, d.binding.Producer, owner.Named())
				id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings-dev"}
				batch := d.loader.BuiltDocuments(id)
				wantCount := 4
				if selected == "a" {
					wantCount = 3
				}
				assert.Equal(t, len(batch), wantCount)
				wantHistory := map[manifest.NamedResource]manifest.DiscoveryRecord{}
				var docs []map[string]any
				for _, child := range batch {
					docs = append(docs, child.Doc)
					record, ok := d.loader.Index.Lookup(child.Obj.Named())
					if !ok || record.Parent != d.binding.Producer || !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return p.Owner == d.binding.Producer }) {
						t.Fatalf("unowned prepared output: %+v", record)
					}
					accepted := manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: owner.Named(), Source: d.binding.Source}}}
					if child.Obj.Named() == testutil.KustomizationID("apps", "driver-dev") {
						source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "cluster-dev"}
						accepted.Refs = []manifest.NamedResource{source}
						accepted.ContentRoots = map[manifest.NamedResource][]string{source: {"output"}}
					}
					wantHistory[child.Obj.Named()] = accepted
					if child.Obj.Named() == id {
						cm := child.Obj.(*manifest.ConfigMap)
						assert.Diff(t, cm.Data, map[string]any{"A": wantValue, "B": "patched"})
					}
				}
				assert.NoError(t, d.loader.FinalizeIndex(t.Context(), root))
				assert.NoError(t, d.acceptEntry(t.Context()))
				if dir != "" {
					assert.Diff(t, packageHistory(t, trees, req, owner.Named()), wantHistory)
				}
				files := []string{"a/kustomization.yaml", "a/cm.yaml", "a/patch.yaml", "a/driver.yaml", "a/source.yaml"}
				if selected == "." {
					files = append(files, "b/kustomization.yaml")
				}
				for _, file := range files {
					if !slices.ContainsFunc(d.loader.Index.Readers[file], func(r manifest.FileReader) bool { return r.Reader == d.binding.Producer }) {
						t.Errorf("root missing actual reader for %s", file)
					}
				}
				if lifetime == "uncached" || lifetime == "changed" {
					previousDocs, previousIndex, previousWarnings = docs, d.loader.Index, st.Warnings()
				} else {
					assert.Diff(t, docs, previousDocs)
					if diff := cmp.Diff(previousIndex, d.loader.Index, cmp.Comparer(manifest.Equal)); diff != "" {
						t.Errorf("discovery index mismatch (-want +got):\n%s", diff)
					}
					assert.Diff(t, st.Warnings(), previousWarnings)
				}
				previousHistory = wantHistory
				closeCache()
			}
		})
	}
}

func TestRun_EmptyEntryHistory(t *testing.T) {
	t.Parallel()
	const root = "/virtual/empty-entry"
	input := fstest.MapFS{"kustomization.yaml": {Data: []byte("resources: []\n")}}
	cacheDir := t.TempDir()
	trees, closeCache := testReceiptTrees(t, cacheDir)
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: input}
	result, err := Run(t.Context(), Config{Input: source, Path: root, Store: st, Trees: trees})
	assert.NoError(t, err)
	owner := st.GetObject(result.Binding.Producer).(*manifest.Kustomization)
	req := preparedRequest(t, Config{}, owner, source)
	assert.Equal(t, len(packageHistory(t, trees, req, owner.Named())), 0)
	closeCache()
	trees, closeCache = testReceiptTrees(t, cacheDir)
	assert.Equal(t, len(packageHistory(t, trees, req, owner.Named())), 0)
	closeCache()
}
