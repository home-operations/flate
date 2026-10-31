package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
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
		d := Session{cfg: Config{Store: st}, loader: loader.New(st)}
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
			d := Session{
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
		d := Session{cfg: Config{Path: root, Store: st, Input: st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact), Trees: testTrees(t)}, loader: loader.New(st)}
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
				d := Session{cfg: Config{Store: st}, loader: loader.New(st)}
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
			selected, err := d.bindEntry(t.Context(), root)
			assert.NoError(t, err)
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
				assert.Equal(t, bindEntryForTest(t, d, root), selected)
				assert.Equal(t, d.prepared[owner].receipt, previous)
				independent := *ready
				independent.DependsOn = nil
				d.loader.Index.Records[ready.Named()] = manifest.DiscoveryRecord{Declaration: &independent}
				assert.Equal(t, bindEntryForTest(t, d, root), owner)
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
				if !bindEntryForTest(t, d, root).Named().ReportHidden() {
					t.Fatal("new owner/snapshot retained the independent decision")
				}
				d.substitutions = vars("ready")
				assert.Equal(t, bindEntryForTest(t, d, root), &projected)
				before := d.prepared[&projected]
				replacement := *before.source
				replacement.Key = "new-source"
				replacement.Content = "new-content"
				st.SetArtifact(manifest.BootstrapSourceID, &replacement)
				assert.Equal(t, bindEntryForTest(t, d, root), &projected)
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
		Application:  `{"apiVersion":"kustomize.toolkit.fluxcd.io/v1","kind":"Kustomization","metadata":{"name":"owner","namespace":"apps"},"spec":{"path":"./app"}}`,
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
			d := Session{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
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
			if d.loader.PromotionDocuments(first) != nil || st.GetObject(first) != nil {
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
			d = Session{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
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
	d := Session{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: trees}, loader: loader.New(st)}
	d.loader.SourceRoot = root
	_, err := d.loader.Load(t.Context(), d.cfg.Path)
	assert.NoError(t, err)
	owner, err := d.bindEntry(t.Context(), root)
	assert.NoError(t, err)
	req := preparedRequest(t, d.cfg, owner, source)
	assert.NoError(t, os.RemoveAll(cacheDir))
	assert.NoError(t, d.prepareEntry(t.Context(), owner, source))
	err = d.acceptEntry(t.Context())
	if !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, fs.ErrNotExist) || !strings.Contains(err.Error(), `discovery entry "app" publish`) {
		t.Fatalf("publication failure lost classification, cause or entry context: %v", err)
	}
	if len(d.loader.PromotionDocuments(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "accepted"})) != 1 {
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

func TestRun_UnappliedPackage(t *testing.T) {
	t.Parallel()
	const root = "/virtual/unapplied-package"
	input := fstest.MapFS{
		"entry/source.yaml":                  {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: https://fixture.invalid/self}\n")},
		"entry/generator.yaml":               {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec:\n  chart:\n    spec:\n      chart: charts/bootstrap\n      sourceRef: {kind: GitRepository, name: self}\n")},
		"entry/unapplied/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\npatches: [{path: missing.yaml}]\n")},
		"entry/unapplied/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unused}\n")},
	}
	st := store.New()
	result, err := Run(t.Context(), Config{
		Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: filepath.Join(root, "entry"),
		Store: st, Trees: testTrees(t), SelfURLs: []string{"https://fixture.invalid/self"},
	})
	assert.NoError(t, err)
	if result.Binding != (Binding{}) {
		t.Fatal("unrendered application qualified the entry")
	}
	if st.GetObject(testutil.HelmReleaseID("apps", "generator")) == nil {
		t.Fatal("unapplied package hid the application producer")
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
			if selected == "." {
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
				owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, selected, manifest.NamedResource{})
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
				batch := d.loader.PromotionDocuments(id)
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
						accepted.Application = `{"apiVersion":"kustomize.toolkit.fluxcd.io/v1","kind":"Kustomization","metadata":{"name":"driver-dev","namespace":"apps"},"spec":{"path":"./output","sourceRef":{"kind":"GitRepository","name":"cluster-dev"}}}`
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
				assert.NoError(t, d.loader.FinalizeIndex(t.Context(), root, true))
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

func TestRun_DiscoveryOnlyEntry(t *testing.T) {
	t.Parallel()
	const root = "/virtual/discovery-only"
	input := fstest.MapFS{
		"Taskfile.yaml":        {Data: []byte("version: '3'\ntasks: {build: {cmds: [true]}}\n")},
		"clusters/a/sync.yaml": {Data: []byte(testutil.KustomizationYAML("a", "apps", "  path: ./clusters/a\n"))},
		"clusters/b/sync.yaml": {Data: []byte(testutil.KustomizationYAML("b", "apps", "  path: ./clusters/b\n"))},
	}
	st := store.New()
	observed := &entryGeneratorFS{FS: input}
	result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: observed}, Path: root, Store: st, Trees: testTrees(t)})
	assert.NoError(t, err)
	assert.Equal(t, result.Binding, Binding{})
	assert.Equal(t, observed.calls.Load(), int64(0))
	for _, name := range []string{"a", "b"} {
		if st.GetObject(testutil.KustomizationID("apps", name)) == nil {
			t.Fatalf("cluster %s was not discovered", name)
		}
	}
	for _, obj := range st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization) {
		if obj.Named().ReportHidden() {
			t.Fatalf("discovery-only entry published %s", obj.Named())
		}
	}
}

func bindEntryForTest(t *testing.T, d *Session, root string) *manifest.Kustomization {
	t.Helper()
	owner, err := d.bindEntry(t.Context(), root)
	assert.NoError(t, err)
	return owner
}

func TestRun_EntryApplication(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, owner, kind, url, path                     string
		instance, admitted, pinned, qualified, dependent bool
	}{
		{name: "gotk-sync", owner: "gotk-sync", qualified: true},
		{name: "arbitrary applier", owner: "applies-here", qualified: true},
		{name: "different path", owner: "elsewhere", path: "./elsewhere"},
		{name: "escaping path", owner: "escape", path: "../elsewhere"},
		{name: "dependent applier", owner: "applies-here", dependent: true, qualified: true},
		{name: "wrong source", owner: "foreign", kind: manifest.KindGitRepository, url: "https://fixture.invalid/foreign"},
		{name: "pinned source", owner: "pinned", kind: manifest.KindGitRepository, url: "https://fixture.invalid/self", admitted: true, pinned: true},
		{name: "admitted oci", owner: "oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/self", admitted: true, qualified: true},
		{name: "unadmitted oci", owner: "oci", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/self"},
		{name: "admitted bucket", owner: "bucket", kind: manifest.KindBucket, url: "s3.fixture.invalid", admitted: true, qualified: true},
		{name: "unadmitted bucket", owner: "bucket", kind: manifest.KindBucket, url: "s3.fixture.invalid"},
		{name: "instance git", owner: "flux", kind: manifest.KindGitRepository, url: "https://fixture.invalid/self", instance: true, admitted: true, qualified: true},
		{name: "instance foreign git", owner: "flux", kind: manifest.KindGitRepository, url: "https://fixture.invalid/foreign", instance: true},
		{name: "instance oci", owner: "flux", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/self", instance: true, admitted: true, qualified: true},
		{name: "instance unadmitted oci", owner: "flux", kind: manifest.KindOCIRepository, url: "oci://fixture.invalid/self", instance: true},
		{name: "instance bucket", owner: "flux", kind: manifest.KindBucket, url: "s3.fixture.invalid", instance: true, admitted: true, qualified: true},
		{name: "instance unadmitted bucket", owner: "flux", kind: manifest.KindBucket, url: "s3.fixture.invalid", instance: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/entry-application"
			input := fstest.MapFS{"object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal}\n")}}
			st := store.New()
			source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Name: "self", Namespace: "apps"}
			declaration := ""
			if tc.instance {
				input["owner.yaml"] = &fstest.MapFile{Data: []byte(fmt.Sprintf("apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: apps}\nspec:\n  sync: {name: self, kind: %s, url: '%s', ref: main, path: ./}\n", tc.kind, tc.url))}
			} else {
				extra := ""
				if tc.dependent {
					extra = "  dependsOn: [{name: sibling}]\n"
					input["sibling.yaml"] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML("sibling", "apps", "  path: ./leaf\n"))}
					input["leaf/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: []\n")}
				}
				if tc.kind != "" {
					extra = "  sourceRef: {kind: " + tc.kind + ", name: self}\n"
					spec := "url: '" + tc.url + "'"
					if tc.kind == manifest.KindBucket {
						spec = "endpoint: '" + tc.url + "', bucketName: cluster"
					}
					if tc.pinned {
						spec += ", ref: {tag: pinned}"
					}
					declaration = fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: %s\nmetadata: {name: self, namespace: apps}\nspec: {%s}\n", tc.kind, spec)
					input["source.yaml"] = &fstest.MapFile{Data: []byte(declaration)}
				}
				path := tc.path
				if path == "" {
					path = "./"
				}
				input["owner.yaml"] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(tc.owner, "apps", "  path: "+path+"\n"+extra))}
			}
			if tc.admitted {
				artifact := &store.SourceArtifact{LocalPath: "/virtual/application-alias", LocalRoot: root, InputAlias: true, FS: input}
				if tc.pinned {
					artifact.InputAlias, artifact.Content, artifact.Key = false, "committed", "committed"
				}
				st.SetArtifact(source, artifact)
			}
			d := newDiscoverer(Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Trees: testTrees(t), SelfURLs: []string{"https://fixture.invalid/self"}})
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.NoError(t, err)
			assert.NoError(t, d.loadManifests(t.Context(), root, true))
			assert.Equal(t, d.binding != (Binding{}), tc.qualified)
			if tc.qualified {
				assert.Equal(t, d.entryApplier.Name, tc.owner)
				if tc.dependent {
					assert.Equal(t, d.binding.Producer.ReportHidden(), true)
				}
				if tc.instance {
					assert.Equal(t, d.entryApplier.Kind, "FluxInstance")
				}
			} else {
				assert.Equal(t, len(d.prepared), 0)
			}
			if tc.instance {
				assert.Equal(t, st.GetObject(source), manifest.BaseManifest(nil))
				instance := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "apps", Name: "flux"}
				assert.Equal(t, st.GetObject(instance), manifest.BaseManifest(nil))
				_, indexed := d.loader.Index.Lookup(instance)
				assert.Equal(t, indexed, true)
			}
		})
	}
}

func TestBindEntry_EvidenceAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		publication  bool
		livePath     string
		history      bool
		historyFirst bool
		dependent    bool
		substitution bool
		invalid      string
		qualified    bool
		reuse        string
	}{
		{name: "retired_publication", publication: true, livePath: "./elsewhere"},
		{name: "withdrawn_publication", publication: true},
		{name: "effective_publication", publication: true, livePath: "./entry", qualified: true, reuse: "current"},
		{name: "same_content_replacement", publication: true, livePath: "./entry", qualified: true, reuse: "current"},
		{name: "receipt_only", history: true, qualified: true, reuse: "history"},
		{name: "receipt_unrelated_live", history: true, livePath: "./elsewhere", qualified: true},
		{name: "current_then_receipt", history: true, livePath: "./entry", qualified: true, reuse: "current"},
		{name: "receipt_then_current", history: true, historyFirst: true, livePath: "./entry", qualified: true, reuse: "current"},
		{name: "dependent_receipt", history: true, dependent: true, qualified: true},
		{name: "substituted_receipt", history: true, substitution: true, qualified: true},
		{name: "malformed_history", history: true, invalid: "malformed"},
		{name: "wrong_id_history", history: true, invalid: "wrong_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const repo = "/virtual/evidence-authority"
			input := &store.SourceArtifact{LocalPath: repo, FS: fstest.MapFS{
				"entry/literal.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal}\n")},
			}}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			a := &Admission{input: input, path: filepath.Join(repo, "entry")}
			current := &manifest.Kustomization{Name: "applier", Namespace: "apps", Path: tc.livePath, TargetNamespace: "current"}
			if tc.publication {
				previous := current.Clone()
				previous.Path = "./entry"
				st.AddObject(previous)
				a.AddApplication(st, previous)
				if tc.name == "same_content_replacement" || tc.livePath == "" {
					st.DeleteObject(previous.Named())
				}
			}
			spec := map[string]any{"path": "./entry", "targetNamespace": "history"}
			if tc.dependent {
				spec["dependsOn"] = []any{map[string]any{"name": "sibling"}}
			}
			if tc.substitution {
				spec["postBuild"] = map[string]any{"substituteFrom": []any{map[string]any{"kind": "ConfigMap", "name": "settings"}}}
			}
			document := map[string]any{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization",
				"metadata": map[string]any{"name": current.Name, "namespace": current.Namespace}, "spec": spec}
			data, err := json.Marshal(document)
			assert.NoError(t, err)
			record := manifest.DiscoveryRecord{Application: string(data)}
			id := current.Named()
			switch tc.invalid {
			case "malformed":
				record.Application = "{"
			case "wrong_id":
				id.Name = "different"
			}
			addHistory := func() {
				if tc.history {
					a.AddHistory(st, id, record)
				}
			}
			if tc.historyFirst {
				addHistory()
			}
			if tc.livePath != "" {
				st.AddObject(current)
				current = st.GetObject(current.Named()).(*manifest.Kustomization)
				a.AddApplication(st, current)
			}
			if !tc.historyFirst {
				addHistory()
			}
			root, binding, err := a.BindEntry(t.Context(), st)
			assert.NoError(t, err)
			if !tc.qualified {
				assert.Equal(t, root, (*manifest.Kustomization)(nil))
				assert.Equal(t, binding, Binding{})
				return
			}
			if root == nil {
				t.Fatal("qualifying evidence lost its entry")
			}
			key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", current.Named()).Named()
			assert.Equal(t, binding.Key, key)
			assert.Equal(t, binding.Path, "entry")
			assert.Equal(t, root.Named().ReportHidden(), tc.reuse == "")
			switch tc.reuse {
			case "current":
				assert.Equal(t, root, current)
			case "history":
				assert.Equal(t, root.TargetNamespace, "history")
			}
			assert.Equal(t, a.entryBound.Load(), true)
			assert.Equal(t, a.applications.Load(), (*applicationEvidence)(nil))
		})
	}
}

func TestBindEntry_ReemittedPublication(t *testing.T) {
	t.Parallel()
	const repo = "/virtual/reemitted-publication"
	input := &store.SourceArtifact{LocalPath: repo, FS: fstest.MapFS{}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "./generator"}
	st.AddObject(producer)
	a := &Admission{input: input, path: filepath.Join(repo, "entry")}
	applier := &manifest.Kustomization{Name: "applier", Namespace: "apps", Path: "./entry"}
	sibling := &manifest.ConfigMap{Name: "sibling", Namespace: "apps", Data: map[string]any{"value": "old"}}
	var publications int
	unsubscribe := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if obj, ok := payload.(manifest.BaseManifest); ok {
			a.AddApplication(st, obj)
		}
		if id == applier.Named() {
			publications++
		}
	}, false)
	defer unsubscribe()
	st.RecordEmitted(producer.Named(), &store.Emission{
		Children: []store.Child{{Obj: applier}, {Obj: sibling}}, Complete: true,
	}, nil)
	replacement := applier.Clone()
	st.RecordEmitted(producer.Named(), &store.Emission{
		Children: []store.Child{{Obj: replacement}, {Obj: &manifest.ConfigMap{
			Name: sibling.Name, Namespace: sibling.Namespace, Data: map[string]any{"value": "new"},
		}}}, Complete: true,
	}, nil)
	assert.Equal(t, publications, 1)
	assert.Equal(t, st.GetObject(applier.Named()), manifest.BaseManifest(replacement))
	appliers, err := a.qualifyEntry(t.Context(), st, nil)
	assert.NoError(t, err)
	if len(appliers) != 1 {
		t.Fatalf("qualifying publications: got %d, want 1", len(appliers))
	}
	assert.Equal(t, appliers[0], manifest.BaseManifest(applier))
	root, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, root, replacement)
	assert.Equal(t, binding.Producer, replacement.Named())
}

func TestRun_EntryPackage(t *testing.T) {
	t.Parallel()
	for _, filename := range manifest.KustomizeBuilderFilenames {
		t.Run(filename, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/entry-package"
			input := fstest.MapFS{filename: {Data: []byte("resources: [cm.yaml]\n")}, "cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal}\n")}}
			st := store.New()
			observed := &entryGeneratorFS{FS: input}
			result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: observed}, Path: root, Store: st, Trees: testTrees(t)})
			assert.NoError(t, err)
			assert.Equal(t, result.Binding.Producer.ReportHidden(), true)
			if observed.calls.Load() == 0 {
				t.Fatal("positive control did not observe the root generator")
			}
			record, ok := result.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "literal"})
			assert.Equal(t, ok, true)
			assert.Equal(t, record.Parent, result.Binding.Producer)
		})
	}
}

func TestBindEntry_PackageReadFailure(t *testing.T) {
	t.Parallel()
	const root = "/virtual/unreadable-entry"
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: unreadableEntryFS{fstest.MapFS{}}})
	d := newDiscoverer(Config{Path: root, Store: st})
	_, err := d.bindEntry(t.Context(), root)
	if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("entry package failure lost its classification or cause: %v", err)
	}
	if strings.Contains(err.Error(), "input error") || strings.Contains(err.Error(), "flux error") {
		t.Fatalf("entry package diagnostic contains classification prose: %v", err)
	}
	assert.Equal(t, d.binding, Binding{})
}

type unreadableEntryFS struct{ fs.FS }

func (f unreadableEntryFS) Open(name string) (fs.File, error) {
	if name == "kustomization.yaml" {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	return f.FS.Open(name)
}

type entryGeneratorFS struct {
	fs.FS
	calls atomic.Int64
}

func (f *entryGeneratorFS) Open(name string) (fs.File, error) {
	f.observe()
	return f.FS.Open(name)
}

func (f *entryGeneratorFS) Stat(name string) (fs.FileInfo, error) {
	f.observe()
	return fs.Stat(f.FS, name)
}

func (f *entryGeneratorFS) ReadDir(name string) ([]fs.DirEntry, error) {
	f.observe()
	return fs.ReadDir(f.FS, name)
}

func (f *entryGeneratorFS) observe() {
	var stack [64]uintptr
	frames := runtime.CallersFrames(stack[:runtime.Callers(2, stack[:])])
	for {
		frame, more := frames.Next()
		if frame.Function == "github.com/home-operations/flate/pkg/kustomize.generateManifest" {
			f.calls.Add(1)
			return
		}
		if !more {
			return
		}
	}
}

func TestAdvance_PlaceholderInputReachesFixpoint(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "namespace: apps\nresources: [consumer.yaml, source.yaml]\n")
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "", "  path: ./app\n  sourceRef: {kind: GitRepository, name: cluster}\n  postBuild:\n    substituteFrom: [{kind: Secret, name: settings}]\n"))
	testutil.WriteFile(t, root, "flux/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster}\nspec: {url: 'git://fixture.invalid/repo'}\n")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [settings.yaml]\n")
	testutil.WriteFile(t, root, "app/settings.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: settings}\nstringData: {KEY: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n")
	st := store.New()
	l := loader.New(st)
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"flux/consumer.yaml"})
	session, err := Begin(t.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"git://fixture.invalid/repo"}, Store: st, Loader: l, WipeSecrets: true, Trees: kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2)), Selection: manifest.Selection{Changed: true}})
	assert.NoError(t, err)
	for range 3 {
		proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		advanced, err := session.Advance(t.Context(), proposal)
		assert.NoError(t, err)
		if !advanced {
			return
		}
	}
	t.Fatal("construction repeatedly readmitted the placeholder input without reaching a fixpoint")
}

type constructionFS struct {
	fs.FS
	statErr error
}

func (f *constructionFS) Stat(name string) (fs.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	return fs.Stat(f.FS, name)
}

func TestAdvance_LeafAdmissionPreservesConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &constructionFS{FS: fstest.MapFS{"flux/owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./app\n"))}, "app/kustomization.yaml": {Data: []byte("resources: []\n")}}}
	st := store.New()
	l := loader.New(st)
	l.Options.Filter = change.NewFilter()
	session, err := Begin(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: filepath.Join(root, "flux"), Store: st, Loader: l, Selection: manifest.Selection{Changed: true}})
	assert.NoError(t, err)
	input.statErr = fs.ErrPermission
	leaf := &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"VALUE": "stable"}}
	advanced, err := session.Advance(t.Context(), &loader.Admission{Objects: []store.Child{{Obj: leaf}}})
	assert.NoError(t, err)
	assert.Equal(t, advanced, true)
	assert.Equal(t, st.GetObject(leaf.Named()), manifest.BaseManifest(leaf))
}
