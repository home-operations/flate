package discovery

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDiscardPinnedWorkingTreeFiles_PreservesSources(t *testing.T) {
	t.Parallel()
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
			artifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalRoot: root, LocalPath: t.TempDir(), Key: "pinned"}
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
				st.UpdateStatus(repo.Named(), store.StatusReady, "", "pinned")
			}
			d.discardPinnedWorkingTreeFiles(root)
			for _, repo := range []*manifest.GitRepository{repository, other} {
				id := repo.Named()
				status, ok := st.GetStatus(id)
				if st.GetObject(id) != repo || st.GetArtifact(id) != artifact || !ok || status.Status != store.StatusReady {
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
			if st.GetObject(repository.Named()) != repository || st.GetArtifact(repository.Named()) != artifact {
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
		st.SetArtifact(manifest.BootstrapSourceID, st.GetArtifact(source.Named()))
		d := discoverer{cfg: Config{Path: root, Store: st, Trees: testTrees(t)}, loader: loader.New(st)}
		if err := d.loadManifests(t.Context(), root); err != nil {
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
	for _, ownership := range []string{"unowned", "reachable", "preseeded", "alias", "foreign", "pinned", "fragment"} {
		t.Run(ownership, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/entry-admission"
			resources := "resources: [child.yaml, source.yaml, cm.yaml]\nnameSuffix: -dev\n"
			input := fstest.MapFS{
				"scope/app/kustomization.yaml": {Data: []byte(resources)},
				"scope/app/child.yaml":         {Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./scope/app/nested\n  suspend: true\n"))},
				"scope/app/source.yaml":        {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: raw-source, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n")},
				"scope/app/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: apps}\n")},
				"scope/app/excluded.yaml":      {Data: []byte(testutil.KustomizationYAML("excluded-driver", "apps", "  path: ./excluded\n") + "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: excluded-source, namespace: apps}\nspec: {url: https://must-not-run.invalid/repo}\n")},
			}
			input["scope/app/nested/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: apps\nresources: [source.yaml]\n")}
			input["scope/app/nested/source.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: nested-source}\nspec: {url: oci://fixture.invalid/chart}\n")}
			input["scope/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: outside\nresources: [app]")}
			st := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./scope/app"}
			if ownership == "reachable" {
				input["scope/app/kustomization.yaml"].Data = []byte("resources: [owner.yaml, child.yaml, source.yaml, cm.yaml]\nnameSuffix: -dev\n")
				input["scope/app/owner.yaml"] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(owner.Name, owner.Namespace, "  path: ./scope/app\n"))}
			} else if ownership != "unowned" {
				if ownership == "foreign" || ownership == "pinned" || ownership == "alias" {
					src := &manifest.GitRepository{Name: ownership, Namespace: owner.Namespace, URL: "https://fixture.invalid/" + ownership}
					st.AddObject(src)
					owner.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: src.Name}
					art := &store.SourceArtifact{LocalPath: "/virtual/" + ownership, FS: input}
					if ownership == "pinned" {
						art.LocalRoot, art.Key = root, "pinned"
					}
					if ownership == "alias" {
						art.LocalRoot, art.InputAlias = root, true
					}
					st.SetArtifact(src.Named(), art)
				}
				if ownership == "fragment" {
					owner.Path = ""
				}
				st.AddObject(owner)
			}
			result, err := Run(t.Context(), Config{Trees: testTrees(t), Input: &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, FS: input}, Path: root + "/scope/app", Store: st})
			if err != nil {
				t.Fatal(err)
			}
			bootstrap := testutil.KustomizationID("flux-system", "flate-bootstrap")
			bare := ownership != "reachable" && ownership != "preseeded" && ownership != "alias" && ownership != "pinned"
			if st.GetObject(bootstrap) != nil {
				t.Fatal("entry invented a Flux owner")
			}
			admitted := owner.Named()
			if bare {
				admitted = testutil.KustomizationID("apps", "child-dev")
				child, ok := st.GetObject(admitted).(*manifest.Kustomization)
				if !ok || st.EmissionOwner(admitted) != (manifest.NamedResource{}) {
					t.Fatalf("built child missing or owned: %+v", child)
				}
				sourceID := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "raw-source-dev"}
				if st.GetObject(sourceID) == nil {
					t.Fatal("built source was not promoted")
				}
				if len(result.Documents) != 3 {
					t.Fatalf("built batch = %v", result.Documents)
				}
			} else if len(result.Documents) != 0 {
				t.Fatalf("authored owner acquired bare output: %v", result.Documents)
			}

			for id, record := range result.Index.Files() {
				if strings.HasPrefix(id.Name, "excluded-") {
					t.Fatalf("excluded sibling indexed: %s", id)
				}
				if id.Name == "raw-source" || id.Name == "child" || id.Name == "nested-source" {
					if st.GetObject(id) != nil {
						t.Fatalf("render-owned authored identity is runnable: %s", id)
					}
					for _, production := range record.Productions {
						if production.Owner == admitted && production.Path != "" {
							t.Fatalf("render-owned driver has a raw fallback: %+v", production)
						}
					}
				}
			}
			builder := "scope/app/kustomization.yaml"
			if ownership == "pinned" {
				builder = "../pinned/" + builder
			}
			if !slices.Contains(result.Index.Readers[builder], admitted) {
				t.Fatalf("builder did not reach its source-qualified owner: %+v", result.Index.Readers)
			}
		})
	}
}

func TestRun_BootstrapNameCollision(t *testing.T) {
	t.Parallel()
	for _, origin := range []string{"store", "index"} {
		t.Run(origin, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := fstest.MapFS{"app/kustomization.yaml": {Data: []byte("resources: []\n")}}
			reserved := &manifest.Kustomization{Name: "flate-bootstrap", Namespace: "flux-system"}
			st := store.New()
			if origin == "store" {
				st.AddObject(reserved)
			} else {
				input["app/kustomization.yaml"].Data = []byte("resources: [reserved.yaml]\n")
				input["app/reserved.yaml"] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(reserved.Name, reserved.Namespace, "  {}\n"))}
			}
			result, err := Run(t.Context(), Config{Trees: testTrees(t), Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: filepath.Join(root, "app"), Store: st})
			if err != nil {
				t.Fatal(err)
			}
			if origin == "store" && st.GetObject(reserved.Named()) != reserved {
				t.Fatal("bootstrap overwrote reserved store identity")
			}
			if origin == "index" {
				record, ok := result.Index.Lookup(reserved.Named())
				if !ok || record.Path != "" || record.Authored != (manifest.NamedResource{}) || st.GetObject(reserved.Named()) == nil {
					t.Fatalf("authored identity lost or acquired raw fallback: %+v", record)
				}
			}
			if got := st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization); len(got) != 1 {
				t.Fatalf("entry added an invented owner: %v", got)
			}

		})
	}
}

func testTrees(t *testing.T) *kustomize.TreeCache {
	t.Helper()
	trees := kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2))
	t.Cleanup(trees.Close)
	return trees
}

func TestRun_BuiltEntryFixedPoint(t *testing.T) {
	t.Parallel()
	const root = "/virtual/built-fixed-point"
	input := fstest.MapFS{
		"app/kustomization.yaml":  {Data: []byte("resources: [child.yaml]\nnameSuffix: -dev\nconfigMapGenerator: [{name: generated, literals: [value=selected]}]\n")},
		"app/child.yaml":          {Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"))},
		"leaf/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"leaf/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n")},
	}
	st := store.New()
	result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root + "/app", Store: st, Trees: testTrees(t)})
	if err != nil {
		t.Fatal(err)
	}
	child := testutil.KustomizationID("apps", "child-dev")
	if st.GetObject(child) == nil || st.GetObject(testutil.KustomizationID("apps", "child")) != nil {
		t.Fatal("transformed child did not replace its raw identity")
	}
	leaf := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "leaf"}
	record, ok := result.Index.Lookup(leaf)
	if !ok || !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return p.Owner == child }) {
		t.Fatalf("built child did not expand its leaf: %+v", record)
	}
	if len(result.Documents) != 2 {
		t.Fatalf("bare batch lost a generated document: %v", result.Documents)
	}
	for _, doc := range result.Documents {
		id := manifest.DocIdentity(doc)
		record, ok := result.Index.Lookup(id)
		if !ok || record.Path != "" || record.Authored != (manifest.NamedResource{}) || st.EmissionOwner(id) != (manifest.NamedResource{}) {
			t.Fatalf("built document gained raw materialization or ownership: %s %+v", id, record)
		}
		if id.Kind == manifest.KindConfigMap && (!strings.HasPrefix(id.Name, "generated-dev-") || len(id.Name) <= len("generated-dev-")) {
			t.Fatalf("generated identity lost its hash: %s", id)
		}
	}
}

func TestRun_BuiltEntrySecretPrecedence(t *testing.T) {
	t.Parallel()
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/built-secrets"
			input := fstest.MapFS{
				"kustomization.yaml": {Data: []byte("resources: [secret.yaml]\n")},
				"secret.yaml":        {Data: []byte("apiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: apps, labels: {selected: 'true'}}\nstringData: {token: 'ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]'}\n")},
			}
			st := store.New()
			value := manifest.ValuePlaceholder("token")
			id := manifest.NamedResource{Kind: manifest.KindSecret, Name: "credentials", Namespace: "apps"}
			var saved *manifest.Secret
			if existing {
				value = "real-value"
				saved = &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"token": value}}
				st.AddObject(saved)
			}
			result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Trees: testTrees(t), WipeSecrets: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Documents) != 1 {
				t.Fatalf("selected secret missing: %v", result.Documents)
			}
			doc := result.Documents[0]
			if doc["stringData"].(map[string]any)["token"] != value {
				t.Fatalf("output lost secret policy or precedence: %v", doc)
			}
			if obj := st.GetObject(id).(*manifest.Secret); obj.StringData["token"] != value || existing && obj != saved {
				t.Fatalf("store lost secret precedence: %+v", obj)
			}
			labels := doc["metadata"].(map[string]any)["labels"].(map[string]any)
			if labels["selected"] != "true" {
				t.Fatalf("sanitization lost built metadata: %v", doc)
			}
		})
	}
}

func TestRun_BuiltEntryRejectedDocument(t *testing.T) {
	t.Parallel()
	root := discoveryRefFixture(t)
	testutil.WriteFile(t, root, "app/kustomization.yaml", `resources: [child.yaml, cm.yaml]
patches:
- target: {kind: Kustomization, name: child}
  patch: '[{"op":"replace","path":"/spec/path","value":"./app"}]'
`)
	testutil.WriteFile(t, root, "app/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"))
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dirty, namespace: apps}\n")
	st := store.New()
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system", URL: "git://fixture.invalid/cluster", Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}}
	st.AddObject(source)
	result, err := Run(t.Context(), Config{Path: filepath.Join(root, "app"), RepoRoot: root, Store: st, Trees: testTrees(t), SelfURLs: []string{source.URL}})
	if err != nil {
		t.Fatal(err)
	}
	rejected := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "dirty"}
	if st.GetObject(rejected) != nil {
		t.Fatal("working-tree document survived the built child's pinned claim")
	}
	if _, ok := result.Index.Lookup(rejected); ok {
		t.Fatal("rejected document retained discovery evidence")
	}
	child := testutil.KustomizationID("apps", "child")
	if st.GetObject(child) == nil || len(result.Documents) != 1 || manifest.DocIdentity(result.Documents[0]) != child {
		t.Fatalf("built output leaked rejected documents or lost its driver: %v", result.Documents)
	}
}

func TestAdmitEntry_BuildFailurePreservesRawErrors(t *testing.T) {
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
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, LocalRoot: root, FS: input})
	d := discoverer{cfg: Config{Path: filepath.Join(root, "app"), Store: st, Trees: testTrees(t)}, loader: loader.New(st)}
	d.loader.SourceRoot = root
	if _, err := d.loader.Load(t.Context(), d.cfg.Path); err != nil {
		t.Fatal(err)
	}
	const selectedFile = "app/selected.yaml"
	rawError := d.loader.Index.Errors[selectedFile]
	if !errors.Is(rawError, manifest.ErrInput) {
		t.Fatalf("raw selected error = %v, want ErrInput", rawError)
	}
	if err := d.admitEntry(t.Context(), root); err == nil {
		t.Fatal("entry with a missing patch built successfully")
	}
	if !errors.Is(d.loader.Index.Errors[selectedFile], rawError) {
		t.Fatal("failed entry build replaced its raw error")
	}
	if len(d.built) != 0 || len(d.documents) != 0 {
		t.Fatalf("failed entry build published documents: built=%v documents=%v", d.built, d.documents)
	}
}
