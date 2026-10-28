package discovery

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

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
		st.SetArtifact(manifest.BootstrapSourceID, st.GetArtifact(source.Named(), store.ArtifactContent))
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
	for _, ownership := range []string{"unowned", "nested", "reader", "reachable", "preseeded", "ancestor", "alias", "foreign", "pinned", "fragment"} {
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
			if ownership == "unowned" {
				for _, file := range []string{"scope/app/child.yaml", "scope/app/source.yaml", "scope/app/cm.yaml"} {
					input[file].Data = []byte("apiVersion: v1\nkind: List\nitems:\n- " + strings.ReplaceAll(strings.TrimSpace(string(input[file].Data)), "\n", "\n  ") + "\n")
				}
			}
			input["scope/app/nested/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: apps\nresources: [source.yaml]\n")}
			input["scope/app/nested/source.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: nested-source}\nspec: {url: oci://fixture.invalid/chart}\n")}
			input["scope/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: outside\nresources: [app]")}
			if ownership == "nested" || ownership == "reader" {
				input["scope/app/kustomization.yaml"].Data = []byte("resources: [child.yaml, source.yaml, cm.yaml, nested]\nnameSuffix: -dev\n")
				if ownership == "reader" {
					input["scope/app/nested/kustomization.yaml"].Data = append(input["scope/app/nested/kustomization.yaml"].Data,
						[]byte("configMapGenerator: [{name: read, files: [../kustomization.yaml]}]\n")...)
				}
			}
			st := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./scope/app"}
			if ownership == "reachable" {
				input["scope/app/kustomization.yaml"].Data = []byte("resources: [owner.yaml, child.yaml, source.yaml, cm.yaml]\nnameSuffix: -dev\n")
				input["scope/app/owner.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: flux-system, uid: selected-owner, annotations: {fixture: retained}}\nspec: {path: ./scope/app, interval: 1m}\n")}
			} else if ownership != "unowned" && ownership != "nested" && ownership != "reader" {
				if ownership == "foreign" || ownership == "pinned" || ownership == "alias" {
					src := &manifest.GitRepository{Name: ownership, Namespace: owner.Namespace, URL: "https://fixture.invalid/" + ownership}
					st.AddObject(src)
					owner.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: src.Name}
					art := &store.SourceArtifact{LocalPath: "/virtual/" + ownership, FS: input}
					if ownership == "pinned" {
						art.LocalRoot, art.Key, art.Content = root, "pinned", "pinned"
					}
					if ownership == "alias" {
						art.LocalRoot, art.InputAlias = root, true
					}
					st.SetArtifact(src.Named(), art)
				}
				switch ownership {
				case "fragment":
					owner.Path = ""
				case "ancestor":
					owner.Path = "./scope"
				}
				st.AddObject(owner)
			}
			result, err := Run(t.Context(), Config{Trees: testTrees(t), Input: &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, FS: input}, Path: root + "/scope/app", Store: st})
			if err != nil {
				t.Fatal(err)
			}
			bootstrap := testutil.KustomizationID("flux-system", "flate-bootstrap")
			bare := ownership != "reachable" && ownership != "preseeded" && ownership != "ancestor" && ownership != "alias" && ownership != "pinned"
			if st.GetObject(bootstrap) != nil {
				t.Fatal("entry invented a Flux owner")
			}
			admitted := owner.Named()
			if ownership == "reachable" {
				_, doc, emission := st.GetEmission(admitted)
				emission.Finish()
				if doc == nil || doc["metadata"].(map[string]any)["uid"] != "selected-owner" || doc["metadata"].(map[string]any)["annotations"].(map[string]any)["fixture"] != "retained" {
					t.Fatalf("selected owner lost authoritative metadata: %v", doc)
				}
			}
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
				want := 3
				switch ownership {
				case "nested":
					want = 4
				case "reader":
					want = 5
				}
				if len(result.Documents) != want {
					t.Fatalf("built batch = %v, want %d documents", result.Documents, want)
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
			if bare {
				record, _ := result.Index.Lookup(admitted)
				if !slices.Contains(record.Inputs, builder) || slices.ContainsFunc(result.Index.Readers[builder], func(r manifest.FileReader) bool { return r.Reader == admitted }) != (ownership == "reader") {
					t.Fatalf("built seed lost declaration input or acquired render ownership: %+v", record)
				}
				if ownership == "reader" {
					for prefix, owners := range result.Index.Claims {
						if strings.HasPrefix("scope/app/", prefix) && slices.Contains(owners, admitted) {
							t.Fatalf("nested reader acquired a covering claim: %s", prefix)
						}
					}
				}
			} else if !slices.ContainsFunc(result.Index.Readers[builder], func(r manifest.FileReader) bool { return r.Reader == admitted }) {
				t.Fatalf("builder did not reach its source-qualified owner: %+v", result.Index.Readers)
			}
		})
	}
	t.Run("plain bootstrap collision preserves native payload", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		for _, dir := range []string{"bootstrap", "flux"} {
			resources := "resources: [namespace.yaml, objects.yaml]\nnamespace: external-secrets\n"
			testutil.WriteFile(t, root, dir+"/kustomization.yaml", resources)
			testutil.WriteFile(t, root, dir+"/namespace.yaml", "apiVersion: v1\nkind: Namespace\nmetadata: {name: _}\n")
		}
		testutil.WriteFile(t, root, "bootstrap/objects.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: onepassword-secret}\nstringData: {fixture: value}\n")
		testutil.WriteFile(t, root, "flux/objects.yaml", testutil.KustomizationYAML("owner", "external-secrets", "  path: ./payload\n"))
		testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: []\n")
		before, err := Run(t.Context(), Config{Path: root, Store: store.New(), Trees: testTrees(t)})
		if err != nil {
			t.Fatal(err)
		}
		entries, err := Plan(t.Context(), Config{Path: root})
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].ID != "." || entries[0].Path != root {
			t.Fatalf("plain bootstrap package established a cluster: %+v", entries)
		}
		after, err := Run(t.Context(), Config{Input: entries[0].Input, Path: entries[0].Path, Store: store.New(), Trees: testTrees(t)})
		if err != nil {
			t.Fatal(err)
		}
		want, err := json.Marshal(before.Documents)
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(after.Documents)
		if err != nil {
			t.Fatal(err)
		}
		if len(before.Documents) == 0 || !bytes.Equal(got, want) {
			t.Fatalf("singleton discovery payload changed: want %s; got %s", want, got)
		}
	})

	t.Run("independent data collision survives a native context", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "entry/owner.yaml", testutil.KustomizationYAML("owner", "flux-system", "  path: ./payload\n"))
		for _, dir := range []string{"payload", "independent-data"} {
			testutil.WriteFile(t, root, dir+"/kustomization.yaml", "namespace: apps\nresources: [configmap.yaml]\n")
			testutil.WriteFile(t, root, dir+"/configmap.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: "+dir+"}\nspec: {url: oci://fixture.invalid/chart}\n")
		}
		entries, err := Plan(t.Context(), Config{Path: root, RepoRoot: root})
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 || entries[0].ID != "entry" || entries[1].ID != "independent-data" {
			t.Fatalf("native context hid an independent data collision: %+v", entries)
		}
	})

	t.Run("native entry contexts omit outside source collisions", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		sides := []string{"amber", "cobalt", "silver"}
		for _, side := range sides {
			testutil.WriteFile(t, root, "entries/"+side+"/owner.yaml", testutil.KustomizationYAML("owner-"+side, "flux-system", "  path: ./payload/"+side+"\n"))
			testutil.WriteFile(t, root, "entries/"+side+"/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cluster-settings, namespace: flux-system}\ndata: {side: "+side+"}\n")
			testutil.WriteFile(t, root, "payload/"+side+"/kustomization.yaml", "resources: [../../shared]\n")
		}
		testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [common.yaml]\n")
		testutil.WriteFile(t, root, "shared/common.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: common, namespace: apps}\n")
		for _, side := range []string{"first", "second"} {
			testutil.WriteFile(t, root, "outside/"+side+"/kustomization.yaml", "resources: [chart.yaml]\n")
			testutil.WriteFile(t, root, "outside/"+side+"/chart.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: repeated-chart, namespace: apps}\nspec: {url: oci://fixture.invalid/chart}\n")
		}
		testutil.WriteFile(t, root, "outside/unused.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: omitted}\n")
		entries, err := Plan(t.Context(), Config{Path: root, RepoRoot: root, WipeSecrets: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != len(sides) {
			t.Fatalf("native entry plan: %+v", entries)
		}
		for i, entry := range entries {
			want := "entries/" + sides[i]
			if entry.ID != manifest.ClusterID(want) || entry.Path != filepath.Join(root, filepath.FromSlash(want)) {
				t.Fatalf("entry lost native coordinates: %+v", entry)
			}
			st := store.New()
			result, err := Run(t.Context(), Config{Input: entry.Input, Path: entry.Path, Store: st, Trees: testTrees(t), WipeSecrets: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := result.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "common"}); !ok {
				t.Fatal("shared resource disappeared from native entry")
			}
			for id := range result.Index.Records {
				if id.Name == "repeated-chart" || id.Name == "omitted" {
					t.Fatalf("outside root entered native payload: %s", id)
				}
			}
			for _, warning := range st.Warnings() {
				if strings.Contains(warning.Message+" "+strings.Join(warning.Detail, " "), "outside") {
					t.Fatalf("outside omission warned: %+v", warning)
				}
			}
		}
	})

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

func packageHistory(t *testing.T, trees *kustomize.TreeCache, req kustomize.RenderRequest) map[manifest.NamedResource]manifest.DiscoveryRecord {
	t.Helper()
	records, ok := trees.PreviousRender(t.Context(), req, manifest.BootstrapSourceID)
	if !ok || records == nil {
		t.Fatalf("package %q has no complete accepted history", req.SubPath)
	}
	history := maps.Collect(records)
	return history
}

func TestRun_BuiltEntryFixedPoint(t *testing.T) {
	t.Parallel()
	const root = "/virtual/built-fixed-point"
	input := fstest.MapFS{
		"app/kustomization.yaml": {Data: []byte(`resources: [child.yaml, source.yaml]
nameSuffix: -dev
patches:
- target: {kind: Kustomization, name: child}
  patch: '[{"op":"replace","path":"/spec/path","value":"./leaf"}]'
configMapGenerator: [{name: generated, literals: [value=selected]}]
`)},
		"app/child.yaml":          {Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./untransformed\n  sourceRef: {kind: GitRepository, name: nested-dev}\n"))},
		"app/source.yaml":         {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: nested, namespace: apps}\nspec: {url: https://fixture.invalid/self}\n")},
		"leaf/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"leaf/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n")},
	}
	st := store.New()
	result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Trees: testTrees(t), SelfURLs: []string{"https://fixture.invalid/self"}})
	if err != nil {
		t.Fatal(err)
	}
	child := testutil.KustomizationID("apps", "child-dev")
	if st.GetObject(child) == nil || st.GetObject(testutil.KustomizationID("apps", "child")) != nil {
		t.Fatal("transformed child did not replace its raw identity")
	}
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "nested-dev"}
	if st.GetObject(source) == nil || st.GetArtifact(source, store.ArtifactContent) == nil {
		t.Fatal("transformed source did not resolve in the fixed point")
	}
	leaf := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "leaf"}
	record, ok := result.Index.Lookup(leaf)
	if !ok || !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return p.Owner == child }) {
		t.Fatalf("built child did not expand its leaf: %+v", record)
	}
	if len(result.Documents) != 1 {
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
	for _, scenario := range []string{"sanitized", "preexisting", "current-built"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/built-secrets"
			payload := "data: {old: c3RhbGU=}\nstringData: {token: 'ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]'}\n"
			wantData := map[string]any{"old": "c3RhbGU="}
			wantStringData := map[string]any{"token": manifest.ValuePlaceholder("token")}
			if scenario == "current-built" {
				payload = "data: {token: YXV0aG9yZWQ=}\n"
				wantData, wantStringData = map[string]any{"token": "YXV0aG9yZWQ="}, nil
			}
			if scenario == "preexisting" {
				wantData, wantStringData = map[string]any{"current": "cmVhbA=="}, map[string]any{"token": "real-value"}
			}
			contents := "apiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: apps, labels: {selected: 'true'}, annotations: {note: authored}}\ntype: Opaque\n" + payload
			input := fstest.MapFS{
				"package/kustomization.yaml": {Data: []byte("resources: [secret.yaml]\n")},
				"package/secret.yaml":        {Data: []byte(contents)},
			}
			id := manifest.NamedResource{Kind: manifest.KindSecret, Name: "credentials", Namespace: "apps"}
			var previous []map[string]any
			for range 2 {
				st := store.New()
				saved := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"current": "cmVhbA=="}, StringData: map[string]any{"token": "real-value"}}
				if scenario == "preexisting" {
					st.AddObject(saved)
				}
				result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Trees: testTrees(t), WipeSecrets: scenario != "current-built"})
				assert.NoError(t, err)
				if len(result.Documents) != 1 {
					t.Fatalf("selected secret missing: %v", result.Documents)
				}
				doc := result.Documents[0]
				assert.Diff(t, doc["data"], wantData)
				if scenario != "preexisting" {
					assert.Equal(t, doc["type"], any("Opaque"))
				} else {
					_, present := doc["type"]
					assert.Equal(t, present, false)
				}
				if scenario == "current-built" {
					_, present := doc["stringData"]
					assert.Equal(t, present, false)
				} else {
					assert.Diff(t, doc["stringData"], wantStringData)
				}
				obj := st.GetObject(id).(*manifest.Secret)
				assert.Diff(t, obj.Data, wantData)
				assert.Diff(t, obj.StringData, wantStringData)
				if scenario == "preexisting" && obj != saved {
					t.Fatal("store lost preexisting Secret pointer")
				}
				assert.Diff(t, doc["metadata"], map[string]any{"name": "credentials", "namespace": "apps", "labels": map[string]any{"selected": "true"}, "annotations": map[string]any{"note": "authored"}})
				assert.Equal(t, string(input["package/secret.yaml"].Data), contents)
				assert.Diff(t, saved.Data, map[string]any{"current": "cmVhbA=="})
				assert.Diff(t, saved.StringData, map[string]any{"token": "real-value"})
				if previous != nil {
					assert.Diff(t, result.Documents, previous)
				}
				previous = result.Documents
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
			trees, closeCache := testReceiptTrees(t, cacheDir)
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
			if err := d.buildPackage(t.Context(), root, "app"); !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("invalid package build error = %v, want ErrFlux", err)
			}
			if got := d.loader.EntryFailure(t.Context(), root); got == nil || got.Error() != rawError.Error() {
				t.Fatal("failed entry build replaced its raw error")
			}
			if len(d.documents) != 0 {
				t.Fatalf("failed package build published documents: %v", d.documents)
			}
			first := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "first"}
			if d.loader.BuiltDocuments(first) != nil || st.GetObject(first) != nil {
				t.Fatal("failed package published a partial batch")
			}
			closeCache()
			trees, closeCache = testReceiptTrees(t, cacheDir)
			req := kustomize.RenderRequest{Source: source, SubPath: "app"}
			if _, ok := trees.PreviousRender(t.Context(), req, manifest.BootstrapSourceID); ok {
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
			assert.NoError(t, d.buildPackage(t.Context(), root, "app"))
			assert.Equal(t, len(packageHistory(t, trees, req)), 1)
			closeCache()
			trees, closeCache = testReceiptTrees(t, cacheDir)
			assert.Equal(t, len(packageHistory(t, trees, req)), 1)
			closeCache()

		})
	}
}

func TestBuildPackage_PublicationFailure(t *testing.T) {
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
	assert.NoError(t, os.RemoveAll(cacheDir))
	err = d.buildPackage(t.Context(), root, "app")
	if !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, fs.ErrNotExist) || !strings.Contains(err.Error(), `discovery package "app"`) {
		t.Fatalf("publication failure lost classification, cause or package context: %v", err)
	}
	if len(d.loader.BuiltDocuments(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "accepted"})) != 1 {
		t.Fatal("publication fault preceded successful rendering and registration")
	}
	closeCache()
	trees, closeCache = testReceiptTrees(t, cacheDir)
	if _, ok := trees.PreviousRender(t.Context(), kustomize.RenderRequest{Source: source, SubPath: "app"}, manifest.BootstrapSourceID); ok {
		t.Fatal("failed publication left durable accepted history")
	}
	closeCache()
}

func TestRun_IndependentPackageRounds(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, count := range []int{2, 16} {
		t.Run(fmt.Sprintf("packages=%d", count), func(t *testing.T) {
			var logs bytes.Buffer
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			const root = "/virtual/independent-packages"
			input := fstest.MapFS{}
			for i := range count {
				dir := fmt.Sprintf("pkg%d/", i)
				input[dir+"kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\npatches: [{path: patch.yaml}]\n")}
				input[dir+"cm.yaml"] = &fstest.MapFile{Data: fmt.Appendf(nil, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm%d}\ndata: {A: base}\n", i)}
				input[dir+"patch.yaml"] = &fstest.MapFile{Data: fmt.Appendf(nil, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm%d}\ndata: {B: patched}\n", i)}
			}
			result, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: store.New(), Trees: testTrees(t)})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Documents) != count {
				t.Fatalf("built documents = %d, want %d", len(result.Documents), count)
			}
			rounds := 0
			for line := range strings.SplitSeq(logs.String(), "\n") {
				if !strings.Contains(line, "discovery: loaded objects") {
					continue
				}
				var record struct{ Rounds int }
				if err := json.Unmarshal([]byte(line), &record); err != nil {
					t.Fatal(err)
				}
				rounds = record.Rounds
			}
			if rounds == 0 || rounds > 2 {
				t.Fatalf("discovery rounds = %d for %d independent packages, want at most 2", rounds, count)
			}
		})
	}
}

func TestRun_BuiltPackages(t *testing.T) {
	t.Parallel()
	for _, patch := range []string{"patches: [{path: patch.yaml}]", "patchesStrategicMerge: [patch.yaml]"} {
		t.Run(patch, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/built-packages"
			input := fstest.MapFS{
				"consumer.yaml":                 {Data: []byte(testutil.KustomizationYAML("consumer", "apps", "  path: ./output\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings-dev}]}\n"))},
				"a/kustomization.yaml":          {Data: []byte("namespace: apps\nnameSuffix: -dev\nresources: [child, deleted.yaml]\n" + patch + "\n")},
				"a/child/kustomization.yaml":    {Data: []byte("resources: [cm.yaml, driver.yaml, source.yaml]\n")},
				"a/child/driver.yaml":           {Data: []byte(testutil.KustomizationYAML("driver", "apps", "  path: ./output\n  sourceRef: {kind: GitRepository, name: cluster-dev}\n"))},
				"a/child/source.yaml":           {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://fixture.invalid/self}\n")},
				"a/child/cm.yaml":               {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {A: base}\n")},
				"a/deleted.yaml":                {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n")},
				"a/patch.yaml":                  {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {B: patched}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n$patch: delete\n")},
				"a/unlisted/kustomization.yaml": {Data: []byte("resources: [missing.yaml]\n")},
				"b/kustomization.yaml":          {Data: []byte("configMapGenerator: [{name: generated, literals: [C=second]}]\ngeneratorOptions: {disableNameSuffixHash: true}\n")},
				"empty/kustomization.yaml":      {Data: []byte("resources: [cm.yaml]\npatches: [{path: patch.yaml}]\n")},
				"empty/patch.yaml":              {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: empty}\n$patch: delete\n")},
				"empty/cm.yaml":                 {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: empty}\n")},
			}
			cacheDir := t.TempDir()
			var previousDocs []map[string]any
			var previousIndex *manifest.DiscoveryIndex
			var previousWarnings []manifest.Warning
			var previousSourcePath string
			wantValue := "base"
			for _, lifetime := range []string{"uncached", "cold", "warm-1", "warm-2", "changed", "changed-uncached"} {
				t.Logf("cache lifetime: %s", lifetime)
				if lifetime == "changed" {
					wantValue = "edited"
					input["a/child/cm.yaml"].Data = []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\ndata: {A: edited}\n")
				}
				dir := cacheDir
				if lifetime == "uncached" || lifetime == "changed-uncached" {
					dir = ""
				}
				trees, closeCache := testReceiptTrees(t, dir)
				st := store.New()
				source := &store.SourceArtifact{LocalPath: root, FS: input}
				st.SetArtifact(manifest.BootstrapSourceID, source)
				d := discoverer{cfg: Config{Path: root, Store: st, Trees: trees, SelfURLs: []string{"https://fixture.invalid/self"}}, loader: loader.New(st)}
				if lifetime == "changed" {
					if _, ok := trees.PreviousRender(t.Context(), kustomize.RenderRequest{Source: source, SubPath: "a"}, manifest.BootstrapSourceID); ok {
						t.Fatal("changed consumed resource retained stale accepted history")
					}
				}
				if strings.HasPrefix(lifetime, "warm-") {
					for _, subpath := range []string{"a", "b", "empty"} {
						packageHistory(t, trees, kustomize.RenderRequest{Source: source, SubPath: subpath})
					}
				}
				if err := d.loadManifests(t.Context(), root, true); err != nil {
					t.Fatal(err)
				}
				if roots := d.loader.StandaloneRoots(root); len(roots) != 0 {
					t.Fatalf("completed or accumulated packages selected again: %v", roots)
				}
				if dir != "" {
					for _, subpath := range []string{"a", "b", "empty"} {
						want := map[manifest.NamedResource]manifest.DiscoveryRecord{}
						for id, record := range d.loader.Index.Records {
							if record.Builder == subpath+"/kustomization.yaml" {
								want[id] = record
							}
						}
						assert.Diff(t, packageHistory(t, trees, kustomize.RenderRequest{Source: source, SubPath: subpath}), want)
					}
				}
				if err := d.loader.FinalizeIndex(t.Context(), root); err != nil {
					t.Fatal(err)
				}
				d.promoteOrphans(t.Context(), nil)
				id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings-dev"}
				cm, ok := st.GetObject(id).(*manifest.ConfigMap)
				if !ok || cm.Data["A"] != wantValue || cm.Data["B"] != "patched" {
					t.Fatalf("nested package did not promote built data: %+v", cm)
				}
				if len(d.documents) != 2 {
					t.Fatalf("package output = %v, want one transformed and one generated document", d.documents)
				}
				for _, id := range []manifest.NamedResource{
					{Kind: manifest.KindConfigMap, Name: "settings"},
					{Kind: manifest.KindConfigMap, Name: "removed"},
					{Kind: manifest.KindConfigMap, Name: "empty"},
					{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "removed-dev"},
				} {
					if _, ok := d.loader.Index.Lookup(id); ok || st.GetObject(id) != nil {
						t.Errorf("raw, deleted or accumulated output survived: %s", id)
					}
				}
				for _, file := range []string{"a/kustomization.yaml", "a/child/kustomization.yaml", "a/child/cm.yaml", "a/child/driver.yaml", "a/child/source.yaml", "a/patch.yaml", "a/deleted.yaml"} {
					record, _ := d.loader.Index.Lookup(cm.Named())
					if !slices.Contains(record.Inputs, file) || slices.ContainsFunc(d.loader.Index.Readers[file], func(r manifest.FileReader) bool { return r.Reader == cm.Named() }) {
						t.Errorf("%s lost built declaration evidence or acquired render ownership", file)
					}
					filter := change.NewFilter(change.NewSet([]string{file}), d.loader.Index, st, root)
					if !filter.ShouldReconcile(testutil.KustomizationID("apps", "consumer")) {
						t.Errorf("%s does not reach substitution consumer", file)
					}
				}
				if change.NewFilter(change.NewSet([]string{"a/unlisted/other.yaml"}), d.loader.Index, st, root).ShouldReconcile(testutil.KustomizationID("apps", "consumer")) {
					t.Fatal("unlisted sibling selected substitution consumer")
				}
				if docs := d.loader.BuiltDocuments(id); len(docs) != 3 || !slices.ContainsFunc(docs, func(child store.Child) bool { return child.Obj == cm }) {
					t.Fatalf("package batch = %v", docs)
				}
				for id, r := range d.loader.Index.Files() {
					if id.Kind == manifest.KindConfigMap && (r.Path != "" || r.Authored != (manifest.NamedResource{})) {
						t.Errorf("built %s retained raw evidence: %+v", id, r)
					}
				}
				driver := testutil.KustomizationID("apps", "driver-dev")
				sourceID := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "cluster-dev"}
				if st.GetObject(driver) == nil || st.GetObject(sourceID) == nil {
					t.Fatal("filtered discovery declarations were not promoted")
				}
				artifact, ok := st.GetArtifact(sourceID, store.ArtifactContent).(*store.SourceArtifact)
				if !ok || !artifact.InputAlias || artifact.LocalRoot != root {
					t.Fatalf("filtered source did not resolve to the bootstrap tree: %+v", artifact)
				}
				if previousSourcePath != "" {
					assert.Equal(t, artifact.LocalPath, previousSourcePath)
				}
				previousSourcePath = artifact.LocalPath
				assert.Diff(t, d.loader.Index.Records[driver].Refs, []manifest.NamedResource{sourceID})
				for _, id := range []manifest.NamedResource{cm.Named(), driver, sourceID, {Kind: manifest.KindConfigMap, Name: "generated"}} {
					assert.Equal(t, st.EmissionOwner(id), manifest.NamedResource{})
				}
				if lifetime == "uncached" || lifetime == "changed" {
					previousDocs, previousIndex, previousWarnings = d.documents, d.loader.Index, st.Warnings()
				} else {
					assert.Diff(t, d.documents, previousDocs)
					assert.Diff(t, d.loader.Index.Records, previousIndex.Records)
					assert.Diff(t, d.loader.Index.Readers, previousIndex.Readers)
					assert.Diff(t, d.loader.Index.Claims, previousIndex.Claims)
					assert.Diff(t, st.Warnings(), previousWarnings)
				}
				closeCache()
			}
		})
	}
}

func TestRun_BuiltPackageCollisions(t *testing.T) {
	t.Parallel()
	const root = "/virtual/package-collisions"
	input := fstest.MapFS{
		"a/kustomization.yaml": {Data: []byte("resources: [cm.yaml, secret.yaml]\n")},
		"a/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\ndata: {VALUE: first}\n")},
		"a/secret.yaml":        {Data: []byte("apiVersion: v1\nkind: Secret\nmetadata: {name: shared}\nstringData: {VALUE: first}\n")},
		"b/kustomization.yaml": {Data: []byte("resources: [cm.yaml, secret.yaml]\n")},
		"b/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\ndata: {VALUE: last}\n")},
		"b/secret.yaml":        {Data: []byte("apiVersion: v1\nkind: Secret\nmetadata: {name: shared}\nstringData: {VALUE: last}\n")},
	}
	for _, preexisting := range []bool{false, true} {
		t.Run(fmt.Sprintf("preexisting=%t", preexisting), func(t *testing.T) {
			t.Parallel()
			cacheDir := t.TempDir()
			var previous *Result
			var previousWarnings []manifest.Warning
			for _, lifetime := range []string{"uncached", "cold", "warm-1", "warm-2"} {
				t.Logf("cache lifetime: %s", lifetime)
				dir := cacheDir
				if lifetime == "uncached" {
					dir = ""
				}
				trees, closeCache := testReceiptTrees(t, dir)
				st := store.New()
				wantValue := "last"
				if preexisting {
					wantValue = "preexisting"
					st.AddObject(&manifest.ConfigMap{Name: "shared", Data: map[string]any{"VALUE": wantValue}})
					st.AddObject(&manifest.Secret{Name: "shared", StringData: map[string]any{"VALUE": wantValue}})
				}
				source := &store.SourceArtifact{LocalPath: root, FS: input}
				if strings.HasPrefix(lifetime, "warm-") {
					for _, subpath := range []string{"a", "b"} {
						packageHistory(t, trees, kustomize.RenderRequest{Source: source, SubPath: subpath})
					}
				}
				result, err := Run(t.Context(), Config{Input: source, Path: root, Store: st, Trees: trees})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Documents) != 2 {
					t.Fatalf("colliding packages published duplicate output: %v", result.Documents)
				}
				for _, doc := range result.Documents {
					id := manifest.DocIdentity(doc)
					record, _ := result.Index.Lookup(id)
					key := "data"
					if id.Kind == manifest.KindSecret {
						key = "stringData"
					}
					wantDocument := "last"
					if preexisting && id.Kind == manifest.KindSecret {
						wantDocument = wantValue
					}
					if record.File != "b/kustomization.yaml" || doc[key].(map[string]any)["VALUE"] != wantDocument {
						t.Errorf("output winner disagrees with record: %v %+v", doc, record)
					}
					switch obj := st.GetObject(id).(type) {
					case *manifest.ConfigMap:
						if obj.Data["VALUE"] != wantValue {
							t.Errorf("ConfigMap admission retained losing package: %+v", obj)
						}
					case *manifest.Secret:
						if obj.StringData["VALUE"] != wantValue {
							t.Errorf("Secret admission retained losing package: %+v", obj)
						}
					default:
						t.Fatalf("missing admitted output: %s", id)
					}
				}
				if dir != "" {
					for _, subpath := range []string{"a", "b"} {
						history := packageHistory(t, trees, kustomize.RenderRequest{Source: source, SubPath: subpath})
						assert.Equal(t, len(history), 2)
						for id, record := range history {
							assert.Equal(t, record.File, subpath+"/kustomization.yaml")
							assert.Equal(t, record.Builder, subpath+"/kustomization.yaml")
							assert.Equal(t, st.EmissionOwner(id), manifest.NamedResource{})
						}
					}
				}
				if previous != nil {
					assert.Diff(t, result.Documents, previous.Documents)
					assert.Diff(t, result.Index.Records, previous.Index.Records)
					assert.Diff(t, result.Index.Readers, previous.Index.Readers)
					assert.Diff(t, result.Index.Claims, previous.Index.Claims)
					assert.Diff(t, st.Warnings(), previousWarnings)
				}
				previous, previousWarnings = result, st.Warnings()
				closeCache()
			}
		})
	}
}

func TestRun_BuiltPackageInvalidBuilder(t *testing.T) {
	t.Parallel()
	const root = "/virtual/invalid-builder"
	st := store.New()
	input := fstest.MapFS{"kustomization.yaml": {Data: []byte("resources: [broken\n")}}
	if _, err := Run(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Trees: testTrees(t)}); !errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("invalid selected builder error = %v, want ErrFlux", err)
	}
}
