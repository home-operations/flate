package discovery

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveEntrySources_EvidenceAfterRemoval(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [source.yaml, child.yaml]\n")
	testutil.WriteFile(t, root, "entry/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps, uid: source-evidence}\nspec: {url: 'https://example.test/cluster'}\n")
	testutil.WriteFile(t, root, "entry/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
	st := store.New()
	l := loader.New(st)
	l.SourceRoot = root
	d := discoverer{cfg: Config{Path: filepath.Join(root, "entry"), RepoRoot: root, Store: st, SelfURLs: []string{"https://example.test/cluster"}}, loader: l}
	if _, err := d.resolveEntrySources(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Load(t.Context(), d.cfg.Path); err != nil {
		t.Fatal(err)
	}
	l.Admit(root)
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "cluster"}
	if count, err := d.resolveEntrySources(t.Context(), false); err != nil || count != 1 {
		t.Fatalf("initial evidence resolution = %d, %v", count, err)
	}
	before := st.GetArtifact(id).(*store.SourceArtifact)
	child, childDoc, emission := st.GetEmission(testutil.KustomizationID("apps", "child"))
	emission.Finish()
	if !st.DeleteObject(id) {
		t.Fatal("source was not admitted before removal")
	}
	st.DeleteObject(testutil.KustomizationID("apps", "child"))
	if st.GetObject(id) != nil || st.GetArtifact(id) != nil || st.GetObject(testutil.KustomizationID("apps", "child")) != nil {
		t.Fatal("source removal retained published evidence")
	}
	if count, err := d.resolveEntrySources(t.Context(), false); err != nil || count != 1 {
		t.Fatalf("removed evidence resolution = %d, %v", count, err)
	}
	after, ok := st.GetArtifact(id).(*store.SourceArtifact)
	if !ok || !after.InputAlias || after.LocalRoot != before.LocalRoot || after.LocalPath != before.LocalPath || st.GetObject(id) != nil {
		t.Fatalf("loader evidence lost source identity: %+v", after)
	}
	if count, err := d.resolveEntrySources(t.Context(), true); err != nil || count != 0 || st.GetArtifact(id) != after {
		t.Fatalf("unchanged evidence resolution = %d, %v", count, err)
	}
	st.AddObject(child, childDoc)
	if count, err := d.resolveEntrySources(t.Context(), false); err != nil || count != 0 || st.GetArtifact(id) != after {
		t.Fatalf("source republication changed resolved evidence: %d, %v", count, err)
	}
	_, doc, attempt := st.GetEmission(id)
	attempt.Finish()
	if doc == nil || doc["metadata"].(map[string]any)["uid"] != "source-evidence" {
		t.Fatalf("republished source lost its document: %v", doc)
	}

}

func TestResolveEntrySources_Identity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, group, namespace, name, path string
		want                               bool
	}{
		{manifest.KindGitRepository, manifest.SourceDomain, "flux-system", "source", "", true},
		{manifest.KindOCIRepository, manifest.SourceDomain, "other", "source", "", true},
		{manifest.KindBucket, manifest.SourceDomain, "other", "source", "", true},
		{manifest.KindHelmRepository, manifest.SourceDomain, "other", "source", "", false},
		{manifest.KindExternalArtifact, manifest.SourceDomain, "other", "source", "", false},
		{manifest.KindGitRepository, "wrong.group", "other", "source", "", false},
		{manifest.KindGitRepository, manifest.SourceDomain, "", "source", "", false},
		{manifest.KindGitRepository, manifest.SourceDomain, "other", "", "", false},
		{"", "", "other", "", "./apps", false},
		{manifest.KindOCIRepository, manifest.SourceDomain, "other", "", "./apps", false},
	} {
		t.Run(tc.kind+"/"+tc.group+"/"+tc.namespace+"/"+tc.name+"/"+tc.path, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), ".flate-base", "missing")
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{"apps/file": {Data: []byte("input")}}})
			ref := kustomizev1.CrossNamespaceSourceReference{Kind: tc.kind, Name: tc.name}
			if tc.group != "" {
				ref.APIVersion = tc.group + "/v1"
			}
			ks := &manifest.Kustomization{Name: "apps", Namespace: tc.namespace, Path: tc.path, SourceRef: ref}
			st.AddObject(ks)
			d := discoverer{cfg: Config{Path: root, Store: st}}
			count, err := d.resolveEntrySources(t.Context(), true)
			if err != nil || (count == 1) != tc.want {
				t.Fatalf("resolution = %d, %v", count, err)
			}
			obj := st.GetObject(ks.SourceID())
			if tc.want && (obj == nil || obj.Named() != ks.SourceID()) || !tc.want && (obj != nil || st.GetArtifact(ks.SourceID()) != nil) {
				t.Fatalf("identity = %v", obj)
			}
		})
	}
	for _, tc := range []struct {
		name string
		obj  manifest.BaseManifest
		want bool
	}{
		{
			name: "source reference without path fact",
			obj: &manifest.HelmRelease{Name: "release", Namespace: "apps", Chart: manifest.HelmChart{
				Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "source"},
			}}, want: true,
		},
		{
			name: "artifact reference without path fact",
			obj: &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps",
				Spec: map[string]any{"sources": []any{map[string]any{"kind": manifest.KindOCIRepository, "name": "source"}}}}, want: true,
		},
		{
			name: "loader witness without path fact",
			obj: &manifest.RawObject{APIVersion: manifest.FluxKustomizeDomain + "/v1", Kind: manifest.KindKustomization, Name: "lookalike", Namespace: "apps",
				Spec: map[string]any{"path": ".", "sourceRef": map[string]any{"kind": manifest.KindOCIRepository, "name": "source"}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{"file": {}}})
			st.AddObject(tc.obj)
			d := discoverer{cfg: Config{Path: root, Store: st}, loader: loader.New(st)}
			count, err := d.resolveEntrySources(t.Context(), true)
			if err != nil || (count == 1) != tc.want {
				t.Fatalf("generic reference resolution = %d, %v", count, err)
			}
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "source"}
			artifact, ok := st.GetArtifact(id).(*store.SourceArtifact)
			if tc.want && (!ok || artifact.LocalRoot != root || st.GetObject(id).Named() != id) || !tc.want && (ok || st.GetObject(id) != nil) {
				t.Fatalf("generic reference identity or root changed: %+v", artifact)
			}
		})
	}
	for _, scenario := range []string{"supplied", "preseeded", "disk"} {
		t.Run("input precedence/"+scenario, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), ".flate-base", "missing")
			seedRoot := filepath.Join(t.TempDir(), "preseeded")
			st := store.New()
			supplied := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{"file": {Data: []byte("supplied")}}}
			preseeded := &store.SourceArtifact{LocalPath: seedRoot, LocalRoot: seedRoot, FS: fstest.MapFS{"file": {Data: []byte("preseeded")}}}
			cfg := Config{Store: st, SelfURLs: []string{"https://example.test/cluster"}}
			want := supplied
			switch scenario {
			case "supplied":
				cfg.Input = supplied
				st.SetArtifact(manifest.BootstrapSourceID, preseeded)
			case "preseeded":
				st.SetArtifact(manifest.BootstrapSourceID, preseeded)
				want = preseeded
			case "disk":
				cfg.Path = t.TempDir()
				testutil.WriteFile(t, cfg.Path, "file", "disk")
				want = nil
			}
			source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system", URL: cfg.SelfURLs[0]}
			st.AddObject(source)
			d := discoverer{cfg: cfg}
			if _, err := d.resolveEntrySources(t.Context(), true); err != nil {
				t.Fatal(err)
			}
			got := st.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
			if d.cfg.Path != got.LocalPath {
				t.Fatalf("scan coordinate = %q, want admitted root %q", d.cfg.Path, got.LocalPath)
			}
			if want != nil && got != want {
				t.Fatalf("bootstrap = %+v, want authoritative %+v", got, want)
			}
			body, err := fs.ReadFile(got.FS, "file")
			if err != nil || string(body) != scenario {
				t.Fatalf("bootstrap bytes = %q, %v, want %s", body, err, scenario)
			}
			if st.GetObject(source.Named()) != source {
				t.Fatal("authored source replaced")
			}
			alias := st.GetArtifact(source.Named()).(*store.SourceArtifact)
			if alias.ContentFingerprint == nil {
				t.Fatal("alias content fingerprint is absent")
			}
			fingerprint, err := alias.ContentFingerprint(t.Context(), alias.FS)
			if err != nil || fingerprint == "" {
				t.Fatalf("alias callback did not identify admitted bytes: %q, %v", fingerprint, err)
			}
			if want != nil && want.ContentFingerprint != nil {
				t.Fatal("SDK bootstrap must not supply the alias fingerprint")
			}
			body, err = fs.ReadFile(alias.FS, "file")
			if err != nil || string(body) != scenario || alias.Key != "" {
				t.Fatalf("alias bytes = %q, %v; key = %q", body, err, alias.Key)
			}
			if want != nil {
				if _, err := os.Stat(want.LocalPath); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("synthetic label unexpectedly exists: %v", err)
				}
			}
		})
	}
	t.Run("separate identities", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
			"kubernetes/clusters/c0/entry": {}, "kubernetes/apps/x/file": {},
		}})
		var ids []manifest.NamedResource
		for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
			for _, namespace := range []string{"one", "two"} {
				for _, name := range []string{"source", "other"} {
					id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: namespace, Name: name}
					ids = append(ids, id)
					st.AddObject(&manifest.Kustomization{Name: kind + name + namespace, Namespace: "consumer", Path: "./apps/x", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: name, Namespace: namespace}})
				}
			}
		}
		wrong := manifest.NamedResource{Group: "wrong.group", Kind: manifest.KindOCIRepository, Namespace: "one", Name: "source"}
		st.AddObject(&manifest.Kustomization{Name: "wrong", Namespace: "consumer", Path: "./apps/x", SourceRef: kustomizev1.CrossNamespaceSourceReference{APIVersion: wrong.Group + "/v1", Kind: wrong.Kind, Namespace: wrong.Namespace, Name: wrong.Name}})
		d := discoverer{cfg: Config{Path: filepath.Join(root, "kubernetes/clusters/c0"), Store: st}}
		count, err := d.resolveEntrySources(t.Context(), true)
		if err != nil || count != len(ids) {
			t.Fatalf("resolution = %d, %v, want %d", count, err, len(ids))
		}
		for _, id := range ids {
			artifact, ok := st.GetArtifact(id).(*store.SourceArtifact)
			want := filepath.Join(root, "kubernetes")
			if id.Kind == manifest.KindGitRepository {
				want = root
			}
			if !ok || artifact.LocalRoot != want || st.GetObject(id).Named() != id {
				t.Fatalf("%s artifact = %+v, want root %s", id, artifact, want)
			}
		}
		if st.GetObject(wrong) != nil || st.GetArtifact(wrong) != nil {
			t.Fatal("wrong-group identity collided with an entry source")
		}
	})
}

func TestAliasMissingKustomizationSources_OCIRoot(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindBucket} {
		for _, tc := range []struct {
			name, scan   string
			paths, files []string
			want         string
		}{
			{name: "subtree", paths: []string{"./apps/x"}, files: []string{"kubernetes/apps/x/file"}, want: "kubernetes"},
			{name: "repository", paths: []string{"kubernetes/apps/x"}, files: []string{"kubernetes/apps/x/file"}, want: "."},
			{name: "scan", paths: []string{"./apps/x"}, files: []string{"kubernetes/clusters/c0/apps/x/file"}, want: "kubernetes/clusters/c0"},
			{name: "shallowest", paths: []string{"./apps/x"}, files: []string{"apps/x/file", "kubernetes/apps/x/file"}, want: "."},
			{name: "shared constraint", paths: []string{"./apps/x", "./apps/y"}, files: []string{"apps/x/file", "kubernetes/apps/x/file", "kubernetes/apps/y/file"}, want: "kubernetes"},
			{name: "incompatible", paths: []string{"./apps/x", "./root-only"}, files: []string{"kubernetes/apps/x/file", "root-only/file"}},
			{name: "missing", paths: []string{"./absent"}, files: []string{"kubernetes/apps/x/file"}},
			{name: "file", paths: []string{"./apps/x/file"}, files: []string{"kubernetes/apps/x/file"}},
			{name: "empty", paths: []string{""}, files: []string{"file"}, want: "."},
			{name: "dot", paths: []string{"."}, files: []string{"file"}, want: "."},
			{name: "outside scan", scan: "../outside", paths: []string{"./apps/x"}, files: []string{"apps/x/file"}},
			{name: "escaping", paths: []string{"./apps/../../outside"}, files: []string{"outside/file"}},
			{name: "normalized", paths: []string{"./apps/../apps/x"}, files: []string{"kubernetes/apps/x/file"}, want: "kubernetes"},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				for _, reverse := range []bool{false, true} {
					root := t.TempDir()
					testutil.WriteFile(t, root, "absent/file", "checkout decoy")
					input := fstest.MapFS{"kubernetes/clusters/c0/ks.yaml": {Data: []byte("scan")}}
					for _, file := range tc.files {
						input[file] = &fstest.MapFile{Data: []byte(file)}
					}
					st := store.New()
					bootstrap := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input}
					st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
					paths := slices.Clone(tc.paths)
					if reverse {
						slices.Reverse(paths)
					}
					var source manifest.NamedResource
					for i, p := range paths {
						ks := &manifest.Kustomization{Name: string(rune('a' + i)), Namespace: "other", Path: p, SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: "source"}}
						source = ks.SourceID()
						st.AddObject(ks)
					}
					scan := "kubernetes/clusters/c0"
					if tc.scan != "" {
						scan = tc.scan
					}
					d := discoverer{cfg: Config{Path: filepath.Join(root, scan), Store: st}}
					aliases, err := d.resolveEntrySources(t.Context(), true)
					if err != nil {
						t.Fatal(err)
					}
					if tc.want == "" {
						if aliases != 0 || st.GetObject(source) != nil || st.GetArtifact(source) != nil {
							t.Fatalf("inconsistent source aliased: %v", aliases)
						}
						continue
					}
					art, ok := st.GetArtifact(source).(*store.SourceArtifact)
					if !ok || aliases != 1 || st.GetObject(source) == nil {
						t.Fatalf("missing alias: %v, %+v", aliases, art)
					}
					if art.LocalRoot != filepath.Join(root, tc.want) || art.Key != "" || !art.InputAlias {
						t.Fatalf("alias coordinates: %+v", art)
					}
					for _, p := range paths {
						name := filepath.ToSlash(filepath.Clean(manifest.NormalizeClaimBase(p)))
						if info, err := fs.Stat(art.FS, name); err != nil || !info.IsDir() {
							t.Fatalf("alias path %q: %v", name, err)
						}
					}
					st.AddObject(&manifest.Kustomization{Name: "later", Namespace: "other", Path: "./root-only", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name}})
					if aliases, err := d.resolveEntrySources(t.Context(), true); err != nil || aliases != 0 || st.GetArtifact(source) != art {
						t.Fatalf("published root changed: %v, %v", aliases, err)
					}
					if st.GetArtifact(manifest.BootstrapSourceID) != bootstrap {
						t.Fatal("bootstrap replaced")
					}
				}
			})
		}
		t.Run(kind+"/duplicate and replaced witnesses", func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			bootstrap := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{"apps/x/file": {}}}
			st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
			witness := &manifest.Kustomization{Name: "apps", Namespace: "other", Path: "./absent",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: "source"}}
			st.AddObject(witness)
			source := witness.SourceID()
			d := discoverer{cfg: Config{Path: root, Store: st}, loader: loader.New(st)}
			if aliases, err := d.resolveEntrySources(t.Context(), true); err != nil || aliases != 0 || st.GetArtifact(source) != nil {
				t.Fatalf("failed root resolution = %d, %v", aliases, err)
			}
			if !slices.Equal(d.resolvedSources[source].refs, []manifest.BaseManifest{witness}) {
				t.Fatalf("loader/store witnesses were duplicated or copied: %+v", d.resolvedSources[source].refs)
			}
			if aliases, err := d.resolveEntrySources(t.Context(), true); err != nil || aliases != 0 || st.GetArtifact(source) != nil {
				t.Fatalf("unchanged witnesses made progress = %d, %v", aliases, err)
			}
			replacement := witness.Clone()
			replacement.Path = "./apps/x"
			st.AddObject(replacement)
			if aliases, err := d.resolveEntrySources(t.Context(), true); err != nil || aliases != 1 {
				t.Fatalf("replacement witness did not retry = %d, %v", aliases, err)
			}
			artifact, ok := st.GetArtifact(source).(*store.SourceArtifact)
			if !ok || artifact.LocalRoot != root || !artifact.InputAlias || st.GetObject(source).Named() != source || st.GetArtifact(manifest.BootstrapSourceID) != bootstrap {
				t.Fatalf("retried alias changed source or bootstrap identity: %+v", artifact)
			}
		})
	}
}

func TestAliasMissingKustomizationSources_OCIReadErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		canceled bool
		want     error
	}{
		{name: "permission", want: fs.ErrPermission},
		{name: "canceled", canceled: true, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: deniedBootstrap{}})
			st.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "source"}})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			d := discoverer{cfg: Config{Path: root, Store: st}}
			if _, err := d.resolveEntrySources(ctx, true); !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

type deniedBootstrap struct{}

func (deniedBootstrap) Open(name string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
}
