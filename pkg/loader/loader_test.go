package loader

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_ExcludesWorkingTreeGeneratorsUnderPinnedClaim(t *testing.T) {
	t.Parallel()
	root, artifactDir := t.TempDir(), t.TempDir()
	s := store.New()
	sourceID := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
	s.AddObject(&manifest.Kustomization{
		Name: "pinned", Namespace: "flux-system", TargetNamespace: "apps", Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: sourceID.Kind, Name: sourceID.Name, Namespace: sourceID.Namespace},
	})
	s.SetArtifact(sourceID, &store.SourceArtifact{LocalRoot: root, LocalPath: artifactDir, Key: "pinned", FS: os.DirFS(artifactDir)})
	l := loaderFixture(s, nil)
	l.SourceRoot = root
	l.documents = readerForTest(root)
	l.documents.views[artifactDir] = os.DirFS(artifactDir)
	admitForTest(s, root)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "configMapGenerator:\n- name: dirty\n  literals: [value=dirty]\n")
	testutil.WriteFile(t, artifactDir, "apps/kustomization.yaml", "configMapGenerator:\n- name: pinned\n  literals: [value=pinned]\n")
	l.documents.read(filepath.Join(root, "apps"))
	l.documents.read(filepath.Join(artifactDir, "apps"))
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	objects := s.ListAs[*manifest.ConfigMap]("", manifest.KindConfigMap)
	if len(objects) != 1 || objects[0].Name != "pinned" || objects[0].Namespace != "apps" || objects[0].Data["value"] != "pinned" {
		t.Fatalf("expected only the artifact generator: %+v", objects)
	}
	want, err := filepath.Rel(root, filepath.Join(artifactDir, "apps/kustomization.yaml"))
	if got := filesForTest(l.Index)[objects[0].Named()]; err != nil || got != filepath.ToSlash(want) {
		t.Fatalf("artifact generator source = %q, want %q: %v", got, want, err)
	}
}

func TestLoader_DiscoveryBootstrapScanSkipsCoveredDir(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		alias, listed, external bool
	}{
		{name: "same root"},
		{name: "external omitted", external: true},
		{name: "alias omitted", alias: true},
		{name: "alias listed", alias: true, listed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/test/kustomization.yaml", "resources: [network]\n")
			resources := "resources: [ns.yaml, echo.yaml]\n# - ks.yaml\n"
			if tc.listed {
				resources = "resources: [ns.yaml, echo.yaml, ks.yaml]\n"
			}
			testutil.WriteFile(t, root, "apps/test/network/kustomization.yaml", resources)
			testutil.WriteFile(t, root, "apps/test/network/ns.yaml", "apiVersion: v1\nkind: Namespace\nmetadata: {name: network}\n")
			testutil.WriteFile(t, root, "apps/test/network/echo.yaml", testutil.KustomizationYAML("echo", "network", "  path: ./apps/base/echo\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"))
			testutil.WriteFile(t, root, "apps/test/network/ks.yaml", testutil.KustomizationYAML("tunnel", "network", "  path: ./apps/base/tunnel\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n"))
			st := store.New()
			l := loaderFixture(st, nil)
			l.SourceRoot = root
			parent := &manifest.Kustomization{Name: "cluster-apps", Namespace: "flux-system", Path: "apps/test"}
			if tc.alias {
				testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", "  path: ./apps/test\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
				location, err := gitrepo.Locate(t.Context(), root, root)
				if err != nil {
					t.Fatal(err)
				}
				input, err := gittree.AdmitDirectory(t.Context(), location)
				assert.NoError(t, err)
				st.SetArtifact(manifest.BootstrapSourceID, input)
				view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: input.FS})
				assert.NoError(t, err)
				source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system", URL: "file://" + root}
				st.AddObject(source)
				st.SetArtifact(source.Named(), &store.SourceArtifact{LocalRoot: root, LocalPath: filepath.Join(t.TempDir(), "alias"), InputAlias: true, FS: view})
				_, err = loadForTest(t.Context(), t, l, filepath.Join(root, "flux"))
				assert.NoError(t, err)
				parent, _ = st.Get[*manifest.Kustomization](parent.Named())
				if parent == nil {
					t.Fatal("bootstrap parent was not loaded")
				}
			} else {
				if tc.external {
					source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system", URL: "https://github.com/example/other"}
					st.AddObject(source)
					parent.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: source.Name, Namespace: source.Namespace}
				}
				st.AddObject(parent)
			}
			sourceRoot, _, _ := SourceView(st, parent.SourceID(), root)
			_, err := loadForTest(t.Context(), t, l, filepath.Join(sourceRoot, parent.Path))
			assert.NoError(t, err)
			l.Admit(root)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			for _, child := range []struct {
				name string
				want bool
			}{{"echo", true}, {"tunnel", tc.listed}} {
				id := testutil.KustomizationID("network", child.name)
				if got := st.GetObject(id) != nil; got != (child.want && (tc.alias || tc.external)) {
					t.Errorf("Store admission for %s = %v, want %v", child.name, got, child.want)
				}
				if _, got := l.Index.Lookup(id); got != child.want {
					t.Errorf("Index admission for %s = %v, want %v", child.name, got, child.want)
				}
			}
		})
	}
}

func TestLoader_DiscoveryBootstrapScanSkipsPatchFragment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./cm.yaml
  - ./entry.yaml
patches:
  - path: ./patch.yaml
`)
	testutil.WriteFile(t, dir, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm, namespace: apps}\n")
	testutil.WriteFile(t, dir, "apps/entry.yaml", testutil.KustomizationYAML("entry", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	testutil.WriteFile(t, dir, "apps/patch.yaml", testutil.KustomizationYAML("patch", "", "  patches:\n    - patch: |-\n        apiVersion: helm.toolkit.fluxcd.io/v2\n        kind: HelmRelease\n        metadata: {name: not-used}\n      target: {kind: HelmRelease}\n"))

	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	if _, err := loadForTest(context.Background(), t, l, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if s.GetObject(testutil.KustomizationID("flux-system", "entry")) == nil {
		t.Errorf("real bootstrap-entry KS (has spec.path) must surface")
	}
	if got := s.GetObject(testutil.KustomizationID("", "patch")); got != nil {
		t.Errorf("path-less/source-less patch-fragment Kustomization must NOT be surfaced; got %v", got)
	}
}

func TestLoader_Load(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n"))
	testutil.WriteFile(t, dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cm
  namespace: ns
data:
  k: v
`)
	testutil.WriteFile(t, dir, "README.md", "ignored")

	s := store.New()
	l := New(s)
	n, err := loadForTest(t.Context(), t, l, dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 1 || len(l.Index.Records) != 2 {
		t.Errorf("admitted %d objects with %d records, want 1 and 2", n, len(l.Index.Records))
	}
	if got := len(s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)); got != 1 {
		t.Errorf("expected 1 Kustomization, got %d", got)
	}
}

// TestLoader_FollowsEscapingDirResource pins the Biohazard healthcheck
// fix: a directory listed under `resources:` via a parent-escaping
// path is descended into, so a Flux Kustomization defined there reaches
// the store even though nothing points spec.path at it and a tree walk
// would never reach it (it's only a kustomize resources: include).
func TestLoader_FollowsEscapingDirResource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../gate/
`)
	testutil.WriteFile(t, root, "gate/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ks.yaml
`)
	testutil.WriteFile(t, root, "gate/ks.yaml", testutil.KustomizationYAML("crds", "flux-system", "  path: ./blank\n"))

	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = root
	if _, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry")); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.GetObject(testutil.KustomizationID("flux-system", "crds")) == nil {
		t.Fatal("escaping dir resource not followed: Kustomization flux-system/crds missing from store")
	}
}

// TestLoader_EscapingFileResourceStillRejected guards the asymmetry at
// the heart of the fix: directory resources may escape (descend-only),
// but a parent-escaping FILE resource gets opened, so the file policy's
// TOCTOU/path-traversal rejection must still apply.
func TestLoader_EscapingFileResourceStillRejected(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../escape.yaml
`)
	testutil.WriteFile(t, root, "escape.yaml", testutil.KustomizationYAML("escaped", "flux-system", "  path: ./x\n"))

	s := store.New()
	l := loaderFixture(s, nil)
	if _, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry")); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.GetObject(testutil.KustomizationID("flux-system", "escaped")) != nil {
		t.Fatal("escaping FILE resource was loaded; the opened-path escape guard regressed")
	}
}

func TestLoader_SkipsTemplatesDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "chart", "templates"), 0o750); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, dir, "chart/templates/cm.yaml", `{{ if .Values.x }}foo: bar{{ end }}`)
	testutil.WriteFile(t, dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
`)
	l := New(store.New())
	n, err := loadForTest(t.Context(), t, l, dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 0 || len(l.Index.Records) != 1 || len(l.Index.Errors) != 0 {
		t.Errorf("templates scan: admitted=%d records=%v errors=%v", n, l.Index.Records, l.Index.Errors)
	}
}

func TestLoader_SkipsDotDirs(t *testing.T) {
	t.Parallel()
	for _, name := range []string{".git", ".cache", ".hidden"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, filepath.Join(name, "hidden.yaml"), testutil.KustomizationYAML("hidden", "ns", "  {path: ./hidden, sourceRef: {kind: GitRepository, name: flux-system}}\n"))
			testutil.WriteFile(t, dir, "visible.yaml", testutil.KustomizationYAML("visible", "ns", "  {path: ./visible, sourceRef: {kind: GitRepository, name: flux-system}}\n"))
			s := store.New()
			l := New(s)
			if _, err := loadForTest(t.Context(), t, l, dir); err != nil {
				t.Fatalf("Load: %v", err)
			}
			if s.GetObject(testutil.KustomizationID("ns", "hidden")) != nil {
				t.Error("dot-directory manifest was loaded")
			}
			if s.GetObject(testutil.KustomizationID("ns", "visible")) == nil {
				t.Error("root manifest was skipped")
			}
		})
	}
}

// A directory whose kustomization.yaml declares `kind: Component` is
// a template fragment — parents reference it via spec.components and
// kustomize materializes it at parent-render time. flate's standalone
// loader must skip such subtrees, otherwise unresolved template names
// (e.g. `${APP}-db`) land in the store as bogus standalone resources.
func TestLoader_SkipsKustomizeComponentSubtree(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/db/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n")
	testutil.WriteFile(t, dir, "components/db/template.yaml", testutil.KustomizationYAML("${APP}-db", "ns", "  path: ./does/not/matter\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 1h\n"))
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: real, namespace: ns}
`)
	s := store.New()
	l := New(s)
	n, err := loadForTest(t.Context(), t, l, dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 0 || len(l.Index.Records) != 1 {
		t.Errorf("admitted %d objects with records %v, want only the real ConfigMap indexed", n, l.Index.Records)
	}
	if got := s.GetObject(testutil.KustomizationID("ns", "${APP}-db")); got != nil {
		t.Errorf("Component-subtree resource should not be loaded; got %v", got)
	}
}

func TestLoader_SkipsKustomizeComponent_JSONForm(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/foo/Kustomization",
		`{"apiVersion":"kustomize.config.k8s.io/v1alpha1","kind":"Component"}`)
	testutil.WriteFile(t, dir, "components/foo/leak.yaml", testutil.KustomizationYAML("${APP}-leak", "ns", "  path: ./irrelevant\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 1h\n"))
	s := store.New()
	l := loaderFixture(s, nil)
	if _, err := loadForTest(context.Background(), t, l, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := s.GetObject(testutil.KustomizationID("ns", "${APP}-leak"))
	if got != nil {
		t.Errorf("Component subtree leaked a templated KS: %v", got)
	}
}

// TestLoader_ForeignComponentKindIsNotSkipped pins the apiVersion
// gate on the Component detection: a CR with `kind: Component` but a
// non-kustomize apiVersion (e.g. someone's operator's custom CR) MUST
// NOT be treated as a kustomize Component fragment — the subtree
// would silently be skipped and any Flux resources inside vanish from
// the load. Use a kustomization.yaml as the carrier so descend's
// readKustomization picks it up.
func TestLoader_ForeignComponentKindIsNotSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Foreign Component CR shaped exactly like the kustomize one
	// except for apiVersion. With the kind-only check, descend would
	// short-circuit and the sibling KS would never load.
	testutil.WriteFile(t, dir, "addons/kustomization.yaml", `apiVersion: example.com/v1alpha1
kind: Component
resources:
  - ./ks.yaml
`)
	testutil.WriteFile(t, dir, "addons/ks.yaml", testutil.KustomizationYAML("addons", "flux-system", "  path: ./addons\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 1h\n"))
	s := store.New()
	l := loaderFixture(s, nil)
	if _, err := loadForTest(context.Background(), t, l, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := s.GetObject(testutil.KustomizationID("flux-system", "addons"))
	if got == nil {
		t.Error("foreign 'kind: Component' (non-kustomize apiVersion) silently skipped sibling KS — Component gate fired without apiVersion check")
	}
}

func TestLoader_GeneratorDataFiles(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  []manifest.NamedResource
	}{
		{
			name: "SkipsConfigMapGeneratorDataFile",
			files: map[string]string{
				"kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./helmrelease.yaml
configMapGenerator:
  - name: notifier-configmap
    files:
      - hooks.yaml=./resources/hooks.yaml
`,
				"helmrelease.yaml": `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: notifier, namespace: default}
spec:
  chart:
    spec:
      chart: foo
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`,
				"resources/hooks.yaml": `---
- id: radarr-pushover
  execute-command: /config/radarr-pushover.sh
- id: seerr-pushover
  execute-command: /config/seerr-pushover.sh
`,
			},
			want: []manifest.NamedResource{testutil.HelmReleaseID("default", "notifier")},
		},
		{
			name: "SkipsSecretGeneratorDataFile",
			files: map[string]string{
				"kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
secretGenerator:
  - name: tls-bundle
    files:
      - raw.yaml=./data/raw.yaml
`,
				"data/raw.yaml": "this: is: not: valid: yaml: ::\n",
			},
			want: nil,
		},
		{
			name: "GeneratorFilesKeyParsing",
			files: map[string]string{
				"kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
configMapGenerator:
  - name: with-keys
    files:
      - keyed=./data/keyed.yaml
      - ./data/unkeyed.yaml
`,
				"data/keyed.yaml":   "- top: level\n- seq: here\n",
				"data/unkeyed.yaml": "- another: top\n- level: seq\n",
			},
			want: nil,
		},
		{
			name: "ResourceWinsOverGenerator",
			files: map[string]string{
				"kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./shared.yaml
configMapGenerator:
  - name: also-used-as-data
    files:
      - ./shared.yaml
`,
				"shared.yaml": `apiVersion: v1
kind: ConfigMap
metadata: {name: shared, namespace: ns}
`,
			},
			want: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "shared"}},
		},
		{
			name: "NestedKustomizationDataFile",
			files: map[string]string{
				"apps/notifier/kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./manifest.yaml
configMapGenerator:
  - name: cm
    files:
      - hooks.yaml=./resources/hooks.yaml
`,
				"apps/notifier/resources/hooks.yaml": `- id: a\n- id: b\n`,
				"apps/notifier/manifest.yaml": `apiVersion: v1
kind: ConfigMap
metadata: {name: real, namespace: ns}
`,
			},
			want: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "real"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			for path, content := range tc.files {
				testutil.WriteFile(t, dir, path, content)
			}
			l := New(store.New())
			n, err := loadForTest(t.Context(), t, l, dir)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if n != 0 || len(l.Index.Records) != len(tc.want) || len(l.Index.Errors) != 0 {
				t.Fatalf("generator scan: admitted=%d records=%v errors=%v, want %v", n, l.Index.Records, l.Index.Errors, tc.want)
			}
			for _, id := range tc.want {
				if _, ok := l.Index.Lookup(id); !ok {
					t.Errorf("resource %v must still be indexed", id)
				}
			}
		})
	}
}

// TestLoader_OrphanNestedSubtreeSkipped pins the buroa/k8s-gitops
// case: a commented-out wrapper KS in `default/kustomization.yaml`
// (resources: [./valheim/ks.yaml, # ./atuin/ks.yaml]) should leave
// BOTH the wrapper AND the deep HelmRelease under
// `default/atuin/app/helmrelease.yaml` invisible. The wrapper lives
// in a directory (`atuin/`) that has no kustomization.yaml of its
// own; the HR lives in `atuin/app/` which DOES have one but is only
// reachable via the orphan wrapper's spec.path. PR #346 fixed only
// the same-dir orphan case; this test reproduces the deeper shape.
func TestLoader_OrphanNestedSubtreeSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "default/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./valheim/ks.yaml
  # - ./atuin/ks.yaml   # commented toggle stub — should be invisible
`)
	for _, app := range []string{"valheim", "atuin"} {
		testutil.WriteFile(t, dir, "default/"+app+"/ks.yaml", testutil.KustomizationYAML(app, "", "  path: ./default/"+app+"/app\n  sourceRef: { kind: GitRepository, name: flux-system, namespace: flux-system }\n"))
		testutil.WriteFile(t, dir, "default/"+app+"/app/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./helmrelease.yaml
`)
		testutil.WriteFile(t, dir, "default/"+app+"/app/helmrelease.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: `+app+`
spec:
  chart:
    spec:
      chart: app-template
      sourceRef: { kind: HelmRepository, name: bjw-s, namespace: flux-system }
      version: "5.0.0"
`)
	}
	s := store.New()
	// Entry the loader at `default/` so its kustomization.yaml is the
	// graph root. The orphan-tree skip only applies under graph-walk;
	// see Loader.Load doc-comment.
	l := loaderFixture(s, nil)
	if _, err := loadForTest(context.Background(), t, l, filepath.Join(dir, "default")); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Valheim (claimed) MUST be present.
	if _, ok := l.Index.Lookup(testutil.KustomizationID("", "valheim")); !ok {
		t.Error("claimed Kustomization 'valheim' missing")
	}
	// Atuin (commented out) MUST be absent.
	if s.GetObject(testutil.KustomizationID("", "atuin")) != nil {
		t.Error("orphan Kustomization 'atuin' should be skipped (#346 deep-orphan)")
	}
	// Atuin's deep HR MUST also be absent — its only kustomize-graph
	// connection is via the orphan wrapper.
	if s.GetObject(testutil.HelmReleaseID("", "atuin")) != nil {
		t.Error("HelmRelease only reachable via orphan wrapper should be skipped")
	}
	// Valheim's HR should NOT load via the initial graph walk either
	// — it lives in `default/valheim/app/` which is only reachable
	// via the wrapper's spec.path, NOT via `default/kustomization.yaml`'s
	// resource graph. The orchestrator's discovery later calls
	// loadAt(valheim/app/) which becomes a fresh entry-point.
	// (This test stops before that step, so the HR is absent here —
	// which is the correct flux-local behavior.)
	if s.GetObject(testutil.HelmReleaseID("", "valheim")) != nil {
		t.Error("HelmRelease in unreachable subtree leaked into initial walk")
	}
}

// TestLoader_OrphanYAMLSkipped pins the issue-#342 fix: a YAML
// file in a directory governed by a kustomization.yaml is loaded
// only when it appears in `resources:`. The unreferenced file (a
// "toggle stub" — common pattern where maintainers comment out
// resources entries to disable a wrapper without deleting the file)
// must NOT reach the store. Before the fix, the orphan was
// discovered and reconciled against the wrong overlay state,
// producing spurious "dependency not found" failures for any chart
// source the parent's namespace transform was supposed to inject.
func TestLoader_OrphanYAMLSkipped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/games/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: games
resources:
  - ./valheim.yaml
  # - ./vrising.yaml   # disabled toggle stub
`)
	testutil.WriteFile(t, dir, "apps/games/valheim.yaml", testutil.KustomizationYAML("valheim", "", "  interval: 1h\n  path: ./apps/base/games/valheim\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
	testutil.WriteFile(t, dir, "apps/games/vrising.yaml", testutil.KustomizationYAML("vrising", "", "  interval: 1h\n  path: ./apps/base/games/vrising\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
	s := store.New()
	s.AddObject(&manifest.Kustomization{Name: "games", Path: "./apps/games"})
	l := New(s)
	l.SourceRoot = dir
	if _, err := loadForTest(t.Context(), t, l, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// valheim is referenced — must load. Kustomization namespace
	// is left empty by the parser (the YAML has no
	// metadata.namespace); the parent kustomization.yaml's
	// `namespace:` overlay would fill it at render time.
	if _, ok := l.Index.Lookup(testutil.KustomizationID("games", "valheim")); !ok {
		t.Error("referenced Kustomization 'valheim' missing from store")
	}
	// vrising is NOT referenced — must be skipped as orphan.
	if s.GetObject(testutil.KustomizationID("", "vrising")) != nil {
		t.Error("orphan Kustomization 'vrising' should not be loaded (issue #342)")
	}
}

// Component data must retain producer evidence for substituteFrom dependencies
// without admitting templated Flux resources outside their parent's render.
func TestLoader_DiscoveryRecordsComponentDataResources(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/settings/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - ./cluster-settings.yaml
  - ./templated-ks.yaml
`)
	testutil.WriteFile(t, dir, "components/settings/cluster-settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  FOO: bar
`)
	testutil.WriteFile(t, dir, "components/settings/templated-ks.yaml", testutil.KustomizationYAML("${APP}-leak", "ns", "  path: ./irrelevant\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 1h\n"))
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	if _, err := loadForTest(context.Background(), t, l, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	if _, ok := l.Index.Lookup(cmID); !ok {
		t.Errorf("Component-housed CM must be recorded in Existence during discovery")
	}
	if _, ok := filesForTest(l.Index)[cmID]; !ok {
		t.Errorf("Component-housed CM must be recorded in SourceFiles during discovery")
	}
	if s.GetObject(cmID) != nil {
		t.Errorf("Component-housed CM must NOT be in Store during discovery (existence-only)")
	}
	// The templated KS would be filtered upstream by parseFile's
	// envsubst guard even if walkComponentData tried to publish it,
	// but this assertion pins the "Component CRs stay hidden" contract
	// at the boundary the caller cares about: the Store.
	leakID := testutil.KustomizationID("ns", "${APP}-leak")
	if s.GetObject(leakID) != nil {
		t.Errorf("templated Flux CR inside a Component must NOT leak into the Store")
	}
}

// Discovery keeps HelmRelease references even when it defers admission.
func TestLoader_DiscoveryRecordsSourceRefs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "hr.yaml")
	testutil.WriteFile(t, dir, "hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: envoy-gateway, namespace: envoy-gateway}\nspec: {chartRef: {kind: OCIRepository, name: envoy-gateway, namespace: flux-system}}\n")
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	if _, err := loadForTest(t.Context(), t, l, dir); err != nil {
		t.Fatal(err)
	}
	hrID := testutil.HelmReleaseID("envoy-gateway", "envoy-gateway")
	assert.Equal(t, s.GetObject(hrID), manifest.BaseManifest(nil))
	assert.Diff(t, l.Index.Records[hrID].Refs, []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "envoy-gateway"}})
	assert.Equal(t, len(l.documents.cache[path].objects), 1)
	ksPath := filepath.Join(dir, "ks.yaml")
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("app", "apps", "  {path: ./apps}\n"))
	if _, err := l.loadFile(ksPath); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(l.documents.cache[ksPath].objects), 1)
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		file := filepath.Join(dir, kind+".yaml")
		testutil.WriteFile(t, dir, kind+".yaml", "apiVersion: v1\nkind: "+kind+"\nmetadata: {name: data, namespace: apps}\n")
		if _, err := l.loadFile(file); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, len(l.documents.cache[file].objects), 1)
		assert.Equal(t, s.GetObject(manifest.NamedResource{Kind: kind, Name: "data", Namespace: "apps"}), manifest.BaseManifest(nil))
	}
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps", SourceRef: sourcev1.LocalHelmChartSourceReference{Name: "source"}}
	l.recordObject(chart, filepath.Join(dir, "chart.yaml"))
	assert.Diff(t, l.Index.Records[chart.Named()].Refs, []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "source"}})
	for _, source := range []kustomizev1.CrossNamespaceSourceReference{{Kind: manifest.KindGitRepository}, {Name: "source"}, {}} {
		ks := &manifest.Kustomization{Name: "incomplete", Namespace: "apps", SourceRef: source}
		l.recordObject(ks, ksPath)
		assert.Equal(t, len(l.Index.Records[ks.Named()].Refs), 0)
	}
}

// TestLoader_DiscoveryRecordsNestedComponentData pins the
// Component-of-Component recursion in walkComponentData: an outer
// Component that references an inner Component via `components:`
// must surface the inner Component's CM/Secret data files into the
// producer index. The reference PR (tjwallace/flate#1) walked only
// `resources:` and missed this graph shape — this test is the fence.
func TestLoader_DiscoveryRecordsNestedComponentData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/outer/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../inner
`)
	testutil.WriteFile(t, dir, "components/inner/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - ./shared.yaml
`)
	testutil.WriteFile(t, dir, "components/inner/shared.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: shared-cm
  namespace: flux-system
data:
  KEY: value
`)
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	// Drive the loader at the outer Component directly — same shape
	// as TestLoader_SkipsKustomizeComponentSubtree's entry pattern.
	if _, err := loadForTest(context.Background(), t, l, filepath.Join(dir, "components/outer")); err != nil {
		t.Fatalf("Load: %v", err)
	}

	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "shared-cm"}
	if _, ok := l.Index.Lookup(id); !ok {
		t.Errorf("inner Component CM reached via Component-of-Component must be recorded in Existence")
	}
	if _, ok := filesForTest(l.Index)[id]; !ok {
		t.Errorf("inner Component CM must be recorded in SourceFiles")
	}
}

// TestLoader_DiscoveryComponentCycleTerminates pins the
// w.visited cycle protection in walkComponentData: a pair of
// Components that reference each other via `components:` must not
// infinite-loop. The same primitive descend uses; verify here so a
// future change can't drop it.
func TestLoader_DiscoveryComponentCycleTerminates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/a/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../b
`)
	testutil.WriteFile(t, dir, "components/b/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
components:
  - ../a
`)
	s := store.New()
	l := loaderFixture(s, nil)
	// No timeout context: if cycle protection fails, the test hangs
	// and the test framework's overall timeout surfaces the bug.
	if _, err := loadForTest(context.Background(), t, l, filepath.Join(dir, "components/a")); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestLoader_DiscoveryPicksUpBootstrapSiblingKS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "repositories/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./source.yaml
generators: ["# flux-entry.yaml"]
`)
	testutil.WriteFile(t, dir, "repositories/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: chart
  namespace: flux-system
spec:
  url: oci://example.com/chart
  ref:
    tag: 1.0.0
`)
	testutil.WriteFile(t, dir, "repositories/# flux-entry.yaml", testutil.KustomizationYAML("flux-entry-repos", "flux-system", "  path: ./repositories\n  sourceRef:\n    kind: GitRepository\n    name: cluster\n  interval: 10m\n"))
	testutil.WriteFile(t, dir, "repositories/stray-hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: stray
  namespace: flux-system
spec:
  chartRef:
    kind: OCIRepository
    name: chart
    namespace: flux-system
`)

	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	if _, err := loadForTest(context.Background(), t, l, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	ksID := testutil.KustomizationID("flux-system", "flux-entry-repos")
	if s.GetObject(ksID) != nil || l.AdmitsRoot(ksID, filepath.Join(dir, "repositories/# flux-entry.yaml")) {
		t.Error("unlisted declaration became runnable")
	}
	if got, want := filesForTest(l.Index)[ksID], "repositories/# flux-entry.yaml"; got != want {
		t.Errorf("SourceFiles[KS] = %q; want %q", got, want)
	}

	hrID := testutil.HelmReleaseID("flux-system", "stray")
	if s.GetObject(hrID) != nil {
		t.Errorf("sibling HR must NOT be picked up by the bootstrap scan (scope is Flux KS only)")
	}
	if _, ok := l.Index.Lookup(hrID); ok {
		t.Errorf("sibling HR must NOT be recorded in Existence either — it's not in the kustomize graph")
	}
}

// TestLoader_BootstrapSiblingScanDedupesWithResources guards the
// "in BOTH places" edge case: a Flux KS file listed in resources AND
// sitting beside the kustomization.yaml must load exactly once. The
// dedup key is the resolved absolute path on each side.
func TestLoader_BootstrapSiblingScanDedupesWithResources(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ./entry.yaml
`)
	testutil.WriteFile(t, dir, "entry.yaml", testutil.KustomizationYAML("entry", "flux-system", "  path: ./\n  sourceRef:\n    kind: GitRepository\n    name: cluster\n  interval: 10m\n"))
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	if _, err := loadForTest(context.Background(), t, l, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := len(s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)); got != 1 {
		t.Errorf("expected exactly 1 Kustomization (no double-load), got %d", got)
	}
}

// TestLoader_BootstrapSiblingHonorsKrmignore guards the ignore
// integration: a Flux KS sibling that matches a .krmignore pattern
// must NOT be loaded. Otherwise the bootstrap scan would silently
// override the user's explicit "don't look here" directive — the rest
// of the loader honors .krmignore for ad-hoc and resource paths.
func TestLoader_BootstrapSiblingHonorsKrmignore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, ".krmignore", "flux-entry.yaml\n")
	testutil.WriteFile(t, dir, "kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
`)
	testutil.WriteFile(t, dir, "flux-entry.yaml", testutil.KustomizationYAML("ignored", "flux-system", "  path: ./\n  sourceRef:\n    kind: GitRepository\n    name: cluster\n  interval: 10m\n"))
	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	if _, err := loadForTest(context.Background(), t, l, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := len(s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)); got != 0 {
		t.Errorf("ignored bootstrap-sibling KS must NOT load, got %d in Store", got)
	}
}

// TestLoader_RespectsCanceledContext asserts the walk bails out on
// context cancellation. Useful when a stuck NFS mount or symlink
// loop would otherwise block Bootstrap indefinitely.
func TestLoader_RespectsCanceledContext(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "a.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New(store.New()).Load(ctx, dir)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestLoader_MalformedManifestFails(t *testing.T) {
	t.Parallel()
	const broken = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: blackbox-exporter
  namespace: flux-system
spec:
  path: ./apps/blackbox-exporter
  sourceRef:
    kind: GitRepository
    name: flux-system
brokenPointer: [ unclosed
`
	cases := []struct {
		name, body string
		tolerated  bool
	}{
		{name: "invalid YAML", body: broken},
		{name: "missing required version", body: "dependsOn: [{kind: Kustomization, name: target}]"},
		{name: "slash", body: "dependsOn: [{apiVersion: '/', kind: Kustomization, name: target}]"},
		{name: "empty version", body: "dependsOn: [{apiVersion: 'example.test/', kind: Kustomization, name: target}]"},
		{name: "multi slash", body: "dependsOn: [{apiVersion: 'example.test/v1/extra', kind: Kustomization, name: target}]"},
		{name: "optional empty version", body: "inputsFrom: [{apiVersion: '', name: target}]"},
		{name: "optional multi slash", body: "inputsFrom: [{apiVersion: 'example.test/v1/extra', name: target}]"},
		{name: "typed empty version", body: "apiVersion: helm.toolkit.fluxcd.io/\nkind: HelmRelease\nmetadata: {name: invalid}\n"},
		{name: "raw empty version", body: "apiVersion: example.test/\nkind: Foreign\nmetadata: {name: invalid}\n"},
		{name: "raw parse error", body: "apiVersion: example.test/v1/extra\nkind: Foreign\nmetadata: {name: invalid}\n"},
		{name: "unrelated configuration", body: "kind: Configuration\nsettings: {enabled: true}\n", tolerated: true},
		{name: "unrelated HelmRelease", body: "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: invalid}\nspec: {chart: {spec: {}}}\n", tolerated: true},
		{name: "unrelated ResourceSet", body: "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {}\nspec: {}\n", tolerated: true},
	}
	for _, tc := range cases {
		for _, graph := range []bool{false, true} {
			t.Run(tc.name+"/graph="+fmt.Sprint(graph), func(t *testing.T) {
				t.Parallel()
				dir := filepath.Join(t.TempDir(), "app")
				body := tc.body
				if strings.HasPrefix(body, "dependsOn:") || strings.HasPrefix(body, "inputsFrom:") {
					body = "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: invalid}\nspec:\n  " + body + "\n"
				}
				if tc.tolerated {
					body += "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy}\n"
				}
				testutil.WriteFile(t, dir, "blackbox-exporter.yaml", body)
				if graph {
					testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [blackbox-exporter.yaml]\n")
				}
				l := New(store.New())
				l.SourceRoot = filepath.Dir(dir)
				if _, err := loadForTest(t.Context(), t, l, dir); err != nil {
					t.Fatalf("indexed discovery aborted before ownership was known: %v", err)
				}
				first := l.Index.Errors["app/blackbox-exporter.yaml"]
				if tc.tolerated {
					if _, indexed := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "healthy"}); first != nil || !indexed {
						t.Fatalf("unrelated document poisoned its healthy sibling: error=%v", first)
					}
				} else if !errors.Is(first, manifest.ErrInput) || !strings.Contains(first.Error(), filepath.Join(dir, "blackbox-exporter.yaml")) {
					t.Fatalf("source-qualified decode error = %v, want manifest.ErrInput", first)
				}
				if _, err := loadForTest(t.Context(), t, l, filepath.Dir(dir)); err != nil {
					t.Fatal(err)
				}
				if got := l.Index.Errors["app/blackbox-exporter.yaml"]; !errors.Is(got, first) {
					t.Fatalf("second walk replaced the first decode failure: first=%+v got=%+v", first, got)
				}
			})
		}
	}
}

func TestLoader_IndependentRoots(t *testing.T) {
	t.Parallel()
	for _, first := range []string{"plain", "builder"} {
		t.Run(first+" first", func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "plain/empty.txt", "")
			testutil.WriteFile(t, root, "builder/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, root, "builder/ks.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"))
			l := New(store.New())
			l.SourceRoot = root
			l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./builder"})
			second := "builder"
			if first == "builder" {
				second = "plain"
			}
			for _, dir := range []string{first, second} {
				_, err := l.Load(t.Context(), filepath.Join(root, dir))
				assert.NoError(t, err)
			}
			l.Admit(root)
			child := testutil.KustomizationID("apps", "child")
			if _, ok := l.Index.Lookup(child); !ok {
				t.Fatal("loaded child evidence missing")
			}
			if (l.Store.GetObject(child) != nil) != (first == "plain") {
				t.Fatal("later load changed first entry admission policy")
			}
			independent := New(store.New())
			independent.SourceRoot = root
			independent.Store.AddObject(l.Store.GetObject(testutil.KustomizationID("apps", "owner")))
			_, err := independent.Load(t.Context(), filepath.Join(root, "builder"))
			assert.NoError(t, err)
			independent.Admit(root)
			if independent.Store.GetObject(child) != nil {
				t.Fatal("independent loader inherited first entry policy")
			}
		})
	}

	t.Run("retained native views", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		for _, name := range []string{"first", "second"} {
			testutil.WriteFile(t, root, name+"/ks.yaml", testutil.KustomizationYAML(name, "", "  {path: ./apps}\n"))
		}
		location, err := gitrepo.Locate(t.Context(), root, root)
		assert.NoError(t, err)
		artifact, err := gittree.AdmitDirectory(t.Context(), location)
		assert.NoError(t, err)
		s := store.New()
		s.SetArtifact(manifest.BootstrapSourceID, artifact)
		l := New(s)
		n, err := loadForTest(t.Context(), t, l, filepath.Join(root, "first"))
		assert.NoError(t, err)
		assert.Equal(t, n, 1)
		view := l.documents.views[root]
		observed, err := fs.Stat(view, "first")
		assert.NoError(t, err)
		if view == artifact.FS {
			t.Fatal("loader did not retain a private operation view")
		}
		_, err = fs.ReadDir(view, "second")
		assert.NoError(t, err)
		_, err = fs.Stat(view, "second/late.yaml")
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("uncreated file error = %v, want not exist", err)
		}
		testutil.WriteFile(t, root, "second/late.yaml", testutil.KustomizationYAML("late", "", "  {path: ./apps}\n"))
		n, err = loadForTest(t.Context(), t, l, filepath.Join(root, "second"))
		assert.NoError(t, err)
		if n != 1 {
			t.Fatalf("second graph loaded %d objects, want one from retained directory observations", n)
		}
		if l.documents.views[root] != view {
			t.Fatal("unchanged artifact replaced the private view")
		}
		retained, err := fs.Stat(l.documents.views[root], "first")
		assert.NoError(t, err)
		if retained != observed {
			t.Fatal("load discarded retained directory observations")
		}
		for _, name := range []string{"first", "second"} {
			id := testutil.KustomizationID("", name)
			record, indexed := l.Index.Lookup(id)
			if !indexed || record.Path != filepath.Join(root, name, "ks.yaml") || s.GetObject(id) == nil {
				t.Fatalf("%s graph was not loaded and indexed: %+v", name, record)
			}
		}
		if _, err := fs.Stat(l.documents.views[root], "second/late.yaml"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("load discarded the retained miss: %v", err)
		}
		other := New(s)
		_, err = other.Load(t.Context(), filepath.Join(root, "second"))
		assert.NoError(t, err)
		if other.documents.views[root] == view || other.documents.views[root] == artifact.FS {
			t.Fatal("loaders shared their operation views")
		}
		if _, err := fs.Stat(other.documents.views[root], "second/late.yaml"); err != nil {
			t.Fatalf("second loader inherited a private miss: %v", err)
		}
		if s.GetArtifact(manifest.BootstrapSourceID) != artifact {
			t.Fatal("loader replaced the source artifact")
		}
		if _, err := fs.Stat(artifact.FS, "second/late.yaml"); err != nil {
			t.Fatalf("loader mutated the artifact's observations: %v", err)
		}
	})
	t.Run("admitted membership", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "first.yaml", testutil.KustomizationYAML("first", "", "  {path: ./apps}\n"))
		location, err := gitrepo.Locate(t.Context(), root, root)
		assert.NoError(t, err)
		admitted, err := gittree.AdmitDirectory(t.Context(), location)
		assert.NoError(t, err)
		input, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: admitted.FS})
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "second.yaml", testutil.KustomizationYAML("second", "", "  {path: ./apps}\n"))
		s := store.New()
		s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
		l := New(s)
		n, err := loadForTest(t.Context(), t, l, root)
		assert.NoError(t, err)
		assert.Equal(t, n, 1)
		objects := s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
		if len(objects) != 1 || objects[0].Name != "first" {
			t.Fatalf("admitted objects = %v, want first only", objects)
		}
	})
	for _, tc := range []struct {
		name    string
		bounded bool
		nested  bool
	}{
		{name: "independent"},
		{name: "bounded", bounded: true},
		{name: "nested", nested: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			roots := []string{t.TempDir(), t.TempDir()}
			if tc.nested {
				roots[0] = filepath.Join(roots[1], "child")
			}
			ids := []manifest.NamedResource{
				testutil.KustomizationID("", "first"),
				testutil.KustomizationID("", "second"),
			}
			for i, root := range roots {
				testutil.WriteFile(t, root, "ks.yaml", testutil.KustomizationYAML(ids[i].Name, "", "  {path: ./apps}\n"))
			}
			l := New(store.New())
			if tc.bounded {
				l.SourceRoot = roots[0]
			}
			if _, err := loadForTest(t.Context(), t, l, roots[0]); err != nil {
				t.Fatal(err)
			}
			if l.Store.GetArtifact(manifest.BootstrapSourceID) != nil || l.Store.GetObject(manifest.BootstrapSourceID) != nil {
				t.Fatal("standalone load published bootstrap identity")
			}
			view := l.documents.views[roots[0]]
			observed, err := fs.Stat(view, ".")
			assert.NoError(t, err)
			for _, root := range []string{roots[1], roots[0]} {
				_, err := loadForTest(t.Context(), t, l, root)
				if tc.bounded && root == roots[1] {
					if !errors.Is(err, manifest.ErrInput) {
						t.Fatalf("outside SourceRoot error = %v, want unavailable source", err)
					}
					if l.documents.views[roots[0]] != view {
						t.Fatal("outside SourceRoot load replaced the mounted view")
					}
					if _, indexed := l.Index.Lookup(ids[1]); indexed || l.Store.GetObject(ids[1]) != nil || len(l.Index.Records) != 1 || len(l.Index.Errors) != 0 {
						t.Fatal("unrelated root supplied objects or evidence outside SourceRoot")
					}
				} else if err != nil {
					t.Fatal(err)
				}
				retained, err := fs.Stat(l.documents.views[roots[0]], ".")
				assert.NoError(t, err)
				if retained != observed {
					t.Fatal("standalone binding discarded operation observations")
				}
				if l.Store.GetArtifact(manifest.BootstrapSourceID) != nil || l.Store.GetObject(manifest.BootstrapSourceID) != nil {
					t.Fatal("independent load published bootstrap identity")
				}
				if tc.nested {
					input, file := l.documents.input(filepath.Join(roots[0], "ks.yaml"))
					if input != view || file != "ks.yaml" {
						t.Fatalf("child source binding = (%v, %q), want original view and ks.yaml", input, file)
					}
				}
				for i, id := range ids {
					if tc.bounded && i == 1 {
						continue
					}
					r, indexed := l.Index.Lookup(id)
					path := filepath.Join(roots[i], "ks.yaml")
					file := filepath.ToSlash(path)
					if tc.bounded {
						file = "ks.yaml"
					}
					if !indexed || r.Path != path || r.File != file || l.Store.GetObject(id) == nil {
						t.Fatalf("%s record = %+v, present = %t; want path %s, file %s and stored object", id, r, indexed, path, file)
					}
				}
			}
		})
	}
}

type unreadableListingFS struct{ fs.FS }

type unreadableInputFS struct{ fs.FS }

func (f unreadableInputFS) Open(name string) (fs.File, error) {
	if name == "kustomization.yaml" {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}
	return f.FS.Open(name)
}

func (unreadableListingFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrPermission}
}

func TestLoader_WarningConditions(t *testing.T) {
	t.Parallel()
	for _, site := range []string{"kustomization", "sibling", "component", "readdir"} {
		t.Run(site, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
			bad := "apiVersion: v1\nkind: ConfigMap\nbroken: [ unclosed\n"
			switch site {
			case "sibling":
				testutil.WriteFile(t, root, "bad.yaml", bad)
			case "component":
				testutil.WriteFile(t, root, "kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [bad.yaml]\n")
				testutil.WriteFile(t, root, "bad.yaml", bad)
			}
			st := store.New()
			l := New(st)
			l.documents = readerForTest(root)
			if site == "kustomization" {
				l.documents.views[root] = unreadableInputFS{FS: os.DirFS(root)}
			}
			if site == "readdir" {
				l.documents.views[root] = unreadableListingFS{FS: os.DirFS(root)}
			}
			w := &walker{loader: l, scanRoot: root}
			var err error
			if site == "component" {
				_, err = w.walkComponentData(t.Context(), root)
			} else {
				_, err = w.walkKustomize(t.Context(), root, filepath.Join(root, "kustomization.yaml"))
			}
			assert.NoError(t, err)
			warnings := st.Warnings()
			category := manifest.WarnInputParseFailed
			if site == "readdir" {
				category = manifest.WarnInputReadFailed
			}
			if len(warnings) != 1 || warnings[0].Category != category || warnings[0].Count != 1 || warnings[0].Resource != (manifest.NamedResource{}) || !strings.HasPrefix(warnings[0].Detail[0], root) || len(warnings[0].Detail) != 2 {
				t.Fatalf("%s warning = %+v", site, warnings)
			}
		})
	}
}

func TestLoader_PreferExistingIndexedObject(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "first/kustomization.yaml", "resources: [ks.yaml]\n")
	for _, dir := range []string{"first", "second"} {
		testutil.WriteFile(t, root, dir+"/ks.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./"+dir+"\n"))
	}
	l := New(store.New())
	l.SourceRoot, l.PreferExisting = root, true
	for _, dir := range []string{"first", "second"} {
		_, err := l.Load(t.Context(), filepath.Join(root, dir))
		assert.NoError(t, err)
	}
	l.Admit(root)
	id := testutil.KustomizationID("apps", "child")
	record, _ := l.Index.Lookup(id)
	if record.Path != filepath.Join(root, "first/ks.yaml") || len(l.Kustomizations()) != 1 || l.Kustomizations()[0].Obj.(*manifest.Kustomization).Path != "./first" {
		t.Fatalf("first indexed object was replaced: %+v, %+v", record, l.Kustomizations())
	}
}

func TestLoader_InvalidBuilderStopsRawDescent(t *testing.T) {
	t.Parallel()
	for _, entry := range []string{"graph", "plain"} {
		t.Run(entry, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			if entry == "graph" {
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: [invalid]\n")
			}
			testutil.WriteFile(t, root, "invalid/kustomization.yaml", "resources: [\n")
			testutil.WriteFile(t, root, "invalid/ks.yaml", testutil.KustomizationYAML("raw", "apps", "  path: ./leaf\n"))
			l := New(store.New())
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
			l.Admit(root)
			if _, ok := l.Index.Lookup(testutil.KustomizationID("apps", "raw")); ok {
				t.Fatal("invalid builder admitted raw resource")
			}
		})
	}
}

func TestLoader_ProjectedEvidence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "namespace: apps\nresources: [objects.yaml]\n")
	testutil.WriteFile(t, root, "objects.yaml", testutil.KustomizationYAML("child", "", "  path: ./leaf\n")+"---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: source}\nspec: {url: https://fixture.invalid/source}\n")
	l := New(store.New())
	l.SourceRoot = root
	l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."})
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	for _, cached := range []bool{true, false} {
		if !cached {
			l.candidates = nil
		}
		sources := 0
		for child := range l.Sources() {
			source := child.Obj
			sources++
			assert.Equal(t, source.Named().Namespace, "apps")
		}
		assert.Equal(t, sources, 1)
		owners := l.Kustomizations()
		assert.Equal(t, len(owners), 2)
		for _, owner := range owners {
			assert.Equal(t, owner.Obj.Named().Namespace, "apps")
		}
	}
	for _, obj := range l.documents.cache[filepath.Join(root, "objects.yaml")].objects {
		assert.Equal(t, obj.Obj.Named().Namespace, "")
	}
}
