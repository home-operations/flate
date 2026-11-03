package loader

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
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
	sourceID := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "cluster",
	}
	s.AddObject(&manifest.Kustomization{
		Name:            "pinned",
		Namespace:       "flux-system",
		TargetNamespace: "apps",
		Path:            "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      sourceID.Kind,
			Name:      sourceID.Name,
			Namespace: sourceID.Namespace,
		},
	})
	s.SetArtifact(
		sourceID,
		&store.SourceArtifact{
			LocalRoot: root,
			LocalPath: artifactDir,
			Key:       "pinned",
			Content:   "pinned",
			FS:        os.DirFS(artifactDir),
		},
	)
	l := loaderFixture(s, nil)
	l.SourceRoot = root
	l.documents = readerForTest(root)
	l.documents.views[artifactDir] = os.DirFS(artifactDir)
	admitForTest(s, root)
	testutil.WriteFile(
		t,
		root,
		"apps/kustomization.yaml",
		"configMapGenerator:\n- name: dirty\n  literals: [value=dirty]\n",
	)
	testutil.WriteFile(
		t,
		artifactDir,
		"apps/kustomization.yaml",
		"configMapGenerator:\n- name: pinned\n  literals: [value=pinned]\n",
	)
	l.documents.read(filepath.Join(root, "apps"))
	l.documents.read(filepath.Join(artifactDir, "apps"))
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "pinned"}
	object, ok := lookupDataForTest[*manifest.ConfigMap](t, s, id)
	if !ok || object.Data["value"] != "pinned" || len(s.ListObjects("", manifest.KindConfigMap)) != 0 {
		t.Fatalf("artifact lookup data=%+v, canonical maps=%v", object, s.ListObjects("", manifest.KindConfigMap))
	}
	want, err := filepath.Rel(root, filepath.Join(artifactDir, "apps/kustomization.yaml"))
	{
		got := filesForTest(l.Index)[id]
		assert.FatalIf(
			t,
			err != nil || got != filepath.ToSlash(want),
			"artifact generator source=%q, want %q: %v",
			got,
			want,
			err,
		)
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
			testLoaderDiscoveryBootstrapScanSkipsCoveredDirCase(
				t,
				&tc,
			)
		},
		)
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
	testutil.WriteFile(
		t,
		dir,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm, namespace: apps}\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/entry.yaml",
		testutil.KustomizationYAML(
			"entry",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/patch.yaml",
		testutil.KustomizationYAML(
			"patch",
			"",
			"  patches:\n    - patch: |-\n        apiVersion: helm.toolkit.fluxcd.io/v2\n        kind: HelmRelease\n        metadata: {name: not-used}\n      target: {kind: HelmRelease}\n",
		),
	)

	s := store.New()
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}

	{
		assert.ErrorIf(
			t,
			s.GetObject(testutil.KustomizationID("flux-system", "entry")) == nil,
			"real bootstrap-entry KS (has spec.path) must surface",
		)
	}
	{
		got := s.GetObject(testutil.KustomizationID("", "patch"))
		assert.ErrorIf(
			t,
			got != nil,
			"path-less/source-less patch-fragment Kustomization must NOT be surfaced; got %v",
			got,
		)
	}
}

func TestLoader_Load(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n",
		),
	)
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
	{
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	if n != 1 || len(l.Index.Records) != 2 {
		t.Errorf("admitted %d objects with %d records, want 1 and 2", n, len(l.Index.Records))
	}
	{
		got := len(s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization))
		assert.ErrorIf(t, got != 1, "expected 1 Kustomization, got %d", got)
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
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry"))
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	{
		assert.FatalIf(
			t,
			s.GetObject(testutil.KustomizationID("flux-system", "crds")) == nil,
			"%s",
			"escaping dir resource not followed: Kustomization flux-system/crds missing from store",
		)
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
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry"))
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	{
		assert.FatalIf(
			t,
			s.GetObject(testutil.KustomizationID("flux-system", "escaped")) != nil,
			"%s",
			"escaping FILE resource was loaded; the opened-path escape guard regressed",
		)
	}
}

func TestLoader_SkipsTemplatesDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	{
		err := os.MkdirAll(filepath.Join(dir, "chart", "templates"), 0o750)
		assert.NoError(t, err)
	}
	testutil.WriteFile(t, dir, "chart/templates/cm.yaml", `{{ if .Values.x }}foo: bar{{ end }}`)
	testutil.WriteFile(t, dir, "cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
`)
	l := New(store.New())
	n, err := loadForTest(t.Context(), t, l, dir)
	{
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	if n != 0 || len(l.Index.Records) != 1 || len(retainedErrorsForTest(l)) != 0 {
		t.Errorf("templates scan: admitted=%d records=%v errors=%v", n, l.Index.Records, retainedErrorsForTest(l))
	}
}

func TestLoader_SkipsDotDirs(t *testing.T) {
	t.Parallel()
	for _, name := range []string{".git", ".cache", ".hidden"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(
				t,
				dir,
				filepath.Join(name, "hidden.yaml"),
				testutil.KustomizationYAML(
					"hidden",
					"ns",
					"  {path: ./hidden, sourceRef: {kind: GitRepository, name: flux-system}}\n",
				),
			)
			testutil.WriteFile(
				t,
				dir,
				"visible.yaml",
				testutil.KustomizationYAML(
					"visible",
					"ns",
					"  {path: ./visible, sourceRef: {kind: GitRepository, name: flux-system}}\n",
				),
			)
			s := store.New()
			l := New(s)
			{
				_, err := loadForTest(t.Context(), t, l, dir)
				assert.FatalIf(t, err != nil, "Load: %v", err)
			}
			{
				assert.ErrorIf(
					t,
					s.GetObject(testutil.KustomizationID("ns", "hidden")) != nil,
					"%s",
					"dot-directory manifest was loaded",
				)
			}
			{
				assert.ErrorIf(
					t,
					s.GetObject(testutil.KustomizationID("ns", "visible")) == nil,
					"%s",
					"root manifest was skipped",
				)
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
	testutil.WriteFile(
		t,
		dir,
		"components/db/template.yaml",
		testutil.KustomizationYAML(
			"${APP}-db",
			"ns",
			"  path: ./does/not/matter\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 1h\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: real, namespace: ns}
`)
	s := store.New()
	l := New(s)
	n, err := loadForTest(t.Context(), t, l, dir)
	{
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	if n != 0 || len(l.Index.Records) != 1 {
		t.Errorf("admitted %d objects with records %v, want only the real ConfigMap indexed", n, l.Index.Records)
	}
	{
		got := s.GetObject(testutil.KustomizationID("ns", "${APP}-db"))
		assert.ErrorIf(t, got != nil, "Component-subtree resource should not be loaded; got %v", got)
	}
}

func TestLoader_SkipsKustomizeComponent_JSONForm(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "components/foo/Kustomization",
		`{"apiVersion":"kustomize.config.k8s.io/v1alpha1","kind":"Component"}`)
	testutil.WriteFile(
		t,
		dir,
		"components/foo/leak.yaml",
		testutil.KustomizationYAML(
			"${APP}-leak",
			"ns",
			"  path: ./irrelevant\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 1h\n",
		),
	)
	s := store.New()
	l := loaderFixture(s, nil)
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	got := s.GetObject(testutil.KustomizationID("ns", "${APP}-leak"))
	{
		assert.ErrorIf(t, got != nil, "Component subtree leaked a templated KS: %v", got)
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
	testutil.WriteFile(
		t,
		dir,
		"addons/ks.yaml",
		testutil.KustomizationYAML(
			"addons",
			"flux-system",
			"  path: ./addons\n  sourceRef: {kind: GitRepository, name: flux-system}\n  interval: 1h\n",
		),
	)
	s := store.New()
	l := loaderFixture(s, nil)
	{
		_, err := loadForTest(context.Background(), t, l, dir)
		assert.FatalIf(t, err != nil, "Load: %v", err)
	}
	got := s.GetObject(testutil.KustomizationID("flux-system", "addons"))
	{
		assert.ErrorIf(
			t,
			got == nil,
			"%s",
			"foreign 'kind: Component' (non-kustomize apiVersion) silently skipped sibling KS — Component gate fired without apiVersion check",
		)
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
			{
				assert.FatalIf(t, err != nil, "Load: %v", err)
			}
			if n != 0 || len(l.Index.Records) != len(tc.want) || len(retainedErrorsForTest(l)) != 0 {
				t.Fatalf(
					"generator scan: admitted=%d records=%v errors=%v, want %v",
					n,
					l.Index.Records,
					retainedErrorsForTest(l),
					tc.want,
				)
			}
			for _, id := range tc.want {
				{
					_, ok := l.Index.Lookup(id)
					assert.ErrorIf(t, !ok, "resource %v must still be indexed", id)
				}
			}
		})
	}
}

func testLoaderDiscoveryBootstrapScanSkipsCoveredDirCase(
	t *testing.T,
	testCaseInput *struct {
		name     string
		alias    bool
		listed   bool
		external bool
	},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/test/kustomization.yaml", "resources: [network]\n")
	resources := "resources: [ns.yaml, echo.yaml]\n# - ks.yaml\n"
	if testCaseInput.listed {
		resources = "resources: [ns.yaml, echo.yaml, ks.yaml]\n"
	}
	testutil.WriteFile(t, root, "apps/test/network/kustomization.yaml", resources)
	testutil.WriteFile(
		t,
		root,
		"apps/test/network/ns.yaml",
		"apiVersion: v1\nkind: Namespace\nmetadata: {name: network}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/test/network/echo.yaml",
		testutil.KustomizationYAML(
			"echo",
			"network",
			"  path: ./apps/base/echo\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"apps/test/network/ks.yaml",
		testutil.KustomizationYAML(
			"tunnel",
			"network",
			"  path: ./apps/base/tunnel\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n",
		),
	)
	st := store.New()
	l := loaderFixture(st, nil)
	l.SourceRoot = root
	parent := &manifest.Kustomization{Name: "cluster-apps", Namespace: "flux-system", Path: "apps/test"}
	if testCaseInput.alias {
		testutil.WriteFile(
			t,
			root,
			"flux/parent.yaml",
			testutil.KustomizationYAML(
				"cluster-apps",
				"flux-system",
				"  path: ./apps/test\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			),
		)
		location, err := gitrepo.Locate(t.Context(), root, root)
		{
			assert.NoError(t, err)
		}
		input, err := gittree.AdmitDirectory(t.Context(), location)
		assert.NoError(t, err)
		st.SetArtifact(manifest.BootstrapSourceID, input)
		view, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Source, Current: input.FS},
		)
		assert.NoError(t, err)
		source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system", URL: "file://" + root}
		st.AddObject(source)
		st.SetArtifact(
			source.Named(),
			&store.SourceArtifact{
				LocalRoot:  root,
				LocalPath:  filepath.Join(t.TempDir(), "alias"),
				InputAlias: true,
				FS:         view,
			},
		)
		_, err = loadForTest(t.Context(), t, l, filepath.Join(root, "flux"))
		assert.NoError(t, err)
		parent, _ = st.Get[*manifest.Kustomization](parent.Named())
		{
			assert.FatalIf(t, parent == nil, "%s", "bootstrap parent was not loaded")
		}
	} else {
		if testCaseInput.external {
			source := &manifest.GitRepository{
				Name:      "cluster",
				Namespace: "flux-system",
				URL:       "https://github.com/example/other",
			}
			st.AddObject(source)
			parent.SourceRef = kustomizev1.CrossNamespaceSourceReference{
				Kind:      manifest.KindGitRepository,
				Name:      source.Name,
				Namespace: source.Namespace,
			}
		}
		st.AddObject(parent)
	}
	sourceRoot, _, _ := SourceView(st, parent.SourceID(), root)
	_, err := loadForTest(t.Context(), t, l, filepath.Join(sourceRoot, parent.Path))
	assert.NoError(t, err)
	l.Admit(root)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	for _, child := range []struct {
		name string
		want bool
	}{{"echo", true}, {"tunnel", testCaseInput.listed}} {
		id := testutil.KustomizationID("network", child.name)
		{
			got := st.GetObject(
				id,
			) != nil
			assert.ErrorIf(t, got != (child.want && (testCaseInput.alias || testCaseInput.external)), "Store admission for %s = %v, want %v", child.name, got, child.want)
		}
		{
			_, got := l.Index.Lookup(id)
			assert.ErrorIf(t, got != child.want, "Index admission for %s = %v, want %v", child.name, got, child.want)
		}
	}
}
