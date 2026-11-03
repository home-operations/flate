package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// namespaceTransformer is the shared builtin NamespaceTransformer
// flatops uses (kubernetes/transformers/transformer.yaml): it writes
// spec/targetNamespace onto every Kustomization.
const namespaceTransformer = `apiVersion: builtin
kind: NamespaceTransformer
metadata:
  name: apply-ns
  namespace: .invalid
fieldSpecs:
  - path: metadata/name
    kind: Namespace
    create: true
  - path: spec/targetNamespace
    group: kustomize.toolkit.fluxcd.io
    kind: Kustomization
    create: true
`

// writeFlatopsOverlay lays down the flatops namespace-overlay shape: an
// overlay kustomization.yaml that pulls in a transformers/ subtree whose
// `namespace:` directive feeds the shared NamespaceTransformer.
func writeFlatopsOverlay(t *testing.T, root, ns string) {
	t.Helper()
	testutil.WriteFile(t, root, "apps/"+ns+"/kustomization.yaml", `resources:
  - ./namespace.yaml
  - ./`+ns+`/ks.yaml
transformers:
  - ./transformers
`)
	testutil.WriteFile(t, root, "apps/"+ns+"/transformers/kustomization.yaml", `namespace: `+ns+`
resources:
  - ../../../transformers
`)
	testutil.WriteFile(t, root, "transformers/kustomization.yaml", "resources:\n  - ./transformer.yaml\n")
	testutil.WriteFile(t, root, "transformers/transformer.yaml", namespaceTransformer)
}

func leafKS(name, path string) *manifest.Kustomization {
	return &manifest.Kustomization{Name: name, Path: path}
}

func TestAdmit_SelfProducedTransformerIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFlatopsOverlay(t, root, "apps")
	testutil.WriteFile(t, root, "apps/apps/namespace.yaml", "apiVersion: v1\nkind: Namespace\nmetadata: {name: apps}\n")
	const definition = "apps/apps/apps/ks.yaml"
	testutil.WriteFile(t, root, definition, testutil.KustomizationYAML("child", "", "  {path: ./leaf}\n"))
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: []\n")

	s := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system", Path: "./apps/apps"}
	child := leafKS("child", "./leaf")
	s.AddObject(parent)
	l := loaderFixture(s, nil)
	l.SourceRoot = root
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "apps/apps"))
		assert.NoError(t, err)
	}
	l.Admit(root)

	for _, tc := range []struct {
		name, namespace string
		want            bool
	}{
		{"transformed", "apps", true},
		{"empty", "", false},
		{"parent fallback", "flux-system", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmitSelfProducedTransformerIdentityCase(
				t,
				&testAdmitSelfProducedTransformerIdentityCaseInputs{s: &s, child: &child, l: &l, tc: &tc},
			)
		},
		)
	}
	t.Run("namespace prediction declines Component-only proof", func(t *testing.T) {
		t.Parallel()
		testAdmitSelfProducedTransformerIdentityNamespacePredictionDeclinesComponentOnlyProof(t)
	},
	)
}

func TestStampTransformerTargetNamespaces_NamespaceTransformer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, authored, target string
		namespaces             []string
		mounted                bool
	}{
		{name: "flatops lifecycle", authored: "flux-system", namespaces: []string{"gpu-operator"}},
		{name: "authored null target", authored: "flux-system", target: ", targetNamespace: null", namespaces: []string{"gpu-operator"}},
		{name: "authored empty target", authored: "flux-system", target: ", targetNamespace: ''", namespaces: []string{"gpu-operator"}},
		{name: "same identity", authored: "gpu-operator", namespaces: []string{"gpu-operator"}},
		{name: "mounted replacement", namespaces: []string{"red", "blue", ""}, mounted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStampTransformerTargetNamespacesNamespaceTransformerCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestStampTransformerTargetNamespaces_PlainNamespaceUntouched(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// A plain `namespace:` directive with NO NamespaceTransformer — the
	// existing metadata.namespace inheritance owns this; we must not
	// stamp targetNamespace.
	testutil.WriteFile(t, root, "apps/plain/kustomization.yaml", `namespace: media
resources:
  - ./app/ks.yaml
`)

	s := store.New()
	ks := leafKS("plain", "./apps/plain/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/plain/app/ks.yaml",
	}

	stampForTest(s, sourceFiles, root)

	got, _ := s.Get[*manifest.Kustomization](ks.Named())
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (no NamespaceTransformer)", got.TargetNamespace)
	}
}

func TestStampTransformerTargetNamespaces_TransformerDeclines(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, from, to string }{
		{"other kind", "kind: NamespaceTransformer", "kind: LabelTransformer"},
		{"unset only", "fieldSpecs:", "unsetOnly: true\nfieldSpecs:"},
		{"no creation", "kind: Kustomization\n    create: true", "kind: Kustomization\n    create: false"},
		{"foreign group", "group: kustomize.toolkit.fluxcd.io", "group: example.com"},
		{"foreign version", "kind: Kustomization", "kind: Kustomization\n    version: v2"},
		{"invalid subject mode", "fieldSpecs:", "setRoleBindingSubjects: invalid\nfieldSpecs:"},
		{"malformed trailing document", "fieldSpecs:", "fieldSpecs:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			writeFlatopsOverlay(t, root, "other")
			transformer := strings.Replace(namespaceTransformer, tc.from, tc.to, 1)
			if tc.name == "malformed trailing document" {
				transformer += "---\nbroken: [unclosed\n"
			}
			testutil.WriteFile(t, root, "transformers/transformer.yaml", transformer)
			s := store.New()
			ks := leafKS("other", "./apps/other/other/app")
			s.AddObject(ks)
			stampForTest(s, map[manifest.NamedResource]string{ks.Named(): "apps/other/other/ks.yaml"}, root)
			got, _ := s.Get[*manifest.Kustomization](ks.Named())
			if got.TargetNamespace != "" {
				t.Errorf("TargetNamespace=%q want empty (conditional or invalid transformer)", got.TargetNamespace)
			}
		})
	}
}

func TestStampTransformerTargetNamespaces_DeepestOverlayWins(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// Outer overlay injects "outer"; an inner overlay nested under it
	// injects "inner". The KS lives under the inner overlay, so inner
	// (the deepest enclosing overlay) wins.
	testutil.WriteFile(t, root, "apps/outer/kustomization.yaml", `resources:
  - ./inner
transformers:
  - ./transformers
`)
	testutil.WriteFile(t, root, "apps/outer/transformers/kustomization.yaml", `namespace: outer
resources:
  - ../../../transformers
`)
	testutil.WriteFile(t, root, "apps/outer/inner/kustomization.yaml", `resources:
  - ./app/ks.yaml
transformers:
  - ./transformers
`)
	testutil.WriteFile(t, root, "apps/outer/inner/transformers/kustomization.yaml", `namespace: inner
resources:
  - ../../../../transformers
`)
	testutil.WriteFile(t, root, "transformers/kustomization.yaml", "resources:\n  - ./transformer.yaml\n")
	testutil.WriteFile(t, root, "transformers/transformer.yaml", namespaceTransformer)

	s := store.New()
	ks := leafKS("app", "./apps/outer/inner/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/outer/inner/app/ks.yaml",
	}

	stampForTest(s, sourceFiles, root)

	got, _ := s.Get[*manifest.Kustomization](ks.Named())
	if got.TargetNamespace != "inner" {
		t.Errorf("TargetNamespace=%q want inner (deepest overlay)", got.TargetNamespace)
	}
}

// nsReplacementRule is the home-operations replacement that copies a
// Namespace's name into every Flux Kustomization's spec.targetNamespace
// (kubernetes/components/replacements/ks.yaml). The file is a YAML list.
const nsReplacementRule = `- source:
    kind: Namespace
    fieldPath: metadata.name
  targets:
    - select:
        kind: Kustomization
        group: kustomize.toolkit.fluxcd.io
      fieldPaths:
        - spec.targetNamespace
      options:
        create: true
`

// stampReplacementsKS is the shared harness for the replacements-pattern
// tests: store a leaf KS under overlayDir/app, run the stamp, return the
// resolved TargetNamespace.
func stampReplacementsKS(t *testing.T, root, overlayDir, overlayKustomization string) *manifest.Kustomization {
	t.Helper()
	testutil.WriteFile(t, root, overlayDir+"/kustomization.yaml", overlayKustomization)
	s := store.New()
	ks := leafKS("app", "./"+overlayDir+"/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): overlayDir + "/app/ks.yaml",
	}
	stampForTest(s, sourceFiles, root)
	got, _ := s.Get[*manifest.Kustomization](ks.Named())
	return got
}

func TestStampTransformerTargetNamespaces_Replacements(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, from, to, want string
		file                 bool
	}{
		{name: "inline", want: "storage"},
		{name: "file", file: true, want: "storage"},
		{name: "no namespace", from: "namespace: storage\n"},
		{name: "wrong field", from: "spec.targetNamespace", to: "spec.path"},
		{name: "wrong source kind", from: "kind: Namespace", to: "kind: ConfigMap"},
		{name: "source group", from: "kind: Namespace", to: "kind: Namespace\n      group: example.com"},
		{name: "source version", from: "kind: Namespace", to: "kind: Namespace\n      version: v2"},
		{name: "source name", from: "kind: Namespace", to: "kind: Namespace\n      name: foreign"},
		{name: "source namespace", from: "kind: Namespace", to: "kind: Namespace\n      namespace: foreign"},
		{name: "source options", from: "kind: Namespace", to: "kind: Namespace\n      options: {delimiter: '-', index: 1}"},
		{name: "source field", from: "fieldPath: metadata.name", to: "fieldPath: metadata.labels.namespace"},
		{name: "source value", from: "- source:", to: "- sourceValue: foreign\n    source:"},
		{name: "target reject", from: "        fieldPaths:", to: "        reject: [{name: app}]\n        fieldPaths:"},
		{name: "target version", from: "kind: Kustomization", to: "kind: Kustomization\n          version: v2"},
		{name: "target name", from: "kind: Kustomization", to: "kind: Kustomization\n          name: foreign"},
		{name: "target namespace", from: "kind: Kustomization", to: "kind: Kustomization\n          namespace: foreign"},
		{name: "target labels", from: "kind: Kustomization", to: "kind: Kustomization\n          labelSelector: app=foreign"},
		{name: "target annotations", from: "kind: Kustomization", to: "kind: Kustomization\n          annotationSelector: app=foreign"},
		{name: "target no creation", from: "create: true", to: "create: false"},
		{name: "target no options", from: "        options:\n          create: true\n", to: ""},
		{name: "target options", from: "create: true", to: "create: true\n          delimiter: '-'"},
		{name: "no targets", from: "    targets:", to: "    unused:"},
		{name: "file and source", file: true, from: "- path: ../../components/replacements/ks.yaml", to: "- path: ../../components/replacements/ks.yaml\n    source: {kind: Namespace}"},
		{name: "file and targets", file: true, from: "- path: ../../components/replacements/ks.yaml", to: "- path: ../../components/replacements/ks.yaml\n    targets: [{select: {kind: Kustomization}}]"},
		{name: "file and source value", file: true, from: "- path: ../../components/replacements/ks.yaml", to: "- path: ../../components/replacements/ks.yaml\n    sourceValue: foreign"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			rule := strings.TrimSuffix(nsReplacementRule, "\n")
			if tc.file {
				testutil.WriteFile(t, root, "components/replacements/ks.yaml", nsReplacementRule)
				rule = "- path: ../../components/replacements/ks.yaml"
			}
			overlay := "namespace: storage\nreplacements:\n  " + strings.ReplaceAll(
				rule,
				"\n",
				"\n  ",
			) + "\nresources: [./app/ks.yaml]\n"
			if tc.from != "" {
				overlay = strings.Replace(overlay, tc.from, tc.to, 1)
			}
			got := stampReplacementsKS(t, root, "apps/storage", overlay)
			if got.TargetNamespace != tc.want {
				t.Fatalf("TargetNamespace=%q want %q", got.TargetNamespace, tc.want)
			}
		})
	}
}

// A malformed overlay kustomization.yaml (namespace must be a scalar, not a
// map) parses to ok=false and yields no stamp — best-effort, never a panic.
func TestStampTransformerTargetNamespaces_MalformedOverlayIgnored(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	got := stampReplacementsKS(
		t,
		root,
		"apps/storage",
		"namespace:\n  unexpected: map\nresources:\n  - ./app/ks.yaml\n",
	)
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (malformed overlay)", got.TargetNamespace)
	}
}

// A `replacements: {path:}` whose external file is malformed (a map where a
// list of rules is expected) yields no stamp rather than a panic.
func TestStampTransformerTargetNamespaces_MalformedReplacementsFileIgnored(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "components/replacements/ks.yaml", "this: is not a list\n")
	got := stampReplacementsKS(t, root, "apps/storage", `namespace: storage
replacements:
  - path: ../../components/replacements/ks.yaml
resources:
  - ./app/ks.yaml
`)
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (malformed replacements file)", got.TargetNamespace)
	}
}

// A KS whose ancestor dirs carry no kustomization.yaml at all resolves to no
// injected namespace (the walk terminates cleanly at the repo root).
func TestStampTransformerTargetNamespaces_NoEnclosingOverlay(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s := store.New()
	ks := leafKS("bare", "./apps/bare/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{ks.Named(): "apps/bare/app/ks.yaml"}
	stampForTest(s, sourceFiles, root)
	got, _ := s.Get[*manifest.Kustomization](ks.Named())
	if got.TargetNamespace != "" {
		t.Errorf("TargetNamespace=%q want empty (no enclosing overlay)", got.TargetNamespace)
	}
}

func TestResolve_TransformerRejections(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, base, ref string }{
		{"empty", "apps/x", ""},
		{"remote scheme", "apps/x", "https://example.com/repo//base?ref=v1"},
		{"absolute", "apps/x", "/etc/passwd"},
		{"escapes repo root", "apps/x", "../../../../../../etc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			{
				got, ok := kustpath.Resolve(tc.base, tc.ref, kustpath.Transformer)
				assert.ErrorIf(t, ok, "kustpath.Resolve(%q,%q)=(%q,true), want rejected", tc.base, tc.ref, got)
			}
			{
				got, ok := kustomizeinput.Local(".", tc.base, tc.ref)
				assert.ErrorIf(t, ok, "kustomizeinput.Local(%q,%q)=(%q,true), want rejected", tc.base, tc.ref, got)
			}
		})
	}
	{
		got, ok := kustpath.Resolve("apps/x", "../shared", kustpath.Transformer)
		assert.ErrorIf(
			t,
			!ok || got != "apps/shared",
			"kustpath.Resolve(apps/x,../shared)=(%q,%v), want (apps/shared,true)",
			got,
			ok,
		)
	}
	{
		got, ok := kustomizeinput.Local(".", "apps/x", "../shared")
		assert.ErrorIf(
			t,
			!ok || got != "apps/shared",
			"kustomizeinput.Local(apps/x,../shared)=(%q,%v), want (apps/shared,true)",
			got,
			ok,
		)
	}
}

func TestSubtreeHasNamespaceTransformer_DepthCapAndCycle(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, depth := range []int{transformerScanMaxDepth, transformerScanMaxDepth + 1} {
		t.Run(fmt.Sprintf("depth %d", depth), func(t *testing.T) {
			t.Parallel()
			tempDirRoot := t.TempDir()
			for i := range depth {
				testutil.WriteFile(
					t,
					tempDirRoot,
					fmt.Sprintf("d%d/kustomization.yaml", i),
					fmt.Sprintf("resources: [../d%d]\n", i+1),
				)
			}
			testutil.WriteFile(t, tempDirRoot, fmt.Sprintf("d%d/kustomization.yaml", depth), "resources: [transformer.yaml]\n")
			testutil.WriteFile(t, tempDirRoot, fmt.Sprintf("d%d/transformer.yaml", depth), namespaceTransformer)
			got := readerForTest(tempDirRoot).subtreeHasNamespaceTransformer(tempDirRoot, "d0", map[string]struct{}{})
			{
				assert.FatalIf(t, got != (depth == transformerScanMaxDepth), "proof at depth %d = %t", depth, got)
			}
		})
	}
	testutil.WriteFile(t, root, "component/kustomization.yaml", "components: [nested]\n")
	testutil.WriteFile(
		t,
		root,
		"component/nested/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [transformer.yaml]\n",
	)
	testutil.WriteFile(t, root, "component/nested/transformer.yaml", namespaceTransformer)
	{
		assert.FatalIf(
			t,
			readerForTest(root).subtreeHasNamespaceTransformer(root, "component", map[string]struct{}{}),
			"%s",
			"Component edge supplied namespace proof",
		)
	}

	// A reference cycle a → b → a must terminate (visited set) and return false.
	testutil.WriteFile(t, root, "cyc/a/kustomization.yaml", "resources:\n  - ../b\n")
	testutil.WriteFile(t, root, "cyc/b/kustomization.yaml", "resources:\n  - ../a\n")
	{
		assert.ErrorIf(
			t,
			readerForTest(root).subtreeHasNamespaceTransformer(root, "cyc/a", map[string]struct{}{}),
			"%s",
			"want false on a reference cycle (must not loop)",
		)
	}
}

// Deepest enclosing overlay wins for the replacements pattern too.
func TestStampTransformerTargetNamespaces_ReplacementsDeepestOverlayWins(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "components/replacements/ks.yaml", nsReplacementRule)
	testutil.WriteFile(t, root, "apps/outer/kustomization.yaml", `namespace: outer
replacements:
  - path: ../../components/replacements/ks.yaml
resources:
  - ./inner
`)
	testutil.WriteFile(t, root, "apps/outer/inner/kustomization.yaml", `namespace: inner
replacements:
  - path: ../../../components/replacements/ks.yaml
resources:
  - ./app/ks.yaml
`)
	s := store.New()
	ks := leafKS("app", "./apps/outer/inner/app/workload")
	s.AddObject(ks)
	sourceFiles := map[manifest.NamedResource]string{
		ks.Named(): "apps/outer/inner/app/ks.yaml",
	}
	stampForTest(s, sourceFiles, root)
	got, _ := s.Get[*manifest.Kustomization](ks.Named())
	if got.TargetNamespace != "inner" {
		t.Errorf("TargetNamespace=%q want inner (deepest overlay)", got.TargetNamespace)
	}
}

func TestAdmit_UnloadedAncestorNamespace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFlatopsOverlay(t, root, "outside")
	testutil.WriteFile(
		t,
		root,
		"apps/outside/selected/ks.yaml",
		testutil.KustomizationYAML("selected", "apps", "  path: ./leaf\n"),
	)
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), filepath.Join(root, "apps/outside/selected"))
	{
		assert.NoError(t, err)
	}
	l.Admit(root)
	owners := l.Kustomizations()
	{
		assert.FatalIf(t, len(owners) != 1 || owners[0].Obj.(*manifest.Kustomization).TargetNamespace != "" ||
			owners[0].Obj.(*manifest.Kustomization).Namespace != "apps", "unloaded ancestor injected namespace: %+v", owners)
	}
}

type testAdmitSelfProducedTransformerIdentityCaseInputs struct {
	s     **store.Store
	child **manifest.Kustomization
	l     **Loader
	tc    *struct {
		name      string
		namespace string
		want      bool
	}
}

func testAdmitSelfProducedTransformerIdentityCase(
	t *testing.T,
	testCaseInput *testAdmitSelfProducedTransformerIdentityCaseInputs,
) {
	t.Helper()
	const definition = "apps/apps/apps/ks.yaml"

	id := testutil.KustomizationID(testCaseInput.tc.namespace, (*testCaseInput.child).Name)
	if _, ok := (*testCaseInput.s).Get[*manifest.Kustomization](id); ok {
		t.Errorf("stored identity %s present=%t, want %t", id, ok, testCaseInput.tc.want)
	}
	record, ok := (*testCaseInput.l).Index.Records[id]
	if ok != testCaseInput.tc.want {
		t.Errorf("indexed identity %s present=%t, want %t", id, ok, testCaseInput.tc.want)
	}
	{
		assert.ErrorIf(t, testCaseInput.tc.want && record.File != definition, "identity %s definition=%q, want %q", id, record.File, definition)
	}
}

func testAdmitSelfProducedTransformerIdentityNamespacePredictionDeclinesComponentOnlyProof(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [ks.yaml]\ntransformers: [proof]\n")
	testutil.WriteFile(t, root, "entry/ks.yaml", testutil.KustomizationYAML("child", "", "  path: ./leaf\n"))
	testutil.WriteFile(t, root, "entry/proof/kustomization.yaml", "namespace: forbidden\ncomponents: [component]\n")
	testutil.WriteFile(
		t,
		root,
		"entry/proof/component/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [transformer.yaml]\n",
	)
	testutil.WriteFile(t, root, "entry/proof/component/transformer.yaml", namespaceTransformer)
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: []\n")
	st := store.New()
	st.AddObject(&manifest.Kustomization{Name: "parent", Namespace: "flux-system", Path: "./entry"})
	l := loaderFixture(st, nil)
	l.SourceRoot = root
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry"))
		assert.NoError(t, err)
	}
	l.Admit(root)
	{
		_, found := l.Index.Lookup(testutil.KustomizationID("forbidden", "child"))
		assert.FatalIf(t, found, "%s", "scope namespace prediction accepted a Component-only proof")
	}
	record, found := l.Index.Lookup(testutil.KustomizationID("", "child"))
	{
		assert.FatalIf(
			t,
			!found || record.File != "entry/ks.yaml",
			"%s",
			"declined prediction lost the authored child evidence",
		)
	}
}

func testStampTransformerTargetNamespacesNamespaceTransformerCase(
	t *testing.T,
	testCaseInput *struct {
		name       string
		authored   string
		target     string
		namespaces []string
		mounted    bool
	},
) {
	t.Helper()

	root, local := t.TempDir(), t.TempDir()
	st := store.New()
	source := &manifest.GitRepository{Name: "view"}
	st.AddObject(source)
	l := loaderFixture(st, nil)
	l.SourceRoot = root
	doc := testutil.KustomizationYAML(
		"gpu-operator",
		fmt.Sprintf("%q", testCaseInput.authored),
		"  {path: ./output, sourceRef: {kind: GitRepository, name: view}"+testCaseInput.target+"}\n",
	) + "---\n" + testutil.KustomizationYAML(
		"explicit",
		fmt.Sprintf("%q", testCaseInput.authored),
		"  {path: ./explicit, targetNamespace: authored, sourceRef: {kind: GitRepository, name: view}}\n",
	)
	check := func(ns string) {
		for _, name := range []string{"gpu-operator", "explicit"} {
			id := testutil.KustomizationID(ns, name)
			want := ns
			if name == "explicit" {
				want = "authored"
			}
			record, indexed := l.Index.Lookup(id)
			ks, ok := l.IndexedObject(id, record).Obj.(*manifest.Kustomization)
			{
				assert.FatalIf(
					t,
					!indexed || !ok || ks.TargetNamespace != want,
					"%s declaration = %+v, want targetNamespace %q",
					id,
					ks,
					want,
				)
			}
			projected, err := ks.RenderDocument()
			{
				assert.NoError(t, err)
			}
			target, _ := projected["spec"].(map[string]any)["targetNamespace"].(string)
			{
				assert.FatalIf(t, target != want, "%s projected namespace = %v, %v", id, projected, err)
			}
			for _, staleNS := range []string{testCaseInput.authored, "red", "blue"} {
				if staleNS == ns {
					continue
				}
				stale := testutil.KustomizationID(staleNS, name)
				_, indexed := l.Index.Lookup(stale)
				{
					assert.FatalIf(t, st.GetObject(stale) != nil || indexed, "retained stale %s", stale)
				}
			}
		}
	}
	testStampTransformerTargetNamespacesNamespaceTransformerCaseCompletion(t, &testStampTransformerTargetNamespacesNamespaceTransformerCaseCompletionState{testCaseInput: &testCaseInput, root: &root, local: &local, st: &st, source: &source, l: &l, doc: &doc, check: &check})
}

type testStampTransformerTargetNamespacesNamespaceTransformerCaseCompletionState struct {
	testCaseInput **struct {
		name       string
		authored   string
		target     string
		namespaces []string
		mounted    bool
	}
	root   *string
	local  *string
	st     **store.Store
	source **manifest.GitRepository
	l      **Loader
	doc    *string
	check  *func(ns string)
}

func testStampTransformerTargetNamespacesNamespaceTransformerCaseCompletion(t *testing.T, scope *testStampTransformerTargetNamespacesNamespaceTransformerCaseCompletionState) {
	t.Helper()
	for version, ns := range (*scope.testCaseInput).namespaces {
		if (*scope.testCaseInput).mounted {
			input := fstest.MapFS{
				"apps/gpu-operator/kustomization.yaml":   {Data: []byte("resources: [gpu-operator/ks.yaml]\n")},
				"apps/gpu-operator/gpu-operator/ks.yaml": {Data: []byte((*scope.doc))},
			}
			if version == 0 {
				input["apps/gpu-operator/kustomization.yaml"].Data = []byte(
					"resources: [gpu-operator/ks.yaml, removed.yaml, bad.yaml]\n",
				)
				input["apps/gpu-operator/removed.yaml"] = &fstest.MapFile{
					Data: []byte(testutil.KustomizationYAML("removed", "", "  {path: ./output}\n")),
				}
				input["apps/gpu-operator/bad.yaml"] = &fstest.MapFile{Data: []byte("metadata: [\n")}
			}
			if ns != "" {
				input["apps/gpu-operator/kustomization.yaml"].Data = append(
					input["apps/gpu-operator/kustomization.yaml"].Data,
					[]byte("transformers: [transformers]\n")...)
				input["apps/gpu-operator/transformers/kustomization.yaml"] = &fstest.MapFile{
					Data: []byte("namespace: " + ns + "\nresources: [../../../transformers]\n"),
				}
				input["transformers/kustomization.yaml"] = &fstest.MapFile{
					Data: []byte("resources: [transformer.yaml]\n"),
				}
				input["transformers/transformer.yaml"] = &fstest.MapFile{Data: []byte(namespaceTransformer)}
			}
			(*scope.st).SetArtifact(
				(*scope.source).Named(),
				&store.SourceArtifact{
					Key:       fmt.Sprintf("view-%d", version),
					LocalRoot: (*scope.root),
					LocalPath: (*scope.local),
					FS:        input,
				},
			)
		} else {
			(*scope.local) = (*scope.root)
			writeFlatopsOverlay(t, (*scope.root), ns)
			testutil.WriteFile(t, (*scope.root), "apps/gpu-operator/gpu-operator/ks.yaml", (*scope.doc))
			(*scope.st).SetArtifact(
				(*scope.source).Named(),
				&store.SourceArtifact{LocalRoot: (*scope.root), LocalPath: (*scope.local), FS: os.DirFS((*scope.local))},
			)
		}
		{
			_, err := (*scope.l).Load(t.Context(), filepath.Join((*scope.local), "apps/gpu-operator"))
			assert.NoError(t, err)
		}
		if (*scope.testCaseInput).mounted {
			removedNS := "red"
			if version == 0 {
				removedNS = ""
			}
			removed := testutil.KustomizationID(removedNS, "removed")
			_, indexed := (*scope.l).Index.Lookup(removed)
			if (*scope.st).GetObject(removed) != nil || indexed != (version == 0) {
				t.Fatalf(
					"removed declaration: store=%t index=%t version=%d",
					(*scope.st).GetObject(removed) != nil,
					indexed,
					version,
				)
			}
			if (len(retainedErrorsForTest((*scope.l))) != 0) != (version == 0) {
				t.Fatalf("decode errors after mount version %d: %v", version, retainedErrorsForTest((*scope.l)))
			}
		}
		(*scope.l).Admit((*scope.root))
		(*scope.check)(ns)
	}
	{
		err := (*scope.l).FinalizeIndex(t.Context(), (*scope.root), true)
		assert.NoError(t, err)
	}
	(*scope.check)((*scope.testCaseInput).namespaces[len((*scope.testCaseInput).namespaces)-1])
	if (*scope.testCaseInput).mounted {
		(*scope.st).SetArtifact((*scope.source).Named(), &store.SourceArtifact{Key: "removed", LocalRoot: (*scope.root), LocalPath: (*scope.local)})
		(*scope.l).Admit((*scope.root))
		for _, name := range []string{"gpu-operator", "explicit"} {
			id := testutil.KustomizationID("", name)
			_, indexed := (*scope.l).Index.Lookup(id)
			{
				assert.FatalIf(t, (*scope.st).GetObject(id) != nil || indexed, "%s survived mount removal", id)
			}
		}
	}
}
