package loader

import (
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_ImplicitAndTerminalInputs(t *testing.T) {
	t.Parallel()
	for _, overlay := range []string{"entry", "entry/nested", "entry/deep/nested"} {
		t.Run(overlay, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			base, _ := filepath.Rel(overlay, "shared")
			testutil.WriteFile(t, root, "entry/README.md", "not a manifest\n")
			testutil.WriteFile(t, root, "entry/sibling.json", `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"sibling"}}`)
			testutil.WriteFile(t, root, overlay+"/kustomization.yaml", "resources: ["+filepath.ToSlash(base)+"]\n")
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\npatches: [{path: ../inputs/patch}]\nconfigMapGenerator: [{name: values, files: [key=../inputs/value], env: ../inputs/env}]\nreplacements: [{path: ../inputs/missing}]\n")
			testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n")
			testutil.WriteFile(t, root, "inputs/patch", "- op: add\n  path: /data\n  value: {key: changed}\n")
			testutil.WriteFile(t, root, "inputs/value", testutil.KustomizationYAML("terminal", "apps", "  path: ./hidden\n"))
			testutil.WriteFile(t, root, "inputs/env", "KEY=value\n")
			s := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
			s.AddObject(owner)
			l := New(s)
			l.SourceRoot = root
			admitForTest(s, root)
			if _, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry")); err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			if overlay != "entry" {
				assert.Diff(t, l.Index.Readers["entry/sibling.json"], []manifest.NamedResource{owner.Named()})
				if len(l.Index.Readers["entry/README.md"]) != 0 {
					t.Fatal("non-manifest implicit sibling became a reader")
				}
			}
			for _, file := range []string{overlay + "/kustomization.yaml", "shared/kustomization.yaml", "shared/cm.yaml", "inputs/patch", "inputs/value", "inputs/env", "inputs/missing"} {
				assert.Diff(t, l.Index.Readers[file], []manifest.NamedResource{owner.Named()})
				if !slices.Contains(ownersForTest(l.Index, file), owner.Named()) {
					t.Errorf("owner missing for %s", file)
				}
				filter := change.NewFilter(change.NewSet([]string{file}), l.Index, s)
				if !filter.ShouldReconcile(owner.Named()) {
					t.Errorf("changed %s did not select owner", file)
				}
			}
			if id := testutil.KustomizationID("apps", "terminal"); s.GetObject(id) != nil || len(l.Index.Records[id].Productions) != 0 {
				t.Fatal("terminal input became an object or producer")
			}
			if change.NewFilter(change.NewSet([]string{"inputs/unrelated"}), l.Index, s).ShouldReconcile(owner.Named()) {
				t.Fatal("unrelated input selected owner")
			}
		})
	}
}

func TestFinalizeIndex_DuplicateDocumentKeys(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [../shared]\nnamespace: one\nnamespace: two\n")
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\npatches: [{path: ../inputs/ignored, path: ../inputs/patch}]\n")
	testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n")
	testutil.WriteFile(t, root, "inputs/ignored", "[]\n")
	testutil.WriteFile(t, root, "inputs/patch", "- op: add\n  path: /data\n  value: {key: changed}\n")
	testutil.WriteFile(t, root, "alias/kustomization.yaml", "namespace: one\nx: &namespace y\n*namespace: z\n")
	testutil.WriteFile(t, root, "merge/kustomization.yaml", "a: &a {namespace: one}\nb: &b {namePrefix: pre-}\n<<: *a\n<<: *b\n")
	s := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
	s.AddObject(owner)
	l := New(s)
	l.SourceRoot = root
	admitForTest(s, root)
	_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry"))
	assert.NoError(t, err)
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"shared/kustomization.yaml", "shared/cm.yaml", "inputs/patch"} {
		assert.Diff(t, l.Index.Readers[file], []manifest.NamedResource{owner.Named()})
		if !slices.Contains(ownersForTest(l.Index, file), owner.Named()) {
			t.Errorf("owner missing for %s", file)
		}
	}
	if !change.NewFilter(change.NewSet([]string{"inputs/patch"}), l.Index, s).ShouldReconcile(owner.Named()) {
		t.Error("changed terminal patch did not select owner")
	}
	doc, _ := l.documents.read(filepath.Join(root, "entry"))
	if doc == nil {
		t.Fatal("duplicate keys discarded the typed document")
	}
	assert.Equal(t, doc.Namespace, "two")
	doc, _ = l.documents.read(filepath.Join(root, "shared"))
	if doc == nil || len(doc.Patches) != 1 {
		t.Fatal("nested duplicate keys discarded the typed patch")
	}
	assert.Equal(t, doc.Patches[0].Path, "../inputs/patch")
	if len(l.Index.Readers["inputs/ignored"]) != 0 || slices.Contains(ownersForTest(l.Index, "inputs/ignored"), owner.Named()) {
		t.Error("discarded duplicate patch gained a reader or owner")
	}
	doc, _ = l.documents.read(filepath.Join(root, "alias"))
	if doc == nil {
		t.Fatal("alias key discarded the typed document")
	}
	assert.Equal(t, doc.Namespace, "one")
	doc, _ = l.documents.read(filepath.Join(root, "merge"))
	if doc == nil {
		t.Fatal("duplicate merge keys discarded a typed document")
	}
	assert.Equal(t, doc.Namespace, "one")
	assert.Equal(t, doc.NamePrefix, "pre-")
}

func TestGraphWalker_ImplicitMountedBoundary(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	documents := documentReader{
		cache: map[string]document{},
		views: map[string]fs.FS{
			root: fstest.MapFS{
				"entry/local.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: local}\n")},
				"entry/nested/decoy.yaml": {Data: []byte("decoy")},
			},
			filepath.Join(root, "entry/nested"): fstest.MapFS{
				"private.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: private}\n")},
			},
		},
	}
	walker := graphWalker{documents: documents, visited: map[graphContext]struct{}{}}
	var visits []string
	walker.walk(graphVisit{root: root, dir: "entry"}, func(v graphVisit) bool {
		visits = append(visits, v.coordinate)
		return true
	})
	assert.Diff(t, visits, []string{"entry/local.yaml"})
}

func TestFinalizeIndex_ConfigurationBoundaries(t *testing.T) {
	t.Parallel()
	for _, mounted := range []bool{false, true} {
		name := "outside root"
		if mounted {
			name = "cross mount"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			root := filepath.Join(parent, "source")
			input := fstest.MapFS{
				"source/apps/kustomization.yaml": {Data: []byte("generators: [../../foreign/plugin.yaml]\n")},
				"foreign/plugin.yaml":            {Data: []byte("apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: foreign}\nfiles: [private.txt]\n")},
			}
			if mounted {
				root = parent
			}
			s := store.New()
			path := "./apps"
			if mounted {
				path = "./source/apps"
			}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: path}
			s.AddObject(owner)
			s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: parent, LocalRoot: parent, FS: input})
			if mounted {
				source := &manifest.GitRepository{Name: "foreign", Namespace: "apps"}
				s.AddObject(source)
				s.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: filepath.Join(parent, "foreign"), FS: fstest.MapFS{"plugin.yaml": input["foreign/plugin.yaml"]}})
			}
			l := New(s)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			warnings := s.Warnings()
			if len(warnings) != 1 || warnings[0].Resource != owner.Named() || warnings[0].Category != manifest.WarnIncompleteInputCoverage || !slices.ContainsFunc(warnings[0].Detail, func(detail string) bool {
				return strings.Contains(detail, "configuration reference cannot be inspected locally")
			}) {
				t.Fatalf("missing source-boundary uncertainty: %+v", warnings)
			}
			for _, file := range []string{"../foreign/plugin.yaml", "foreign/plugin.yaml", "source/apps/private.txt", "apps/private.txt"} {
				if len(l.Index.Readers[file]) != 0 {
					t.Errorf("foreign configuration gained attribution: %s", file)
				}
			}
			if id := (manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "foreign"}); len(l.Index.Records[id].Productions) != 0 || s.GetObject(id) != nil {
				t.Fatal("foreign configuration admitted a producer")
			}
		})
	}
}

func TestFinalizeIndex_ConfigurationInvokingLoader(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "configs/kustomization.yaml", "namespace: wrappers\nresources: [nested]\n")
	testutil.WriteFile(t, root, "configs/nested/kustomization.yaml", "resources: [../plugin.yaml]\ncomponents: [../component]\n")
	testutil.WriteFile(t, root, "configs/component/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [plugin.yaml]\n")
	testutil.WriteFile(t, root, "configs/component/plugin.yaml", "apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: nested}\nenvs: [extra.env]\n")
	testutil.WriteFile(t, root, "configs/plugin.yaml", "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [value.txt]\n")
	s := store.New()
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(t, root, name+"/kustomization.yaml", "resources: [../configs/plugin.yaml]\ngenerators: [../configs]\n")
		testutil.WriteFile(t, root, name+"/value.txt", name)
		testutil.WriteFile(t, root, name+"/extra.env", "VALUE="+name)
		s.AddObject(&manifest.Kustomization{Name: name, Namespace: "apps", Path: "./" + name})
	}
	testutil.WriteFile(t, root, "combined/kustomization.yaml", "resources: [../a, ../b, dual.yaml]\ngenerators: [dual.yaml]\n")
	testutil.WriteFile(t, root, "combined/dual.yaml", "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: dual}\nfiles: [dual.txt]\n")
	testutil.WriteFile(t, root, "combined/dual.txt", "dual input")
	combined := &manifest.Kustomization{Name: "combined", Namespace: "apps", Path: "./combined"}
	s.AddObject(combined)
	testutil.WriteFile(t, root, "ordinary/kustomization.yaml", "resources: [../configs/plugin.yaml]\n")
	s.AddObject(&manifest.Kustomization{Name: "ordinary", Namespace: "apps", Path: "./ordinary"})
	l := New(s)
	idx := rendererFixture(t, l, root)
	for _, name := range []string{"a", "b"} {
		assert.Diff(t, idx.Readers[name+"/value.txt"], []manifest.NamedResource{testutil.KustomizationID("apps", name), combined.Named()})
		assert.Diff(t, idx.Readers[name+"/extra.env"], []manifest.NamedResource{testutil.KustomizationID("apps", name), combined.Named()})
	}
	assert.Diff(t, idx.Readers["combined/dual.txt"], []manifest.NamedResource{combined.Named()})
	if len(idx.Readers["configs/value.txt"]) != 0 || len(idx.Readers["configs/nested/value.txt"]) != 0 || len(idx.Readers["ordinary/value.txt"]) != 0 || len(s.Warnings()) != 0 {
		t.Fatalf("configuration used its own loader or lost namespace-wrapper proof: %+v", s.Warnings())
	}
	if _, exists := idx.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "generated", Namespace: "wrappers"}); exists {
		t.Fatal("configuration content became a generated object")
	}
}

func TestFinalizeIndex_NestedConfigurationLoader(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, transformer string
		warn              bool
	}{
		{"metadata", "kind: NamespaceTransformer\nnamespace: apps\n", false},
		{"operand", "kind: PatchTransformer\npath: operand-patch\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "generators: [../config]\n")
			testutil.WriteFile(t, root, "config/kustomization.yaml", "resources: [plugin.yaml]\ntransformers: [nested.yaml]\n")
			testutil.WriteFile(t, root, "config/plugin.yaml", "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [value.txt]\n")
			testutil.WriteFile(t, root, "config/nested.yaml", "apiVersion: builtin\nmetadata: {name: nested}\n"+tc.transformer)
			testutil.WriteFile(t, root, "config/operand-patch", "[]\n")
			testutil.WriteFile(t, root, "apps/value.txt", "value")
			s := store.New()
			owner := &manifest.Kustomization{Name: "apps", Namespace: "apps", Path: "./apps"}
			s.AddObject(owner)
			idx := rendererFixture(t, New(s), root)
			assert.Diff(t, idx.Readers["apps/value.txt"], []manifest.NamedResource{owner.Named()})
			if tc.warn {
				assert.Diff(t, idx.Readers["config/operand-patch"], []manifest.NamedResource{owner.Named()})
				if len(s.Warnings()) != 1 {
					t.Fatalf("nested operand rewrite must warn: %+v", s.Warnings())
				}
			} else if len(s.Warnings()) != 0 {
				t.Fatalf("metadata-only nested transformer warned: %+v", s.Warnings())
			}
			if len(idx.Readers["apps/operand-patch"]) != 0 {
				t.Fatal("nested invocation retained the outer plugin loader")
			}
		})
	}
}

func TestLoader_InputRolesAndAdmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, patch, terminal string }{
		{"modern", "patches: [{path: terminal.yaml}]", "apps/terminal.yaml"},
		{"strategic", "patchesStrategicMerge: [terminal.yaml]", "apps/terminal.yaml"},
		{"modern outside", "patches: [{path: ../inputs/terminal.yaml}]", "inputs/terminal.yaml"},
		{"strategic outside", "patchesStrategicMerge: [../inputs/terminal.yaml]", "inputs/terminal.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: projected\nresources: [dual.yaml]\nconfigMapGenerator: [{name: data, files: [dual.yaml]}]\n"+tc.patch+"\ngenerators: [configuration.yaml, configuration]\n")
			testutil.WriteFile(t, root, "apps/dual.yaml", testutil.KustomizationYAML("dual", "authored", "  path: ./child\n"))
			testutil.WriteFile(t, root, tc.terminal, testutil.KustomizationYAML("terminal", "authored", "  path: ./hidden\n"))
			testutil.WriteFile(t, root, "apps/configuration.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: hidden-config, namespace: authored}\nspec: {path: ./hidden}\n---\napiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: config}\nspec: {target: {name: hidden}, data: [{secretKey: KEY, remoteRef: {key: hidden}}]}\n")
			testutil.WriteFile(t, root, "apps/configuration/kustomization.yaml", "namespace: configuration-only\nresources: [flux.yaml, external.yaml]\n")
			testutil.WriteFile(t, root, "apps/configuration/flux.yaml", testutil.KustomizationYAML("directory-config", "authored", "  path: ./hidden\n"))
			testutil.WriteFile(t, root, "apps/configuration/external.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: directory-config}\nspec: {target: {name: directory-secret}, data: [{secretKey: KEY, remoteRef: {key: hidden}}]}\n")
			s := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps"}
			s.AddObject(owner)
			l := New(s)
			l.SourceRoot = root
			admitForTest(s, root)
			if _, err := loadForTest(t.Context(), t, l, root); err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			if candidateForTest(t, l, testutil.KustomizationID("projected", "dual")) == nil {
				t.Fatal("generator data role suppressed the declared resource")
			}
			for _, id := range []manifest.NamedResource{testutil.KustomizationID("authored", "terminal"), testutil.KustomizationID("projected", "terminal"), secretID("projected", "hidden"), secretID("authored", "hidden"), testutil.KustomizationID("authored", "hidden-config"), testutil.KustomizationID("projected", "hidden-config"), testutil.KustomizationID("authored", "directory-config"), testutil.KustomizationID("configuration-only", "directory-config"), testutil.KustomizationID("projected", "directory-config"), secretID("configuration-only", "directory-secret"), secretID("projected", "directory-secret")} {
				if s.GetObject(id) != nil || len(l.Index.Records[id].Productions) != 0 {
					t.Fatalf("terminal/configuration input was admitted, projected or registered as producer: %v", id)
				}
			}
			for _, file := range []string{"apps/dual.yaml", tc.terminal, "apps/configuration.yaml", "apps/configuration/flux.yaml", "apps/configuration/external.yaml"} {
				assert.Diff(t, l.Index.Readers[file], []manifest.NamedResource{owner.Named()})
			}
			terminal := testutil.KustomizationID("authored", "terminal")
			if _, ok := l.Index.Lookup(terminal); ok || l.candidates[terminal] != nil || len(l.documents.cache[filepath.Join(root, tc.terminal)].objects) != 0 {
				t.Fatal("terminal-only patch retained raw admission evidence")
			}
		})
	}
}

func TestAdmit_FluxComponentInputContexts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: effective\n")
	testutil.WriteFile(t, root, "components/a/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [secret.yaml]\ntransformers: [https://example.invalid/opaque]\n")
	testutil.WriteFile(t, root, "components/a/secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: component}\nstringData: {KEY: 'ENC[AES256_GCM,data:x]'}\nsops: {mac: encrypted}\n")
	testutil.WriteFile(t, root, "components/b/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nconfigMapGenerator: [{name: b, files: [value.txt]}]\n")
	testutil.WriteFile(t, root, "components/b/value.txt", "value")
	s := store.New()
	a := &manifest.Kustomization{Name: "a", Namespace: "apps", Path: "./apps"}
	a.Components = []string{"../components/a", "../components/a", "missing"}
	a.IgnoreMissingComponents = true
	b := &manifest.Kustomization{Name: "b", Namespace: "apps", Path: "./apps"}
	b.Components = []string{"../components/b"}
	s.AddObject(a)
	s.AddObject(b)
	required := &manifest.Kustomization{Name: "required", Namespace: "apps", Path: "./apps"}
	required.Components = []string{"missing"}
	s.AddObject(required)
	l := New(s)
	idx := rendererFixture(t, l, root)
	assert.Diff(t, idx.Readers["apps/kustomization.yaml"], []manifest.NamedResource{a.Named(), b.Named(), required.Named()})
	assert.Diff(t, idx.Readers["components/a/secret.yaml"], []manifest.NamedResource{a.Named()})
	assert.Diff(t, idx.Readers["components/b/value.txt"], []manifest.NamedResource{b.Named()})
	assert.Diff(t, idx.Readers["apps/missing"], []manifest.NamedResource{required.Named()})
	if len(l.documents.cache[filepath.Join(root, "apps")].body.Components) != 0 {
		t.Fatal("injected components mutated the cached document or retained ignored input")
	}
	if s.GetObject(secretID("effective", "component")) == nil {
		t.Fatal("injected component lost its inherited namespace")
	}
	warnings := s.Warnings()
	if len(warnings) != 1 || len(warnings[0].Detail) != 1 || warnings[0].Resource != a.Named() || warnings[0].Category != manifest.WarnIncompleteInputCoverage {
		t.Fatalf("component warnings crossed owners: %+v", warnings)
	}
}

func TestFinalizeIndex_ConfigurationCoverage(t *testing.T) {
	trap := &configurationReadTrap{}
	previousHTTP, previousGit := http.DefaultTransport, client.Protocols["https"]
	http.DefaultTransport = trap
	client.InstallProtocol("https", server.NewClient(trap))
	t.Cleanup(func() {
		http.DefaultTransport = previousHTTP
		client.InstallProtocol("https", previousGit)
	})
	for _, tc := range []struct {
		name, config string
		warn         bool
	}{
		{"namespace wrapper", "namespace: apps\nresources: [plugin.yaml]\n", false},
		{"custom label operand", "resources: [plugin.yaml]\nlabels: [{pairs: {hidden: value}, fields: [{path: files}]}]\n", true},
		{"metadata label", "resources: [plugin.yaml]\nlabels: [{pairs: {owner: apps}, fields: [{path: metadata/labels}]}]\n", false},
		{"operand rewrite", "resources: [plugin.yaml]\npatches: [{target: {kind: ConfigMapGenerator}, patch: '[{\"op\":\"replace\",\"path\":\"/files/0\",\"value\":\"hidden.txt\"}]' }]\n", true},
		{"aggregate", "resources: [plugin.yaml, https://example.invalid/z.yaml, https://example.invalid/a.yaml]\npatches: [{path: operand-patch}]\n", true},
		{"unknown plugin", "resources: [plugin.yaml, unknown.yaml]\n", true},
		{"remote file", "generators: [https://example.invalid/plugin.yaml]\n", true},
		{"remote Git directory", "generators: [https://example.invalid/configs.git//generators?ref=main]\n", true},
		{"remote subtree", "resources: [https://example.invalid/configs.git//generators?ref=main]\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "generators: [../config]\n")
			testutil.WriteFile(t, root, "config/kustomization.yaml", tc.config)
			testutil.WriteFile(t, root, "config/plugin.yaml", "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: values}\nfiles: [value.txt]\n")
			testutil.WriteFile(t, root, "config/unknown.yaml", "apiVersion: example.invalid/v1\nkind: UnknownPlugin\nmetadata: {name: opaque}\nfiles: [../shared/value.txt]\n")
			testutil.WriteFile(t, root, "apps/value.txt", "known")
			testutil.WriteFile(t, root, "shared/value.txt", "hidden remote input")
			testutil.WriteFile(t, root, "clean/kustomization.yaml", "resources: [https://example.invalid/ordinary.yaml]\n")
			s := store.New()
			owner := &manifest.Kustomization{Name: "apps", Namespace: "apps", Path: "./apps"}
			s.AddObject(owner)
			s.AddObject(&manifest.Kustomization{Name: "clean", Namespace: "clean", Path: "./clean"})
			l := New(s)
			rendererFixture(t, l, root)
			first := s.Warnings()
			if tc.warn {
				if len(first) != 1 || first[0].Resource != owner.Named() || first[0].Category != manifest.WarnIncompleteInputCoverage || first[0].Count != 1 || !strings.Contains(first[0].Message, "flate build all without --base or --path-orig") {
					t.Fatalf("incomplete warning = %+v", first)
				}
			} else if len(first) != 0 {
				t.Fatalf("safe namespace wrapper warned: %+v", first)
			}
			if !strings.HasPrefix(tc.name, "remote") {
				assert.Diff(t, l.Index.Readers["apps/value.txt"], []manifest.NamedResource{owner.Named()})
			}
			if tc.warn {
				declaring := "config/kustomization.yaml"
				if tc.name == "unknown plugin" {
					declaring = "config/unknown.yaml"
				}
				if len(first[0].Detail) == 0 || !strings.Contains(first[0].Detail[0], declaring) || !slices.IsSorted(first[0].Detail) {
					t.Fatalf("missing declaring source or unstable detail: %+v", first)
				}
			}
			if len(l.Index.Readers["shared/value.txt"]) != 0 {
				t.Fatal("opaque remote content invented a local reader")
			}
			if tc.name == "unknown plugin" && !slices.ContainsFunc(first[0].Detail, func(detail string) bool {
				return strings.Contains(detail, "unknown plugin configuration may read additional files")
			}) {
				t.Fatalf("missing unknown-plugin uncertainty: %+v", first)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, s.Warnings(), first)
		})
	}
	assert.Equal(t, trap.calls, 0)
}

type configurationReadTrap struct{ calls int }

func (r *configurationReadTrap) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls++
	return nil, transport.ErrRepositoryNotFound
}

func (r *configurationReadTrap) Load(*transport.Endpoint) (storer.Storer, error) {
	r.calls++
	return nil, transport.ErrRepositoryNotFound
}

func TestFinalizeIndex_FlatopsNamespaceWrappers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kubernetes/transformers/kustomization.yaml", "resources: [namespace.yaml]\n")
	testutil.WriteFile(t, root, "kubernetes/transformers/namespace.yaml", namespaceTransformer)
	s := store.New()
	var owners []manifest.NamedResource
	for i := range 28 {
		name := fmt.Sprintf("ns-%02d", i)
		base := "kubernetes/apps/" + name
		testutil.WriteFile(t, root, base+"/kustomization.yaml", "transformers: [transformers]\n")
		testutil.WriteFile(t, root, base+"/transformers/kustomization.yaml", "namespace: "+name+"\nresources: [../../../transformers]\n")
		owner := &manifest.Kustomization{Name: name, Namespace: "flux-system", Path: "./" + base}
		s.AddObject(owner)
		owners = append(owners, owner.Named())
	}
	l := New(s)
	idx := rendererFixture(t, l, root)
	for _, file := range []string{"kubernetes/transformers/kustomization.yaml", "kubernetes/transformers/namespace.yaml"} {
		assert.Diff(t, idx.Readers[file], owners)
	}
	for _, owner := range owners {
		assert.Diff(t, idx.Readers["kubernetes/apps/"+owner.Name+"/transformers/kustomization.yaml"], []manifest.NamedResource{owner})
	}
	assert.Equal(t, len(s.Warnings()), 0)
}

func TestFinalizeIndex_InputInventory(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body, file string
	}{
		{"normalized base", "bases: [../shared]\n", "shared/cm.yaml"},
		{"component", "components: [../component]\n", "component/value.txt"},
		{"JSON patch", "patchesJson6902: [{path: ../inputs/value}]\n", "inputs/value"},
		{"strategic patch", "patchesStrategicMerge: [../inputs/value]\n", "inputs/value"},
		{"replacement", "replacements: [{path: ../inputs/value}]\n", "inputs/value"},
		{"configuration", "configurations: [../inputs/value]\n", "inputs/value"},
		{"CRD", "crds: [../inputs/value]\n", "inputs/value"},
		{"schema", "openapi: {path: ../inputs/value}\n", "inputs/value"},
		{"inline configuration", "generators:\n- |\n    apiVersion: builtin\n    kind: ConfigMapGenerator\n    metadata: {name: input}\n    files: [key=../inputs/value]\n", "inputs/value"},
		{"file configuration", "validators: [../plugin.yaml]\n", "inputs/value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", tc.body)
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\n")
			testutil.WriteFile(t, root, "component/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nsecretGenerator: [{name: input, files: [value.txt]}]\n")
			testutil.WriteFile(t, root, "component/value.txt", "component data")
			testutil.WriteFile(t, root, "inputs/value", "terminal data")
			testutil.WriteFile(t, root, "plugin.yaml", "apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: input}\nenvs: [../inputs/value]\n")
			s := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps"}
			s.AddObject(owner)
			idx := rendererFixture(t, New(s), root)
			assert.Diff(t, idx.Readers[tc.file], []manifest.NamedResource{owner.Named()})
			assert.Diff(t, ownersForTest(idx, tc.file), []manifest.NamedResource{owner.Named()})
			if len(idx.Readers["inputs/unrelated"]) != 0 {
				t.Fatal("unrelated sibling gained readers")
			}
		})
	}
}

func TestFinalizeIndex_InputCoordinatesAndAdmission(t *testing.T) {
	t.Parallel()
	for _, filename := range manifest.KustomizeBuilderFilenames {
		t.Run(filename, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			root := filepath.Join(parent, "source")
			testutil.WriteFile(t, parent, "outside.yaml", testutil.KustomizationYAML("outside", "authored", "  path: ./hidden\n"))
			testutil.WriteFile(t, root, "inputs/absolute.yaml", testutil.KustomizationYAML("absolute", "authored", "  path: ./hidden\n"))
			testutil.WriteFile(t, root, "inputs/escaped.yaml", testutil.KustomizationYAML("escaped", "authored", "  path: ./hidden\n"))
			body := fmt.Sprintf("resources: [%q, ../inputs/escaped.yaml, ../../outside.yaml]\npatches: [{path: ../inputs/missing}, {path: patch.yaml}]\n", filepath.ToSlash(filepath.Join(root, "inputs/absolute.yaml")))
			testutil.WriteFile(t, root, "apps/"+filename, body)
			s := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps"}
			s.AddObject(owner)
			sibling := &manifest.Kustomization{Name: "sibling", Namespace: "apps", Path: "./hidden"}
			s.AddObject(sibling)
			testutil.WriteFile(t, root, "apps/patch.yaml", testutil.KustomizationYAML("sibling", "apps", "  path: ./hidden\n"))
			l := New(s)
			l.Index.Records[sibling.Named()] = manifest.DiscoveryRecord{File: "apps/patch.yaml", Path: filepath.Join(root, "apps/patch.yaml")}
			idx := rendererFixture(t, l, root)
			if !idx.Records[sibling.Named()].Excluded {
				t.Fatal("file-role input counted as a listed structural resource")
			}
			for _, file := range []string{"apps/" + filename, "inputs/absolute.yaml", "inputs/escaped.yaml", "inputs/missing", "apps/patch.yaml"} {
				assert.Diff(t, idx.Readers[file], []manifest.NamedResource{owner.Named()})
			}
			for _, name := range []string{"absolute", "escaped", "outside"} {
				if s.GetObject(testutil.KustomizationID("authored", name)) != nil || len(idx.Records[testutil.KustomizationID("authored", name)].Productions) != 0 {
					t.Fatalf("dependency coordinate broadened object admission: %s", name)
				}
			}
			if len(idx.Readers["../outside.yaml"]) != 0 {
				t.Fatal("out-of-source resource gained a reader")
			}
		})
	}
}

func TestFinalizeIndex_InputSourceViews(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mounted", "alias", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			local := filepath.Join(root, ".artifacts", mode)
			input := fstest.MapFS{
				"entry/nested/kustomization.yaml": {Data: []byte("resources: [../../shared]\n")},
				"shared/kustomization.yaml":       {Data: []byte("patches: [{path: ../inputs/patch}]\n")},
			}
			testutil.WriteFile(t, local, "shared/kustomization.yaml", "patches: [{path: ../inputs/host-only}]\n")
			s := store.New()
			source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
			s.AddObject(source)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: source.Name, Namespace: source.Namespace}}
			s.AddObject(owner)
			art := &store.SourceArtifact{LocalRoot: root, LocalPath: local, FS: input}
			if mode == "alias" {
				art.InputAlias, art.LocalRoot = true, filepath.Join(root, "subtree")
			}
			if mode == "pinned" {
				art.Key = "pinned"
			}
			s.SetArtifact(source.Named(), art)
			l := New(s)
			l.SourceRoot = root
			admitForTest(s, root)
			_, err := loadForTest(t.Context(), t, l, filepath.Join(local, "entry"))
			assert.NoError(t, err)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			prefix := ".artifacts/" + mode + "/"
			assert.Diff(t, l.Index.Readers[prefix+"inputs/patch"], []manifest.NamedResource{owner.Named()})
			if len(l.Index.Readers[prefix+"inputs/host-only"]) != 0 {
				t.Fatal("discovery read host files outside the mounted view")
			}
			file := "inputs/patch"
			if mode == "alias" {
				file = "subtree/inputs/patch"
			}
			filter := change.NewFilter(change.NewSet([]string{file}), l.Index, s)
			assert.Equal(t, filter.ShouldReconcile(owner.Named()), mode != "pinned")
			assert.Equal(t, filter.ShouldReconcile(source.Named()), mode != "pinned")
			if mode == "pinned" {
				assert.Diff(t, l.Index.Readers[file], []manifest.NamedResource(nil))
			}
		})
	}
}

func TestLoader_InputRolesDualResourceOutside(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\npatches: [{path: ../inputs/dual.yaml}]\n")
	testutil.WriteFile(t, root, "inputs/kustomization.yaml", "resources: [dual.yaml]\n")
	testutil.WriteFile(t, root, "inputs/dual.yaml", testutil.KustomizationYAML("dual", "authored", "  path: ./hidden\n"))
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	if _, err := l.Load(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	l.Admit(root)
	id := testutil.KustomizationID("authored", "dual")
	if _, ok := l.Index.Lookup(id); !ok || l.candidates[id] == nil || st.GetObject(id) == nil || len(l.documents.cache[filepath.Join(root, "inputs/dual.yaml")].objects) != 1 {
		t.Fatal("terminal role suppressed a separately declared resource")
	}
}
