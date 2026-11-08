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

	"github.com/home-operations/flate/internal/selection"

	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFinalizeIndex_DuplicateDocumentKeys(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [../shared]\nnamespace: one\nnamespace: two\n")
	testutil.WriteFile(
		t,
		root,
		"shared/kustomization.yaml",
		"resources: [cm.yaml]\npatches: [{path: ../inputs/ignored, path: ../inputs/patch}]\n",
	)
	testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n")
	testutil.WriteFile(t, root, "inputs/ignored", "[]\n")
	testutil.WriteFile(t, root, "inputs/patch", "- op: add\n  path: /data\n  value: {key: changed}\n")
	testutil.WriteFile(t, root, "alias/kustomization.yaml", "namespace: one\nx: &namespace y\n*namespace: z\n")
	testutil.WriteFile(
		t,
		root,
		"merge/kustomization.yaml",
		"a: &a {namespace: one}\nb: &b {namePrefix: pre-}\n<<: *a\n<<: *b\n",
	)
	s := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
	s.AddObject(owner)
	l := New(s)
	l.SourceRoot = root
	admitForTest(s, root)
	_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "entry"))
	assert.NoError(t, err)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	for _, file := range []string{"shared/kustomization.yaml", "shared/cm.yaml", "inputs/patch"} {
		assert.Diff(
			t,
			l.Index.Readers[file],
			[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
		)
		assert.ErrorIf(
			t,
			!slices.Contains(ownersForTest(l.Index, file), owner.Named()),
			"owner missing for %s",
			file,
		)
	}
	doc := l.documents.read(filepath.Join(root, "entry")).body
	assert.FatalIf(t, doc == nil, "%s", "duplicate keys discarded the typed document")
	assert.Equal(t, doc.Namespace, "two")
	doc = l.documents.read(filepath.Join(root, "shared")).body
	assert.FatalIf(t, doc == nil || len(doc.Patches) != 1, "%s", "nested duplicate keys discarded the typed patch")
	assert.Equal(t, doc.Patches[0].Path, "../inputs/patch")
	assert.ErrorIf(t, len(l.Index.Readers["inputs/ignored"]) != 0 ||
		slices.Contains(
			ownersForTest(l.Index, "inputs/ignored"),
			owner.Named(),
		), "%s", "discarded duplicate patch gained a reader or owner")
	doc = l.documents.read(filepath.Join(root, "alias")).body
	assert.FatalIf(t, doc == nil, "%s", "alias key discarded the typed document")
	assert.Equal(t, doc.Namespace, "one")
	doc = l.documents.read(filepath.Join(root, "merge")).body
	assert.FatalIf(t, doc == nil, "%s", "duplicate merge keys discarded a typed document")
	assert.Equal(t, doc.Namespace, "one")
	assert.Equal(t, doc.NamePrefix, "pre-")
}

func TestTraversal_ImplicitMountedBoundary(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	documents := documentReader{
		cache: map[string]document{},
		views: map[string]fs.FS{
			root: fstest.MapFS{
				"entry/local.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: local}\n")},
				"entry/nested/decoy.yaml": {Data: []byte("decoy")},
			},
			filepath.Join(root, "entry", "nested"): fstest.MapFS{
				"private.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: private}\n")},
			},
		},
	}
	walk := traversal{documents: documents, visited: map[graphContext]struct{}{}}
	var visits []string
	walk.walk(t.Context(), graphVisit{root: root, dir: "entry"}, func(v graphVisit) bool {
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
			testFinalizeIndexConfigurationBoundariesCase(
				t,
				&mounted,
			)
		},
		)
	}
}

func TestFinalizeIndex_ConfigurationInvokingLoader(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "configs/kustomization.yaml", "namespace: wrappers\nresources: [nested]\n")
	testutil.WriteFile(
		t,
		root,
		"configs/nested/kustomization.yaml",
		"resources: [../plugin.yaml]\ncomponents: [../component]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"configs/component/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [plugin.yaml]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"configs/component/plugin.yaml",
		"apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: nested}\nenvs: [extra.env]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"configs/plugin.yaml",
		"apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [value.txt]\n",
	)
	s := store.New()
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(
			t,
			root,
			name+"/kustomization.yaml",
			"resources: [../configs/plugin.yaml]\ngenerators: [../configs]\n",
		)
		testutil.WriteFile(t, root, name+"/value.txt", name)
		testutil.WriteFile(t, root, name+"/extra.env", "VALUE="+name)
		s.AddObject(&manifest.Kustomization{Name: name, Namespace: "apps", Path: "./" + name})
	}
	testutil.WriteFile(
		t,
		root,
		"combined/kustomization.yaml",
		"resources: [../a, ../b, dual.yaml]\ngenerators: [dual.yaml]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"combined/dual.yaml",
		"apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: dual}\nfiles: [dual.txt]\n",
	)
	testutil.WriteFile(t, root, "combined/dual.txt", "dual input")
	combined := &manifest.Kustomization{Name: "combined", Namespace: "apps", Path: "./combined"}
	s.AddObject(combined)
	testutil.WriteFile(t, root, "ordinary/kustomization.yaml", "resources: [../configs/plugin.yaml]\n")
	s.AddObject(&manifest.Kustomization{Name: "ordinary", Namespace: "apps", Path: "./ordinary"})
	l := New(s)
	idx := rendererFixture(t, l, root)
	for _, name := range []string{"a", "b"} {
		assert.Diff(
			t,
			idx.Readers[name+"/value.txt"],
			[]manifest.FileReader{
				{Reader: testutil.KustomizationID("apps", name), Source: manifest.BootstrapSourceID},
				{Reader: combined.Named(), Source: manifest.BootstrapSourceID},
			},
		)
		assert.Diff(
			t,
			idx.Readers[name+"/extra.env"],
			[]manifest.FileReader{
				{Reader: testutil.KustomizationID("apps", name), Source: manifest.BootstrapSourceID},
				{Reader: combined.Named(), Source: manifest.BootstrapSourceID},
			},
		)
	}
	assert.Diff(
		t,
		idx.Readers["combined/dual.txt"],
		[]manifest.FileReader{{Reader: combined.Named(), Source: manifest.BootstrapSourceID}},
	)
	assert.FatalIf(t, len(idx.Readers["configs/value.txt"]) != 0 || len(idx.Readers["configs/nested/value.txt"]) != 0 ||
		len(idx.Readers["ordinary/value.txt"]) != 0 ||
		len(s.Warnings()) != 0,
		"configuration used its own loader or lost namespace-wrapper proof: %+v", s.Warnings())

	{
		_, exists := idx.Lookup(
			manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "generated", Namespace: "wrappers"},
		)
		assert.FatalIf(t, exists, "%s", "configuration content became a generated object")
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
			testFinalizeIndexNestedConfigurationLoaderCase(
				t,
				&tc,
			)
		},
		)
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
			testLoaderInputRolesAndAdmissionCase(t, &tc)
		},
		)
	}
}

func TestAdmit_FluxComponentInputContexts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: effective\n")
	testutil.WriteFile(
		t,
		root,
		"components/a/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [secret.yaml]\ntransformers: [https://example.invalid/opaque]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"components/a/secret.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: component}\nstringData: {KEY: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"components/b/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nconfigMapGenerator: [{name: b, files: [value.txt]}]\n",
	)
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
	assert.Diff(
		t,
		idx.Readers["apps/kustomization.yaml"],
		[]manifest.FileReader{
			{Reader: a.Named(), Source: manifest.BootstrapSourceID},
			{Reader: b.Named(), Source: manifest.BootstrapSourceID},
			{Reader: required.Named(), Source: manifest.BootstrapSourceID},
		},
	)
	assert.Diff(
		t,
		idx.Readers["components/a/secret.yaml"],
		[]manifest.FileReader{{Reader: a.Named(), Source: manifest.BootstrapSourceID}},
	)
	assert.Diff(
		t,
		idx.Readers["components/b/value.txt"],
		[]manifest.FileReader{{Reader: b.Named(), Source: manifest.BootstrapSourceID}},
	)
	assert.Diff(
		t,
		idx.Readers["apps/missing"],
		[]manifest.FileReader{{Reader: required.Named(), Source: manifest.BootstrapSourceID}},
	)
	assert.FatalIf(
		t,
		len(l.documents.cache[filepath.Join(root, "apps")].body.Components) != 0,
		"%s",
		"injected components mutated the cached document or retained ignored input",
	)
	{
		componentRecord, indexed := idx.Lookup(secretID("effective", "component"))
		// SOPS removes encrypted keys, so the index proves namespace inheritance.
		assert.FatalIf(
			t,
			!indexed || componentRecord.Authored.Name != "component",
			"%s",
			"injected component lost its inherited namespace",
		)
	}
	warnings := s.Warnings()
	assert.FatalIf(t, len(warnings) != 1 || len(warnings[0].Detail) != 1 || warnings[0].Resource != a.Named() ||
		warnings[0].Category != manifest.WarnIncompleteInputCoverage, "component warnings crossed owners: %+v", warnings)
}

//nolint:paralleltest // The transport trap changes process-global HTTP and Git clients.
func TestFinalizeIndex_ConfigurationCoverage(t *testing.T) {
	trap := &configurationReadTrap{}
	previousHTTP, previousGit := http.DefaultTransport, client.Protocols["https"]
	http.DefaultTransport = trap //nolint:reassign // The trap proves discovery never uses the process-global transport.
	client.InstallProtocol("https", server.NewClient(trap))
	t.Cleanup(func() {
		http.DefaultTransport = previousHTTP //nolint:reassign // Restore the process-global transport after the trap.
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
			testFinalizeIndexConfigurationCoverageCase(t, &tc)
		},
		)
	}
	assert.Equal(t, trap.calls, 0)
}

type configurationReadTrap struct{ calls int }

func (r *configurationReadTrap) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls++
	return nil, transport.ErrRepositoryNotFound
}

//nolint:ireturn // The go-git loader contract requires a storer interface return.
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
	owners := make([]manifest.NamedResource, 0, 28)
	for i := range 28 {
		name := fmt.Sprintf("ns-%02d", i)
		base := "kubernetes/apps/" + name
		testutil.WriteFile(t, root, base+"/kustomization.yaml", "transformers: [transformers]\n")
		testutil.WriteFile(
			t,
			root,
			base+"/transformers/kustomization.yaml",
			"namespace: "+name+"\nresources: [../../../transformers]\n",
		)
		owner := &manifest.Kustomization{Name: name, Namespace: "flux-system", Path: "./" + base}
		s.AddObject(owner)
		owners = append(owners, owner.Named())
	}
	l := New(s)
	idx := rendererFixture(t, l, root)
	for _, file := range []string{"kubernetes/transformers/kustomization.yaml", "kubernetes/transformers/namespace.yaml"} {
		want := make([]manifest.FileReader, len(owners))
		for i, owner := range owners {
			want[i] = manifest.FileReader{Reader: owner, Source: manifest.BootstrapSourceID}
		}
		assert.Diff(t, idx.Readers[file], want)
	}
	for _, owner := range owners {
		assert.Diff(
			t,
			idx.Readers["kubernetes/apps/"+owner.Name+"/transformers/kustomization.yaml"],
			[]manifest.FileReader{{Reader: owner, Source: manifest.BootstrapSourceID}},
		)
	}
	assert.Equal(t, len(s.Warnings()), 0)
}

func testFinalizeIndexConfigurationBoundariesCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	parent := t.TempDir()
	root := filepath.Join(parent, "source")
	input := fstest.MapFS{
		"source/apps/kustomization.yaml": {Data: []byte("generators: [../../foreign/plugin.yaml]\n")},
		"foreign/plugin.yaml": {
			Data: []byte(
				"apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: foreign}\nfiles: [private.txt]\n",
			),
		},
	}
	if *testCaseInput {
		root = parent
	}
	s := store.New()
	path := "./apps"
	if *testCaseInput {
		path = "./source/apps"
	}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: path}
	s.AddObject(owner)
	s.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{LocalPath: parent, LocalRoot: parent, FS: input},
	)
	if *testCaseInput {
		source := &manifest.GitRepository{Name: "foreign", Namespace: "apps"}
		s.AddObject(source)
		s.SetArtifact(
			source.Named(),
			&store.SourceArtifact{
				LocalPath: filepath.Join(parent, "foreign"),
				FS:        fstest.MapFS{"plugin.yaml": input["foreign/plugin.yaml"]},
			},
		)
	}
	l := New(s)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	warnings := s.Warnings()
	assert.FatalIf(t, len(warnings) != 1 || warnings[0].Resource != owner.Named() ||
		warnings[0].Category != manifest.WarnIncompleteInputCoverage ||
		!slices.ContainsFunc(warnings[0].Detail, func(detail string) bool {
			return strings.Contains(detail, "configuration reference cannot be inspected locally")
		}), "missing source-boundary uncertainty: %+v", warnings)
	for _, file := range []string{"../foreign/plugin.yaml", "foreign/plugin.yaml", "source/apps/private.txt", "apps/private.txt"} {
		assert.ErrorIf(
			t,
			len(l.Index.Readers[file]) != 0,
			"foreign configuration gained attribution: %s",
			file,
		)
	}
	{
		id := (manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "foreign"})
		assert.FatalIf(t, len(
			l.Index.Records[id].Productions,
		) != 0 ||
			s.GetObject(id) != nil, "%s", "foreign configuration admitted a producer")
	}
}

func testFinalizeIndexNestedConfigurationLoaderCase(
	t *testing.T,
	testCaseInput *struct {
		name        string
		transformer string
		warn        bool
	},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "generators: [../config]\n")
	testutil.WriteFile(
		t,
		root,
		"config/kustomization.yaml",
		"resources: [plugin.yaml]\ntransformers: [nested.yaml]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"config/plugin.yaml",
		"apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [value.txt]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"config/nested.yaml",
		"apiVersion: builtin\nmetadata: {name: nested}\n"+testCaseInput.transformer,
	)
	testutil.WriteFile(t, root, "config/operand-patch", "[]\n")
	testutil.WriteFile(t, root, "apps/value.txt", "value")
	s := store.New()
	owner := &manifest.Kustomization{Name: "apps", Namespace: "apps", Path: "./apps"}
	s.AddObject(owner)
	idx := rendererFixture(t, New(s), root)
	assert.Diff(
		t,
		idx.Readers["apps/value.txt"],
		[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
	)
	if testCaseInput.warn {
		assert.Diff(
			t,
			idx.Readers["config/operand-patch"],
			[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
		)
		assert.FatalIf(t, len(s.Warnings()) != 1,
			"nested operand rewrite must warn: %+v", s.Warnings())
	} else if len(s.Warnings()) != 0 {
		t.Fatalf("metadata-only nested transformer warned: %+v", s.Warnings())
	}
	assert.FatalIf(
		t,
		len(idx.Readers["apps/operand-patch"]) != 0,
		"%s",
		"nested invocation retained the outer plugin loader",
	)
}

func testLoaderInputRolesAndAdmissionCase(t *testing.T, testCaseInput *struct {
	name     string
	patch    string
	terminal string
},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"apps/kustomization.yaml",
		"namespace: projected\nresources: [dual.yaml]\nconfigMapGenerator: [{name: data, files: [dual.yaml]}]\n"+testCaseInput.patch+"\ngenerators: [configuration.yaml, configuration]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/dual.yaml",
		testutil.KustomizationYAML("dual", "authored", "  path: ./child\n"),
	)
	testutil.WriteFile(
		t,
		root, testCaseInput.terminal, testutil.KustomizationYAML("terminal", "authored", "  path: ./hidden\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"apps/configuration.yaml",
		"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: hidden-config, namespace: authored}\nspec: {path: ./hidden}\n---\napiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: config}\nspec: {target: {name: hidden}, data: [{secretKey: KEY, remoteRef: {key: hidden}}]}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/configuration/kustomization.yaml",
		"namespace: configuration-only\nresources: [flux.yaml, external.yaml]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/configuration/flux.yaml",
		testutil.KustomizationYAML("directory-config", "authored", "  path: ./hidden\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"apps/configuration/external.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: directory-config}\nspec: {target: {name: directory-secret}, data: [{secretKey: KEY, remoteRef: {key: hidden}}]}\n",
	)
	s := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./apps"}
	s.AddObject(owner)
	l := New(s)
	l.SourceRoot = root
	admitForTest(s, root)
	{
		_, err := loadForTest(t.Context(), t, l, root)
		assert.NoError(t, err)
	}
	admitted := map[manifest.NamedResource]bool{}
	//nolint:gocritic // The iterator supplies filtered value snapshots through its callback.
	for id, record := range l.Index.Files() {
		admitted[id] = selection.Root(
			rootAtForTest(l, l.IndexedObject(id, l.Index.Records[id]).Obj, record.Path),
		).Admitted
	}
	assert.Equal(t, l.terminalInput(filepath.Join(root, testCaseInput.terminal)), true)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	for id, before := range admitted {
		record, _ := l.Index.Lookup(id)
		assert.Equal(
			t,
			selection.Root(rootAtForTest(l, l.IndexedObject(id, l.Index.Records[id]).Obj, record.Path)).Admitted,
			before,
		)
	}
	assert.Equal(t, l.terminalInput(filepath.Join(root, testCaseInput.terminal)), true)
	assert.FatalIf(
		t,
		candidateForTest(t, l, testutil.KustomizationID("projected", "dual")) == nil,
		"%s",
		"generator data role suppressed the declared resource",
	)
	for _, id := range []manifest.NamedResource{testutil.KustomizationID("authored", "terminal"), testutil.KustomizationID("projected", "terminal"), secretID("projected", "hidden"), secretID("authored", "hidden"), testutil.KustomizationID("authored", "hidden-config"), testutil.KustomizationID("projected", "hidden-config"), testutil.KustomizationID("authored", "directory-config"), testutil.KustomizationID("configuration-only", "directory-config"), testutil.KustomizationID("projected", "directory-config"), secretID("configuration-only", "directory-secret"), secretID("projected", "directory-secret")} {
		assert.FatalIf(
			t,
			s.GetObject(id) != nil || len(l.Index.Records[id].Productions) != 0,
			"terminal/configuration input was admitted, projected or registered as producer: %v",
			id,
		)
	}
	for _, file := range []string{"apps/dual.yaml", testCaseInput.terminal, "apps/configuration.yaml", "apps/configuration/flux.yaml", "apps/configuration/external.yaml"} {
		assert.Diff(
			t,
			l.Index.Readers[file],
			[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
		)
	}
	terminal := testutil.KustomizationID("authored", "terminal")
	{
		_, ok := l.Index.Lookup(
			terminal,
		)
		assert.FatalIf(t, ok || l.candidates[terminal].Obj != nil ||
			len(
				l.documents.cache[filepath.Join(root, testCaseInput.terminal)].objects,
			) != 0, "%s", "terminal-only patch retained raw admission evidence")
	}
}

func testFinalizeIndexConfigurationCoverageCase(
	t *testing.T,
	testCaseInput *struct {
		name   string
		config string
		warn   bool
	},
) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "generators: [../config]\n")
	testutil.WriteFile(t, root, "config/kustomization.yaml", testCaseInput.config)
	testutil.WriteFile(
		t,
		root,
		"config/plugin.yaml",
		"apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: values}\nfiles: [value.txt]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"config/unknown.yaml",
		"apiVersion: example.invalid/v1\nkind: UnknownPlugin\nmetadata: {name: opaque}\nfiles: [../shared/value.txt]\n",
	)
	testutil.WriteFile(t, root, "apps/value.txt", "known")
	testutil.WriteFile(t, root, "shared/value.txt", "hidden remote input")
	testutil.WriteFile(
		t,
		root,
		"clean/kustomization.yaml",
		"resources: [https://example.invalid/ordinary.yaml]\n",
	)
	s := store.New()
	owner := &manifest.Kustomization{Name: "apps", Namespace: "apps", Path: "./apps"}
	s.AddObject(owner)
	s.AddObject(&manifest.Kustomization{Name: "clean", Namespace: "clean", Path: "./clean"})
	l := New(s)
	rendererFixture(t, l, root)
	first := s.Warnings()
	if testCaseInput.warn {
		assert.FatalIf(t, len(first) != 1 || first[0].Resource != owner.Named() ||
			first[0].Category != manifest.WarnIncompleteInputCoverage ||
			first[0].Count != 1 ||
			!strings.Contains(
				first[0].Message,
				"flate build all without --base or --path-orig",
			), "incomplete warning = %+v", first)
	} else {
		assert.FatalIf(t, len(first) != 0, "safe namespace wrapper warned: %+v", first)
	}
	if !strings.HasPrefix(testCaseInput.name, "remote") {
		assert.Diff(
			t,
			l.Index.Readers["apps/value.txt"],
			[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
		)
	}
	if testCaseInput.warn {
		declaring := "config/kustomization.yaml"
		if testCaseInput.name == "unknown plugin" {
			declaring = "config/unknown.yaml"
		}
		assert.FatalIf(t, len(first[0].Detail) == 0 || !strings.Contains(first[0].Detail[0], declaring) ||
			!slices.IsSorted(first[0].Detail), "missing declaring source or unstable detail: %+v", first)
	}
	assert.FatalIf(
		t,
		len(l.Index.Readers["shared/value.txt"]) != 0,
		"%s",
		"opaque remote content invented a local reader",
	)
	assert.FatalIf(
		t,
		testCaseInput.name == "unknown plugin" && !slices.ContainsFunc(first[0].Detail, func(detail string) bool {
			return strings.Contains(detail, "unknown plugin configuration may read additional files")
		}),
		"missing unknown-plugin uncertainty: %+v",
		first,
	)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	assert.Diff(t, s.Warnings(), first)
}
