package loader

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestFinalizeIndex_GeneratorData(t *testing.T) {
	t.Parallel()
	const encrypted = "ENC[AES256_GCM,data:fixture,iv:x,tag:y,type:str]"
	for _, tc := range []struct {
		name, kind, namespace, input, value, want        string
		wipe, existing, symlink, external, escape, probe bool
	}{
		{name: "unreached_probe", kind: "configMap", input: "literals: [VALUE=fixture]", external: true, probe: true},
		{name: "outside_flux_graph", kind: "configMap", input: "literals: [VALUE=fixture]", want: "fixture", external: true},
		{name: "missing_file", kind: "configMap", input: "files: [VALUE=missing.txt]"},
		{name: "outside_root", kind: "configMap", input: "files: [VALUE=../../../outside.txt]"},
		{name: "resource_outside_root", kind: "configMap", input: "literals: [VALUE=outside]", escape: true},
		{name: "symlink_escape", kind: "configMap", input: "files: [VALUE=escape.txt]", symlink: true},
		{name: "implicit document", kind: "configMap", input: "files: [VALUE=../input.txt]", value: "fixture", want: "fixture"},
		{name: "normalized environment", kind: "configMap", input: "env: ../input.txt", value: "VALUE=fixture", want: "fixture"},
		{name: "inside_root", kind: "configMap", input: "files: [VALUE=../input.txt]", value: "fixture", want: "fixture"},
		{name: "outer_namespace", kind: "configMap", namespace: "namespace: outer\n", input: "literals: [VALUE=fixture]", want: "fixture"},
		{name: "real_object_wins", kind: "configMap", input: "literals: [VALUE=generated]", existing: true, want: "real"},
		{name: "sops_configmap_wipe", kind: "configMap", input: "envs: [../input.txt]", value: "VALUE=" + encrypted, wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "sops_secret_wipe", kind: "secret", input: "envs: [../input.txt]", value: "VALUE=" + encrypted, wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "sops_secret_preserved", kind: "secret", input: "files: [VALUE=../input.txt]", value: encrypted, want: encrypted},
		{name: "yaml document", kind: "secret", input: "files: [VALUE=../input.txt]", value: "sops:\n  mac: ENC[fixture]\n", wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "json document", kind: "secret", input: "files: [VALUE=../input.txt]", value: `{"sops": {"mac": "ENC[fixture]"}}`, wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "dotenv document", kind: "secret", input: "files: [VALUE=../input.txt]", value: "sops_mac=ENC[fixture]", wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "shortest ini document", kind: "secret", input: "files: [VALUE=../input.txt]", value: "[sops]", wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "configmap document", kind: "configMap", input: "files: [VALUE=../input.txt]", value: "sops_mac=ENC[fixture]", wipe: true, want: "sops_mac=ENC[fixture]"},
		{name: "disabled document", kind: "secret", input: "files: [VALUE=../input.txt]", value: "sops_mac=ENC[fixture]", want: "sops_mac=ENC[fixture]"},
		{name: "cleartext_preserved", kind: "secret", input: "files: [VALUE=../input.txt]", value: "fixture", wipe: true, want: "fixture"},
		{name: "ciphertext literal", kind: "configMap", input: "literals: ['VALUE=" + encrypted + "']", wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "ciphertext file", kind: "secret", input: "files: [VALUE=../input.txt]", value: encrypted, wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "literal prefix", kind: "configMap", input: "envs: [../input.txt]", value: "VALUE=documentation: " + encrypted, wipe: true, want: "documentation: " + encrypted},
		{name: "embedded yaml", kind: "secret", input: "files: [VALUE=../input.txt]", value: "enabled: " + encrypted + "\n", wipe: true, want: "enabled: " + encrypted + "\n"},
		{name: "partial framing", kind: "configMap", input: "literals: ['VALUE=ENC[AES256_GCM,data:fixture]']", wipe: true, want: "ENC[AES256_GCM,data:fixture]"},
		{name: "encoded-looking plaintext", kind: "configMap", input: "files: [VALUE=../input.txt]", value: base64.StdEncoding.EncodeToString([]byte(encrypted)), wipe: true, want: base64.StdEncoding.EncodeToString([]byte(encrypted))},
		{name: "encoded-looking secret", kind: "secret", input: "files: [VALUE=../input.txt]", value: base64.StdEncoding.EncodeToString([]byte(encrypted)), wipe: true, want: base64.StdEncoding.EncodeToString([]byte(encrypted))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexGeneratorDataCase(t, &tc)
		},
		)
	}
}

func testFinalizeIndexGeneratorDataCase(t *testing.T, testCaseInput *struct {
	name      string
	kind      string
	namespace string
	input     string
	value     string
	want      string
	wipe      bool
	existing  bool
	symlink   bool
	external  bool
	escape    bool

	probe bool
},
) {
	t.Helper()

	parent := t.TempDir()
	root := filepath.Join(parent, "repo")
	testutil.WriteFile(t, parent, "outside.txt", "OUTSIDE")
	testutil.WriteFile(t, root, "apps/input.txt", testCaseInput.value)
	input := testCaseInput.input
	if strings.HasPrefix(input, "literals: [") {
		input = strings.TrimSuffix(input, "]") + ", CLEAR=readable]"
	} else {
		input += "\n  literals: [CLEAR=readable]"
	}
	testutil.WriteFile(
		t,
		root,
		"apps/a/kustomization.yaml", testCaseInput.namespace+testCaseInput.kind+"Generator:\n- name: input\n  namespace: inner\n  "+input+"\n- name: sibling\n  namespace: inner\n  literals: [VALUE=sibling]\n",
	)
	if testCaseInput.escape {
		testutil.WriteFile(
			t,
			parent,
			"outside/kustomization.yaml", testCaseInput.kind+"Generator:\n- name: input\n  namespace: inner\n  "+testCaseInput.input+"\n",
		)
		testutil.WriteFile(
			t,
			root,
			"apps/a/kustomization.yaml",
			"resources: [../../../outside]\n"+testCaseInput.kind+"Generator:\n- name: sibling\n  namespace: inner\n  literals: [VALUE=sibling]\n",
		)
	}
	if testCaseInput.symlink {
		{
			err := os.Symlink(
				filepath.Join(parent, "outside.txt"),
				filepath.Join(root, "apps/a/escape.txt"),
			)
			assert.NoError(t, err)
		}
	}
	st := store.New()
	ks := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./apps/a"}
	if testCaseInput.name == "implicit document" {
		ks.Path = "./apps"
	}
	if testCaseInput.external {
		ks.SourceRef.Kind, ks.SourceRef.Name, ks.SourceRef.Namespace = manifest.KindGitRepository, "external", "flux-system"
		st.AddObject(&manifest.GitRepository{Name: "external", Namespace: "flux-system"})
	}
	st.AddObject(ks)
	kind := manifest.KindConfigMap
	if testCaseInput.kind == "secret" {
		kind = manifest.KindSecret
	}
	ns := "inner"
	if testCaseInput.namespace != "" {
		ns = "outer"
	}
	id := manifest.NamedResource{Kind: kind, Namespace: ns, Name: "input"}
	if testCaseInput.existing {
		st.AddObject(
			&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "real"}},
		)
	}
	l := loaderFixture(st, nil)
	l.Options.WipeSecrets = testCaseInput.wipe
	l.documents = readerForTest(root)
	admitForTest(st, root)
	l.mountSources()
	if testCaseInput.probe {
		testutil.WriteFile(
			t,
			root,
			"apps/a/release.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: excluded}\nspec: {suspend: true}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"apps/a/kustomization.yaml",
			"resources: [release.yaml]\nconfigMapGenerator:\n- name: input\n  namespace: inner\n  literals: [VALUE=fixture]\n",
		)
		l.documents.read(filepath.Join(root, "apps/a"))
	} else {
		_, err := l.Load(t.Context(), filepath.Join(root, "apps/a"))
		assert.NoError(t, err)
	}
	testFinalizeIndexGeneratorDataCaseCompletion(t, &testFinalizeIndexGeneratorDataCaseCompletionState{testCaseInput: &testCaseInput, root: &root, st: &st, ks: &ks, kind: &kind, id: &id, l: &l})
}

type testFinalizeIndexGeneratorDataCaseCompletionState struct {
	testCaseInput **struct {
		name      string
		kind      string
		namespace string
		input     string
		value     string
		want      string
		wipe      bool
		existing  bool
		symlink   bool
		external  bool
		escape    bool
		probe     bool
	}
	root *string
	st   **store.Store
	ks   **manifest.Kustomization
	kind *string
	id   *manifest.NamedResource
	l    **Loader
}

func testFinalizeIndexGeneratorDataCaseCompletion(t *testing.T, scope *testFinalizeIndexGeneratorDataCaseCompletionState) {
	t.Helper()
	{
		err := (*scope.l).FinalizeIndex(t.Context(), (*scope.root), true)
		assert.NoError(t, err)
	}
	if (*scope.testCaseInput).probe {
		{
			assert.FatalIf(t, (*scope.st).GetObject((*scope.id)) != nil, "%s", "unreached generator was materialized")
		}
		{
			_, indexed := (*scope.l).Index.Lookup(testutil.HelmReleaseID("", "excluded"))
			assert.FatalIf(t, indexed, "%s", "unreached generator scope indexed its application")
		}
		return
	}
	obj := values.NewStoreProvider((*scope.st), true).Data((*scope.id))
	{
		assert.FatalIf(
			t,
			!(*scope.testCaseInput).existing && (*scope.st).GetObject((*scope.id)) != nil,
			"%s",
			"generator lookup entered canonical inventory",
		)
	}
	if (*scope.testCaseInput).want == "" {
		assert.FatalIf(t, obj != nil, "failed generator materialized: %+v", obj)
	} else {
		want := (*scope.testCaseInput).want
		if (*scope.kind) == manifest.KindSecret {
			want = base64.StdEncoding.EncodeToString([]byte(want))
		}
		var got, clearValue any
		switch o := obj.(type) {
		case *manifest.ConfigMap:
			got, clearValue = o.Data["VALUE"], o.Data["CLEAR"]
		case *manifest.Secret:
			got, clearValue = o.Data["VALUE"], o.Data["CLEAR"]
		}
		if !(*scope.testCaseInput).existing {
			wantClear := "readable"
			if (*scope.kind) == manifest.KindSecret {
				wantClear = base64.StdEncoding.EncodeToString([]byte(wantClear))
			}
			assert.FatalIf(t, clearValue != wantClear, "CLEAR=%v, want %q", clearValue, wantClear)
		}
		assert.FatalIf(t, got != want, "VALUE=%v, want %q", got, want)
	}
	var owners []manifest.NamedResource
	if !(*scope.testCaseInput).external && !(*scope.testCaseInput).escape {
		owners = []manifest.NamedResource{(*scope.ks).Named()}
	}
	{
		got := testutil.Owners((*scope.l).Index.Records[(*scope.id)].Productions)
		assert.FatalIf(t, !slices.Equal(got, owners), "Production owners = %v, want %v", got, owners)
	}
	(*scope.id).Name = "sibling"
	{
		assert.FatalIf(
			t,
			values.NewStoreProvider((*scope.st), true).Data((*scope.id)) == nil,
			"%s",
			"failed generator stopped its sibling",
		)
	}
}
