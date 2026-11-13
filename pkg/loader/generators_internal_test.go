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
		name, kind, namespace, input, value, want              string
		unreadable, existing, symlink, external, escape, probe bool
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
		{name: "sops_configmap_unreadable", kind: "configMap", input: "envs: [../input.txt]", value: "VALUE=" + encrypted, unreadable: true},
		{name: "sops_projected_namespace", kind: "secret", namespace: "namespace: outer\n", input: "envs: [../input.txt]", value: "VALUE=" + encrypted, unreadable: true},
		{name: "sops_secret_unreadable", kind: "secret", input: "envs: [../input.txt]", value: "VALUE=" + encrypted, unreadable: true},
		{name: "sops_secret_preserved", kind: "secret", input: "files: [VALUE=../input.txt]", value: encrypted, unreadable: true},
		{name: "yaml document", kind: "secret", input: "files: [VALUE=../input.txt]", value: "sops:\n  mac: ENC[fixture]\n", unreadable: true},
		{name: "json document", kind: "secret", input: "files: [VALUE=../input.txt]", value: `{"sops": {"mac": "ENC[fixture]"}}`, unreadable: true},
		{name: "dotenv document", kind: "secret", input: "files: [VALUE=../input.txt]", value: "sops_mac=ENC[fixture]", unreadable: true},
		{name: "shortest ini document", kind: "secret", input: "files: [VALUE=../input.txt]", value: "[sops]", unreadable: true},
		{name: "configmap document", kind: "configMap", input: "files: [VALUE=../input.txt]", value: "sops_mac=ENC[fixture]", want: "sops_mac=ENC[fixture]"},
		{name: "disabled document", kind: "secret", input: "files: [VALUE=../input.txt]", value: "sops_mac=ENC[fixture]", unreadable: true},
		{name: "cleartext_preserved", kind: "secret", input: "files: [VALUE=../input.txt]", value: "fixture", want: "fixture"},
		{name: "ciphertext literal", kind: "configMap", input: "literals: ['VALUE=" + encrypted + "']", unreadable: true},
		{name: "ciphertext file", kind: "secret", input: "files: [VALUE=../input.txt]", value: encrypted, unreadable: true},
		{name: "literal prefix", kind: "configMap", input: "envs: [../input.txt]", value: "VALUE=documentation: " + encrypted, want: "documentation: " + encrypted},
		{name: "embedded yaml", kind: "secret", input: "files: [VALUE=../input.txt]", value: "enabled: " + encrypted + "\n", want: "enabled: " + encrypted + "\n"},
		{name: "partial framing", kind: "configMap", input: "literals: ['VALUE=ENC[AES256_GCM,data:fixture]']", want: "ENC[AES256_GCM,data:fixture]"},
		{name: "encoded-looking plaintext", kind: "configMap", input: "files: [VALUE=../input.txt]", value: base64.StdEncoding.EncodeToString([]byte(encrypted)), want: base64.StdEncoding.EncodeToString([]byte(encrypted))},
		{name: "encoded-looking secret", kind: "secret", input: "files: [VALUE=../input.txt]", value: base64.StdEncoding.EncodeToString([]byte(encrypted)), want: base64.StdEncoding.EncodeToString([]byte(encrypted))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexGeneratorDataCase1(t, &tc, encrypted)
		})
	}
}

func testFinalizeIndexGeneratorDataCase1(t *testing.T, tc *struct {
	name       string
	kind       string
	namespace  string
	input      string
	value      string
	want       string
	unreadable bool
	existing   bool
	symlink    bool
	external   bool
	escape     bool
	probe      bool
}, encrypted string,
) {
	t.Helper()

	parent := t.TempDir()
	root := filepath.Join(parent, "repo")
	testutil.WriteFile(t, parent, "outside.txt", "OUTSIDE")
	testutil.WriteFile(t, root, "apps/input.txt", tc.value)
	input := tc.input
	additional := ""
	if tc.unreadable {
		additional = ", 'ZULU=" + encrypted + "', 'ALPHA=" + encrypted + "'"
	}
	if strings.HasPrefix(input, "literals: [") {
		input = strings.TrimSuffix(input, "]") + ", CLEAR=readable" + additional + "]"
	} else {
		input += "\n  literals: [CLEAR=readable" + additional + "]"
	}
	testutil.WriteFile(
		t,
		root,
		"apps/a/kustomization.yaml", tc.
			namespace+tc.kind+"Generator:\n- name: input\n  namespace: inner\n  "+input+"\n- name: sibling\n  namespace: inner\n  literals: [VALUE=sibling]\n",
	)
	if tc.escape {
		testutil.WriteFile(
			t,
			parent,
			"outside/kustomization.yaml", tc.
				kind+"Generator:\n- name: input\n  namespace: inner\n  "+tc.input+"\n",
		)
		testutil.WriteFile(
			t,
			root,
			"apps/a/kustomization.yaml",
			"resources: [../../../outside]\n"+tc.kind+"Generator:\n- name: sibling\n  namespace: inner\n  literals: [VALUE=sibling]\n",
		)
	}
	if tc.symlink {
		if err := os.Symlink(
			filepath.Join(parent, "outside.txt"),
			filepath.Join(root, "apps", "a", "escape.txt"),
		); err != nil {
			t.Fatal(err)
		}
	}
	st := store.New()
	ks := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./apps/a"}
	if tc.name == "implicit document" {
		ks.Path = "./apps"
	}
	if tc.external {
		ks.SourceRef.Kind, ks.SourceRef.Name, ks.SourceRef.Namespace = manifest.KindGitRepository, "external", "flux-system"
		st.AddObject(&manifest.GitRepository{Name: "external", Namespace: "flux-system"})
	}
	st.AddObject(ks)
	kind := manifest.KindConfigMap
	if tc.kind == "secret" {
		kind = manifest.KindSecret
	}
	ns := "inner"
	if tc.namespace != "" {
		ns = "outer"
	}
	id := manifest.NamedResource{Kind: kind, Namespace: ns, Name: "input"}
	if tc.existing {
		st.AddObject(
			&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "real"}},
		)
	}
	l := loaderFixture(st, nil)
	l.documents = readerForTest(root)
	admitForTest(st, root)
	l.mountSources()
	if tc.probe {
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
		l.documents.read(filepath.Join(root, "apps", "a"))
	} else if _, err := l.Load(t.Context(), filepath.Join(root, "apps", "a"), manifest.NamedResource{}, nil); err != nil {
		t.Fatal(err)
	}
	testFinalizeIndexGeneratorDataCase1Tail(
		t,
		&testFinalizeIndexGeneratorDataCase1TailInputs{
			l:    &l,
			root: &root,
			tc:   &tc,
			st:   &st,
			id:   &id,
			kind: &kind,
			ks:   &ks,
		},
	)
}

func testFinalizeIndexGeneratorDataCase1Part1(t *testing.T, tc **struct {
	name       string
	kind       string
	namespace  string
	input      string
	value      string
	want       string
	unreadable bool
	existing   bool
	symlink    bool
	external   bool
	escape     bool
	probe      bool
}, kind *string, obj manifest.Data,
) {
	t.Helper()
	want := (*tc).want
	if (*kind) == manifest.KindSecret {
		want = base64.StdEncoding.EncodeToString([]byte(want))
	}
	var got, clearValue any
	switch o := obj.(type) {
	case *manifest.ConfigMap:
		got, clearValue = o.Data["VALUE"], o.Data["CLEAR"]
	case *manifest.Secret:
		got, clearValue = o.Data["VALUE"], o.Data["CLEAR"]
	}
	if !(*tc).existing {
		wantClear := "readable"
		if (*kind) == manifest.KindSecret {
			wantClear = base64.StdEncoding.EncodeToString([]byte(wantClear))
		}
		assert.FatalIf(t, clearValue != wantClear,
			"CLEAR=%v, want %q", clearValue, wantClear)
	}
	if (*tc).unreadable {
		assert.FatalIf(t, got != nil,
			"encrypted VALUE retained: %v", got)
	} else if got != want {
		t.Fatalf("VALUE=%v, want %q", got, want)
	}
}

type testFinalizeIndexGeneratorDataCase1TailInputs struct {
	l **Loader

	root *string

	tc **struct {
		name string

		kind string

		namespace string

		input string

		value      string
		want       string
		unreadable bool
		existing   bool
		symlink    bool
		external   bool
		escape     bool
		probe      bool
	}

	st **store.
		Store

	id *manifest.
		NamedResource

	kind *string

	ks **manifest.
		Kustomization
}

func testFinalizeIndexGeneratorDataCase1Tail(t *testing.T, inputs *testFinalizeIndexGeneratorDataCase1TailInputs) {
	t.Helper()

	if err := (*inputs.l).FinalizeIndex(t.Context(), (*inputs.root), true); err != nil {
		t.Fatal(err)
	}
	if (*inputs.tc).probe {
		assert.FatalIf(t, (*inputs.st).GetObject((*inputs.id)) != nil, "%s", "unreached generator was materialized")

		if _, indexed := (*inputs.l).Index.Lookup(testutil.HelmReleaseID("", "excluded")); indexed {
			t.Fatal("unreached generator scope indexed its application")
		}
		return
	}
	obj := values.NewStoreProvider((*inputs.st)).Lookup((*inputs.id)).Data
	assert.FatalIf(
		t,
		!(*inputs.tc).existing && (*inputs.st).GetObject((*inputs.id)) != nil,
		"%s",
		"generator lookup entered canonical inventory",
	)

	if (*inputs.tc).want == "" && !(*inputs.tc).unreadable {
		assert.FatalIf(t, obj != nil,
			"failed generator materialized: %+v", obj)
	} else {
		testFinalizeIndexGeneratorDataCase1Part1(t, inputs.tc, inputs.kind, obj)
	}
	if (*inputs.tc).unreadable {
		var reads store.ReadSet
		reads.Init((*inputs.st), (*inputs.ks).Named(), nil)
		reads.GetObject(reads.ID())
		defer reads.Close()
		assert.FatalIf(
			t,
			values.NewStoreProvider(&reads).Lookup((*inputs.id)).Data == nil,
			"%s",
			"captured generator data missing",
		)

		if got := (*inputs.st).Warnings(); len(got) != 0 {
			t.Fatalf("unaccepted lookup published warnings: %+v", got)
		}
		reads.Accept(store.AssessmentReconciled)
		warnings := (*inputs.st).Warnings()
		assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnSOPSUnreadable ||
			warnings[0].Resource != (*inputs.id) ||
			warnings[0].Count != 1 ||
			!slices.Equal(warnings[0].Detail, []string{"ALPHA", "VALUE", "ZULU"}),
			"accepted generator warnings: %+v", warnings)
	}
	var owners []manifest.NamedResource
	if !(*inputs.tc).external && !(*inputs.tc).escape {
		owners = []manifest.NamedResource{(*inputs.ks).Named()}
	}
	var got []manifest.NamedResource
	for valueIndex := range (*inputs.l).Index.Records[(*inputs.id)].Productions {
		production := (*inputs.l).Index.Records[(*inputs.id)].Productions[valueIndex]
		if production.Owner != (manifest.NamedResource{}) {
			got = append(got, production.Owner)
		}
	}
	slices.SortFunc(got, manifest.NamedResource.Compare)
	got = slices.Compact(got)
	assert.FatalIf(t, !slices.Equal(got, owners),
		"Production owners = %v, want %v", got, owners)
	inputs.id.
		Name = "sibling"
	assert.FatalIf(
		t,
		values.NewStoreProvider((*inputs.st)).Lookup((*inputs.id)).Data == nil,
		"%s",
		"failed generator stopped its sibling",
	)
}
