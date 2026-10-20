package kustomizeinput

import (
	"slices"
	"testing"

	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/yaml"
	syaml "sigs.k8s.io/yaml"
)

func TestKustomization_Inputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body, want string
		role             Role
	}{
		{"resource", "resources: [../base]", "../base", Resource},
		{"base", "bases: [../base]", "../base", Resource},
		{"component", "components: [../common]", "../common", Component},
		{"patch", "patches: [{path: ../patch}]", "../patch", File},
		{"json patch", "patchesJson6902: [{path: ../patch}]", "../patch", File},
		{"strategic patch", "patchesStrategicMerge: [../patch]", "../patch", File},
		{"replacement", "replacements: [{path: ../rules}]", "../rules", File},
		{"configuration", "configurations: [../config]", "../config", File},
		{"crd", "crds: [../crd]", "../crd", File},
		{"schema", "openapi: {path: ../schema}", "../schema", File},
		{"generator file", "configMapGenerator: [{name: data, files: [key=../input]}]", "../input", File},
		{"secret file", "secretGenerator: [{name: data, files: [../input]}]", "../input", File},
		{"environment", "configMapGenerator: [{name: data, envs: [../env]}]", "../env", File},
		{"singular environment", "secretGenerator: [{name: data, env: ../env}]", "../env", File},
		{"generator", "generators: [../plugin]", "../plugin", Configuration},
		{"transformer", "transformers: [../plugin]", "../plugin", Configuration},
		{"validator", "validators: [../plugin]", "../plugin", Configuration},
		{"case precedence", "Resources: [ignored]\nresources: [../base]", "../base", Resource},
		{"sequence alias", "inputs: &inputs [../base]\nresources: *inputs", "../base", Resource},
		{"scalar alias", "input: &input ../base\nresources: [*input]", "../base", Resource},
		{"merged mapping", "defaults: &defaults {resources: [../base]}\n<<: *defaults", "../base", Resource},
		{"merge precedence", "first: &first {resources: [../base]}\nsecond: &second {resources: [ignored]}\n<<: [*first, *second]", "../base", Resource},
		{"explicit merge override", "defaults: &defaults {resources: [ignored]}\n<<: *defaults\nresources: [../base]", "../base", Resource},
		{"inline patch", "patches: [{patch: 'some content'}]", "", File},
		{"invalid file source", "configMapGenerator: [{name: data, files: ['a=b=c', '=path', 'key=']}]", "", File},
		{"non-file generator", "configMapGenerator: [{name: data, literals: [key=value]}]", "", File},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var doc types.Kustomization
			if err := syaml.Unmarshal([]byte(tc.body), &doc); err != nil {
				t.Fatal(err)
			}
			refs := slices.Collect(Kustomization(&doc))
			if tc.want == "" {
				if len(refs) != 0 {
					t.Fatalf("unexpected input: %+v", refs)
				}
				return
			}
			if len(refs) != 1 || refs[0].Path != tc.want || refs[0].Role != tc.role {
				t.Fatalf("inputs = %+v; want %s in role %v", refs, tc.want, tc.role)
			}
			if refs[0].Node != nil {
				t.Fatal("typed inventory retained a YAML node")
			}
			var tree yaml.Node
			if err := yaml.Unmarshal([]byte(tc.body), &tree); err != nil {
				t.Fatal(err)
			}
			refs[0].Bind(&tree)
			before := refs[0].Node.Value
			refs[0].Rewrite("rewritten")
			if before == refs[0].Node.Value || refs[0].Node.Value == "" {
				t.Fatal("reference lost its rewrite location")
			}
			if tc.name == "generator file" && refs[0].Node.Value != "key=rewritten" {
				t.Fatalf("generator key lost: %q", refs[0].Node.Value)
			}
			if tc.name == "secret file" {
				refs[0].Rewrite("again")
				if refs[0].Node.Value != "input=again" {
					t.Fatalf("generator key lost on repeated rewrite: %q", refs[0].Node.Value)
				}
			}
		})
	}
}

func TestPlugin_Inputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, fields, want string
		known              bool
	}{
		{"ConfigMapGenerator", "files: [key=../data]", "../data", true},
		{"SecretGenerator", "envs: [../env]", "../env", true},
		{"SecretGenerator", "env: ignored", "", true},
		{"PatchTransformer", "path: ../patch", "../patch", true},
		{"PatchJson6902Transformer", "path: ../patch", "../patch", true},
		{"PatchStrategicMergeTransformer", "paths: [../patch]", "../patch", true},
		{"ReplacementTransformer", "replacements: [{path: ../rules}]", "../rules", true},
		{"ValueAddTransformer", "targetFilePath: ../target", "../target", true},
		{"NamespaceTransformer", "namespace: apps", "", true},
		{"IAMPolicyGenerator", "", "", true},
		{"AnnotationsTransformer", "annotations: {owner: apps}", "", true},
		{"HashTransformer", "", "", true},
		{"ImageTagTransformer", "imageTag: {name: app, newTag: test}", "", true},
		{"LabelTransformer", "labels: {owner: apps}", "", true},
		{"PrefixSuffixTransformer", "prefix: app-", "", true},
		{"PrefixTransformer", "prefix: app-", "", true},
		{"SuffixTransformer", "suffix: -app", "", true},
		{"ReplicaCountTransformer", "replica: {name: app, count: 2}", "", true},
		{"Unknown", "files: [hidden]", "", false},
	} {
		t.Run(tc.kind+"/"+tc.fields, func(t *testing.T) {
			t.Parallel()
			var doc yaml.Node
			if err := yaml.Unmarshal([]byte("apiVersion: builtin\nkind: "+tc.kind+"\n"+tc.fields), &doc); err != nil {
				t.Fatal(err)
			}
			refs, known := Plugin(&doc)
			if known != tc.known || tc.want == "" && len(refs) != 0 || tc.want != "" && (len(refs) != 1 || refs[0].Path != tc.want) {
				t.Fatalf("Plugin = %+v, %v; want %q, %v", refs, known, tc.want, tc.known)
			}
		})
	}
}

func TestKustomization_InlineConfiguration(t *testing.T) {
	t.Parallel()
	var doc types.Kustomization
	if err := syaml.Unmarshal([]byte("generators:\n  - |\n    apiVersion: builtin\n    kind: ConfigMapGenerator\n    metadata: {name: data}\n    files: [input]\npatchesStrategicMerge:\n  - |\n    apiVersion: v1\n    kind: ConfigMap\n    metadata: {name: data}\n"), &doc); err != nil {
		t.Fatal(err)
	}
	refs := slices.Collect(Kustomization(&doc))
	if len(refs) != 1 || refs[0].Role != Configuration || !refs[0].Inline {
		t.Fatalf("inline discrimination = %+v", refs)
	}
}

func TestKustomization_InputFields(t *testing.T) {
	t.Parallel()
	const body = "resources: [a, b]\npatches: [{patch: inline}, {path: c}]\nopenapi: {path: schema}\nconfigMapGenerator:\n  - files: [d, e, f, g, h, i, j, k, l, m, 'n']\n  - envs: [o]\n"
	var doc types.Kustomization
	if err := syaml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	refs := slices.Collect(Kustomization(&doc))
	want := []string{
		".resources[0]", ".resources[1]", ".patches[1].path", ".openapi.path",
		".configMapGenerator[0].files[0]", ".configMapGenerator[0].files[1]", ".configMapGenerator[0].files[2]",
		".configMapGenerator[0].files[3]", ".configMapGenerator[0].files[4]", ".configMapGenerator[0].files[5]",
		".configMapGenerator[0].files[6]", ".configMapGenerator[0].files[7]", ".configMapGenerator[0].files[8]",
		".configMapGenerator[0].files[9]", ".configMapGenerator[0].files[10]", ".configMapGenerator[1].envs[0]",
	}
	if len(refs) != len(want) {
		t.Fatalf("inputs = %+v; want %d", refs, len(want))
	}
	var prefix []string
	for ref := range Kustomization(&doc) {
		prefix = append(prefix, ref.Path)
		if len(prefix) == 2 {
			break
		}
	}
	if !slices.Equal(prefix, []string{"a", "b"}) {
		t.Fatalf("stopped inputs = %v", prefix)
	}
	var tree yaml.Node
	if err := yaml.Unmarshal([]byte(body), &tree); err != nil {
		t.Fatal(err)
	}
	for i, ref := range refs {
		ref.Bind(&tree)
		if ref.Node == nil || ref.Node.Value != ref.Path {
			t.Fatalf("input %d rewrite location does not match %q", i, ref.Path)
		}
		if ref.FieldPath() != want[i] {
			t.Errorf("input %d field = %q; want %q", i, ref.FieldPath(), want[i])
		}
	}
}
