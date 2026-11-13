package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

type membershipFixtureData struct {
	root, scenario, value, kind, field, spec string
	after                                    bool
}

func membershipFixture(t *testing.T, root, scenario string, after bool) {
	t.Helper()
	value := "before"
	if after {
		value = "after"
	}
	kind, field := "ConfigMap", "data"
	if strings.Contains(scenario, "Secret") || strings.Contains(scenario, "encrypted") ||
		scenario == "preexisting producer" {
		kind, field = "Secret", "stringData"
	}
	optional := scenario == "addition" || scenario == "deletion" || scenario == "namespace move"
	spec := "  path: ./payload\n  postBuild:\n    substitute: {SENTINEL: active}\n    substituteFrom: [{kind: " + kind + ", name: settings, optional: " + map[bool]string{false: "false", true: "true"}[optional] + "}]\n"
	if scenario == "Secret siblings" {
		spec += "      - {kind: Secret, name: sibling}\n"
		spec = strings.Replace(
			spec,
			"substituteFrom: [{kind: Secret, name: settings, optional: false}]",
			"substituteFrom:\n      - {kind: Secret, name: settings}",
			1,
		)
	}
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", spec))
	testutil.WriteFile(t, root, "payload/kustomization.yaml", "namespace: apps\nresources: [output.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"payload/output.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, uid: retained, annotations: {flate.test/preserved: exact}}\ndata: {value: '${VALUE:-fallback}', other: '${OTHER:-unused}', sentinel: '${SENTINEL}'}\n",
	)
	fixture := membershipFixtureData{
		root:     root,
		scenario: scenario,
		value:    value,
		kind:     kind,
		field:    field,
		spec:     spec,
		after:    after,
	}
	fixture.writeInput(t)
}

func (f *membershipFixtureData) writeInput(t *testing.T) {
	t.Helper()
	root, scenario, value, kind, field, after := f.root, f.scenario, f.value, f.kind, f.field, f.after
	inputValue, namespace := value, "apps"
	if scenario == "indexed loser" {
		testutil.WriteFile(
			t,
			root,
			"settings/z.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: winner}\n",
		)
	}
	if scenario == "addition" && !after || scenario == "deletion" && after {
		return
	}
	file := "settings/input.yaml"
	if scenario == "indexed winner" {
		testutil.WriteFile(
			t,
			root,
			"settings/a.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: loser}\n",
		)
		file = "settings/z.yaml"
	}
	if strings.HasPrefix(scenario, "graph ") || scenario == "omitted mixed file" || scenario == "indexed document" {
		inputValue = "stable"
	}
	if scenario == "file move" {
		inputValue = "stable"
		if after {
			file = "settings/moved.yaml"
		}
	}
	if scenario == "namespace move" && after {
		namespace = "other"
	}
	body := "apiVersion: v1\nkind: " + kind + "\nmetadata: {name: settings, namespace: " + namespace + "}\n" + field + ": {VALUE: " + inputValue + "}\n"
	if scenario == "bare encrypted input" || scenario == "omitted encrypted input" {
		body = "apiVersion: v1\nkind: Secret\nmetadata: {name: settings, namespace: apps}\nstringData: {VALUE: 'ENC[AES256_GCM,data:" + inputValue + ",iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n"
		if scenario == "bare encrypted input" {
			body = strings.Replace(body, ", namespace: apps", "", 1)
		}
	}
	if scenario == "namespace siblings" {
		body += "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: bare}\ndata: {VALUE: ignored}\n---\napiVersion: v1\nkind: Namespace\nmetadata: {name: apps}\n"
	}
	if scenario == "Secret siblings" {
		body += "---\napiVersion: v1\nkind: Secret\nmetadata: {name: sibling, namespace: apps}\nstringData: {OTHER: " + value + "}\n"
	}
	testutil.WriteFile(t, root, file, body)
	f.writeGraph(t)
}

func (f *membershipFixtureData) writeGraph(t *testing.T) {
	t.Helper()
	root, scenario, value, spec := f.root, f.scenario, f.value, f.spec
	if scenario == "graph-owned Secret" || scenario == "omitted encrypted input" {
		resources := "resources: [input.yaml]\n"
		if scenario == "omitted encrypted input" {
			resources = "resources: []\n"
		}
		testutil.WriteFile(t, root, "settings/kustomization.yaml", resources)
		testutil.WriteFile(
			t,
			root,
			"flux/inputs.yaml",
			testutil.KustomizationYAML("inputs", "apps", "  path: ./settings\n"),
		)
	}
	if scenario == "preexisting producer" {
		testutil.WriteFile(
			t,
			root,
			"flux/consumer.yaml",
			testutil.KustomizationYAML(
				"consumer",
				"apps",
				spec+"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"flux/inputs.yaml",
			testutil.KustomizationYAML(
				"inputs",
				"apps",
				"  path: ./producer\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [external.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"producer/external.yaml",
			"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer, namespace: apps}\nspec: {target: {name: settings}, data: [{secretKey: VALUE, remoteRef: {key: fixture}}]}\n",
		)
	}
	if strings.HasPrefix(scenario, "graph ") || scenario == "omitted mixed file" {
		membershipOwnedGraph(t, root, scenario, value)
	}
	if scenario == "indexed document" {
		testutil.WriteFile(
			t,
			root,
			"payload/kustomization.yaml",
			"namespace: apps\nresources: [output.yaml, generator.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"payload/generator.yaml",
			"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps, uid: authored, annotations: {source.extensions.fluxcd.io/reconcile: disabled}}\nspec: {sources: [{alias: "+value+", kind: GitRepository, name: flux-system, namespace: flux-system}]}\n",
		)
	}
	if scenario == "same identity different producers" {
		testutil.WriteFile(t, root, "flux/second.yaml", testutil.KustomizationYAML("second", "apps", spec))
	}
}

func membershipOwnedGraph(t *testing.T, root, scenario, value string) {
	t.Helper()
	resource := "../shared"
	if scenario == "graph direct file" {
		resource = "../shared/bundle.yaml"
	}
	metadata := "{name: child}"
	if scenario == "graph explicit namespace" {
		metadata = "{name: child, namespace: apps}"
	}
	testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./graph\n"))
	testutil.WriteFile(t, root, "graph/kustomization.yaml", "namespace: apps\nresources: ["+resource+"]\n")
	if scenario != "omitted mixed file" {
		testutil.WriteFile(t, root, "shared/kustomization.yaml", "namespace: apps\nresources: [bundle.yaml]\n")
	}
	bundle := "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: " + metadata + "\nspec: {path: ./child}\n"
	if scenario == "omitted mixed file" {
		testutil.WriteFile(t, root, "graph/kustomization.yaml", "resources: []\n")
		bundle += "---\napiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: excluded, namespace: apps}\nspec: {chart: {spec: {chart: missing, sourceRef: {kind: GitRepository, name: flux-system}}}, values: {value: " + value + "}}\n"
	}
	bundlePath := "shared/bundle.yaml"
	if scenario == "omitted mixed file" {
		bundlePath = "graph/bundle.yaml"
	}
	testutil.WriteFile(t, root, bundlePath, bundle)
	testutil.WriteFile(t, root, "child/kustomization.yaml", "namespace: apps\nresources: [output.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"child/output.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output}\ndata: {value: "+value+"}\n",
	)
}
