package orchestrator

import (
	"strings"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

type editWitnessCase struct {
	name, file, old, next string
	prepare               map[string]string
	second                string
	owners                []manifest.NamedResource
	postRender            bool
	children              [2]string
}

func witnessChartDependencies() map[string]string {
	return map[string]string{
		"charts/demo/Chart.yaml": workWitnessFiles()["charts/demo/Chart.yaml"] +
			"dependencies:\n  - name: child\n    version: 1.0.0\n    alias: copy\n    " +
			"condition: child.enabled\n",

		"charts/demo/charts/child/Chart.yaml": "apiVersion: v2\nname: child\nversion: 1.0.0\n",

		"charts/demo/charts/child/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\n" +
			"metadata: {name: {{ .Chart.Name }}-output, namespace: apps}\n",

		"flux/release.yaml": strings.ReplaceAll(
			workWitnessFiles()["flux/release.yaml"],
			"value: old",
			"value: old, child: {enabled: true}",
		),
	}
}

func editWitnessCases() []editWitnessCase {
	return []editWitnessCase{
		{
			name:   "release-values",
			file:   "flux/release.yaml",
			old:    "value: old",
			next:   "value: new",
			owners: []manifest.NamedResource{testutil.HelmReleaseID("apps", "release")},
		},

		{
			name:   "image",
			file:   "apps/a/cm.yaml",
			old:    "busybox:1",
			next:   "busybox:2",
			owners: []manifest.NamedResource{testutil.KustomizationID("apps", "a")},
		},

		{
			name: "kustomization-patch",
			file: "flux/a.yaml",
			old:  "  interval: 1h",
			next: "  interval: 1h\n  patches:\n    - target: {kind: ConfigMap, name: a-outpu" +
				"t}\n      patch: '[{\"op\": \"add\", \"path\": \"/data/patched\", \"value" +
				"\": \"new\"}]'",
			owners: []manifest.NamedResource{testutil.KustomizationID("apps", "a")},
		},

		{
			name:   "annotation",
			file:   "apps/a/kustomization.yaml",
			old:    "resources: [cm.yaml]",
			next:   "resources: [cm.yaml]\ncommonAnnotations: {edited: yes}",
			owners: []manifest.NamedResource{testutil.KustomizationID("apps", "a")},
		},

		{
			name:   "namespace",
			file:   "apps/a/kustomization.yaml",
			old:    "resources: [cm.yaml]",
			next:   "resources: [cm.yaml]\nnamespace: moved",
			owners: []manifest.NamedResource{testutil.KustomizationID("apps", "a")},
		},

		{
			name: "component",
			file: "shared/kustomization.yaml",
			old:  "shared: old",
			next: "shared: new",
			owners: []manifest.NamedResource{
				testutil.KustomizationID("apps", "a"),
				testutil.KustomizationID("apps", "b"),
			},
		},

		{
			name:   "substitution",
			file:   "flux/vars.yaml",
			old:    "VALUE: old",
			next:   "VALUE: new",
			owners: []manifest.NamedResource{testutil.KustomizationID("apps", "a")},
		},

		{name: "interval", file: "flux/a.yaml", old: "interval: 1h", next: "interval: 2h"},

		{
			name: "nested-patch",
			file: "patches/nested/patch.yaml",
			old:  "patched: old",
			next: "patched: new",
			prepare: map[string]string{
				"apps/a/kustomization.yaml": "resources: [../../bases/a]\ncomponents: [../../shared]\n",

				"bases/a/kustomization.yaml": "resources: [../../apps/a/cm.yaml]\npatches: [{path: ../../patches/nested/patch.yaml}]\n",

				"patches/nested/patch.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: a-output, namespace: ap" +
					"ps}\ndata: {patched: old}\n",
			},
			owners: []manifest.NamedResource{testutil.KustomizationID("apps", "a")},
		},

		{
			name: "resourceset-input",
			file: "flux/set.yaml",
			old:  "value: old",
			next: "value: new",
			owners: []manifest.NamedResource{
				{
					Group:     manifest.FluxOperatorDomain,
					Kind:      manifest.KindResourceSet,
					Namespace: "apps",
					Name:      "set",
				},
			},
		},

		{
			name:   "multi-file",
			file:   "apps/a/cm.yaml",
			old:    "busybox:1",
			next:   "busybox:2",
			second: "flux/release.yaml",
			owners: []manifest.NamedResource{
				testutil.KustomizationID("apps", "a"),
				testutil.HelmReleaseID("apps", "release"),
			},
		},

		{name: "readme", file: "README.md", next: "unconsumed\n"},

		{
			name: "unused",
			file: "apps/a/unused.yaml",
			next: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unused}\n",
		},

		{
			name:     "dependency-alias",
			file:     "charts/demo/Chart.yaml",
			old:      "alias: copy",
			next:     "alias: mirror",
			prepare:  witnessChartDependencies(),
			owners:   []manifest.NamedResource{testutil.HelmReleaseID("apps", "release")},
			children: [2]string{"copy-output", "mirror-output"},
		},

		{
			name:     "dependency-condition",
			file:     "flux/release.yaml",
			old:      "enabled: true",
			next:     "enabled: false",
			prepare:  witnessChartDependencies(),
			owners:   []manifest.NamedResource{testutil.HelmReleaseID("apps", "release")},
			children: [2]string{"copy-output", ""},
		},

		{
			name:   "comment",
			file:   "apps/a/cm.yaml",
			old:    "apiVersion: v1",
			next:   "# byte-sensitive input\napiVersion: v1",
			owners: []manifest.NamedResource{testutil.KustomizationID("apps", "a")},
		},

		{
			name:   "scalar-whitespace",
			file:   "apps/a/cm.yaml",
			old:    "'${VALUE}'",
			next:   "'${VALUE} '",
			owners: []manifest.NamedResource{testutil.KustomizationID("apps", "a")},
		},

		{
			name:   "template-whitespace",
			file:   "charts/demo/templates/cm.yaml",
			old:    ".Values.value | quote",
			next:   "printf \"%s \" .Values.value | quote",
			owners: []manifest.NamedResource{testutil.HelmReleaseID("apps", "release")},
		},

		{
			name:   "chart-version",
			file:   "charts/demo/Chart.yaml",
			old:    "version: 1.0.0",
			next:   "version: 1.0.1",
			owners: []manifest.NamedResource{testutil.HelmReleaseID("apps", "release")},
		},

		{
			name: "chart-lock",
			file: "charts/demo/Chart.lock",
			old:  "generated: 2026-01-01",
			next: "generated: 2026-01-02",
			prepare: map[string]string{
				"charts/demo/Chart.lock": "dependencies: []\ndigest: sha256:fixture\ngenerated: 2026-01-01T00:00:00Z\n",
			},
			owners: []manifest.NamedResource{testutil.HelmReleaseID("apps", "release")},
		},

		{
			name: "post-render",
			file: "flux/release.yaml",
			old:  "post: old",
			next: "post: new",
			prepare: map[string]string{
				"flux/release.yaml": workWitnessFiles()["flux/release.yaml"] +
					"  postRenderers:\n    - kustomize:\n        patches:\n          - target:" +
					" {kind: ConfigMap, name: release-output}\n            patch: |-\n        " +
					"      apiVersion: v1\n              kind: ConfigMap\n              metada" +
					"ta: {name: release-output}\n              data: {post: old}\n",
			},
			owners:     []manifest.NamedResource{testutil.HelmReleaseID("apps", "release")},
			postRender: true,
		},
	}
}
