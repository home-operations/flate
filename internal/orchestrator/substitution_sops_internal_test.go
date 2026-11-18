package orchestrator

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_RootSecretOverridesDeclaredOutput(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [secret.yaml, producer.yaml, consumer.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"secret.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: settings, namespace: apps}\nstringData: {VALUE: repository}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"producer.yaml",
		"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: settings, namespace: apps}\nspec:\n  target: {name: settings}\n  data: [{secretKey: VALUE, remoteRef: {key: fixture}}]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"consumer.yaml",
		substitutionKS("consumer", "leaf", "  postBuild: {substituteFrom: [{kind: Secret, name: settings}]}\n"),
	)
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"leaf/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: '${VALUE}'}\n",
	)
	input := &manifest.Secret{Name: "settings", Namespace: "apps", StringData: map[string]any{"VALUE": "private-value"}}
	o := newSubstitutionOrchestrator(
		t,
		treeConfig{
			Path:     root,
			RepoRoot: root,
			Supply: []store.Declaration{
				{Doc: testutil.ObjectDocument(input), Obj: input, Origin: manifest.SuppliedDeclaration},
			},
		},
	)
	assert.NoError(t, o.bootstrap(t.Context()))
	secret := input.Named()
	assert.FatalIf(
		t,
		o.store.GetObject(secret) != input && o.store.GetObject(secret).(manifest.Data).HasKeys() == false ||
			o.store.EmissionOwner(
				secret,
			) != (manifest.NamedResource{}),
		"%s",
		"root-owned Secret published before its producer",
	)

	record := o.index.Records[secret]
	before, ok := record.Declaration.(*manifest.Secret)
	assert.FatalIf(
		t,
		!ok || before.StringData["VALUE"] != "repository" || record.BuilderOwner != o.entryBinding.Producer,
		"root-owned Secret lost immutable entry evidence: %+v",
		record,
	)

	result, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(result.Close)
	assert.NoError(t, err)
	assert.Equal(t, substitutionValue(t, result, testutil.KustomizationID("apps", "consumer")), "repository")
	after, ok := o.store.GetObject(secret).(*manifest.Secret)
	assert.FatalIf(t, !ok || after.StringData["VALUE"] != "repository",
		"runtime repository precedence lost: %+v", after)

	assert.Equal(t, o.store.EmissionOwner(secret), o.entryBinding.Producer)
	_, _, inventory := o.store.GetEmission(o.entryBinding.Producer)
	inventory.Children, inventory.Complete = nil, true
	o.store.RecordEmitted(o.entryBinding.Producer, &inventory, nil)
	inventory.Finish()
	o.store.DeleteObject(secret)
	facts := o.store.ReferenceFacts(secret, nil, manifest.NamedResource{})
	assert.FatalIf(t, facts.Data == nil, "%s", "repository data removed the invocation supplied snapshot")

	result1, err := facts.Data.Value("VALUE")
	supplied := result1.Value
	assert.NoError(t, err)
	assert.Equal(t, supplied, "private-value")
	assert.Equal(t, input.StringData["VALUE"], "private-value")
}

func TestRender_SuppliedObjectsPreserveRequiredChartFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		references    string
		sources       []store.Declaration
		missingSecret string
	}{
		{
			name:       "unrelated supplied Secret",
			references: "[{kind: Secret, name: absent}]",
			sources: []store.Declaration{
				{
					Doc: testutil.ObjectDocument(&manifest.Secret{
						Name:       "missing",
						Namespace:  "flux-system",
						StringData: map[string]any{"OTHER": "unrelated"},
					}), Origin: manifest.SuppliedDeclaration, Obj: &manifest.Secret{
						Name:       "missing",
						Namespace:  "flux-system",
						StringData: map[string]any{"OTHER": "unrelated"},
					},
					InputPath: "ci.yaml",
				},
			},
			missingSecret: "absent",
		},
		{
			name:       "same name in another namespace",
			references: "[{kind: Secret, name: missing}]",
			sources: []store.Declaration{
				{
					Doc: testutil.ObjectDocument(&manifest.Secret{
						Name:       "missing",
						Namespace:  "elsewhere",
						StringData: map[string]any{"OTHER": "unrelated"},
					}), Origin: manifest.SuppliedDeclaration, Obj: &manifest.Secret{
						Name:       "missing",
						Namespace:  "elsewhere",
						StringData: map[string]any{"OTHER": "unrelated"},
					},
					InputPath: "ci.yaml",
				},
			},
			missingSecret: "missing",
		},
		{
			name:       "supplied and missing references",
			references: "[{kind: Secret, name: given}, {kind: Secret, name: missing}]",
			sources: []store.Declaration{
				{
					Doc: testutil.ObjectDocument(&manifest.Secret{
						Name:       "given",
						Namespace:  "flux-system",
						StringData: map[string]any{"OTHER": "unrelated"},
					}), Origin: manifest.SuppliedDeclaration, Obj: &manifest.Secret{
						Name:       "given",
						Namespace:  "flux-system",
						StringData: map[string]any{"OTHER": "unrelated"},
					},
					InputPath: "ci.yaml",
				},
			},
			missingSecret: "missing",
		},
		{
			name:       "only supplied reference",
			references: "[{kind: Secret, name: missing}]",
			sources: []store.Declaration{
				{
					Doc: testutil.ObjectDocument(&manifest.Secret{
						Name:       "missing",
						Namespace:  "flux-system",
						StringData: map[string]any{"OTHER": "unrelated"},
					}), Origin: manifest.SuppliedDeclaration, Obj: &manifest.Secret{
						Name:       "missing",
						Namespace:  "flux-system",
						StringData: map[string]any{"OTHER": "unrelated"},
					},
					InputPath: "ci.yaml",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "flux/apps.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.test/cluster.git
`+"---\n"+
				testutil.KustomizationYAML("apps", "flux-system", `  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substitute: {KEEP: "ok"}
    substituteFrom: `+tt.references+"\n"))
			testutil.WriteFile(t, dir, "apps/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ./hr.yaml\n")
			testutil.WriteFile(t, dir, "apps/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/req
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  values:
    greeting: "${MISSING}"
`)
			testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux/apps.yaml]\n")
			testutil.WriteFile(t, dir, "charts/req/Chart.yaml", "apiVersion: v2\nname: req\nversion: 0.1.0\n")
			testutil.WriteFile(
				t,
				dir,
				"charts/req/templates/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  g: {{ required \"greeting is required\" .Values.greeting | quote }}\n",
			)

			renderedSingleton1, err := renderTreesForTest(t.Context(), t,
				[]Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
				Config{
					CacheDir: t.TempDir(), Concurrency: 2,
					Supply: tt.sources,
				},
			)
			assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
			res := renderedSingleton1[0].Result
			{
				_, ok := errors.AsType[*selection.FailuresError](err)
				assert.ErrorIf(t, !ok, "expected reconcile failure, got %v", err)
			}
			assert.FatalIf(t, res == nil, "%v", "missing render result")
			demo := testutil.HelmReleaseID("apps", "demo")
			if tt.missingSecret == "" {
				{
					info, failed := res.Failed[demo]
					assert.ErrorIf(t, !failed ||
						!strings.Contains(
							info.Message,
							"greeting is required",
						), "required chart value failure lost: %+v", info)
				}
			} else {
				consumer := testutil.KustomizationID("flux-system", "apps")
				target := manifest.NamedResource{
					Kind:      manifest.KindSecret,
					Namespace: consumer.Namespace,
					Name:      tt.missingSecret,
				}
				assert.ErrorIf(
					t,
					!slices.Contains(res.Blocked[consumer], target),
					"missing required reference admitted: %+v",
					res,
				)
			}
			{
				docs := outputByParent(res.Manifests)[demo]
				assert.ErrorIf(t, len(docs) != 0, "failed input published chart output: %v", docs)
			}
			for _, warning := range res.Warnings {
				assert.ErrorIf(
					t,
					warning.Category == manifest.WarnUnresolvedSubstitution,
					"failed input reported successful substitution: %+v",
					warning,
				)
			}
		})
	}
}
