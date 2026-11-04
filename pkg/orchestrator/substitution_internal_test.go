package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func substitutionKS(name, path, postBuild string) string {
	return testutil.KustomizationYAML(name, "apps", "  interval: 1m\n  path: ./"+path+"\n"+postBuild)
}

func writeSubstitutionCluster(t *testing.T, root, reference, expression string) {
	t.Helper()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux.yaml",
		substitutionKS(
			"consumer",
			"app",
			"  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: "+reference+"}]\n",
		),
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: '"+expression+"'}\n",
	)
}

//nolint:gocritic // Configuration must mutate its own copy because callers reuse their original value.
func newSubstitutionOrchestrator(t *testing.T, cfg treeConfig) *Orchestrator {
	t.Helper()
	if cfg.RepoRoot == "" {
		cfg.RepoRoot = cfg.Path
	}
	cfg.CacheDir = t.TempDir()
	cfg.Concurrency = 4
	cfg.WipeSecrets = true
	o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		o.stop()
	})
	return o
}

func suppliedSubstitution(name string) store.Declaration {
	return store.Declaration{
		InputPath: "ci-values.yaml",
		Doc: testutil.ObjectDocument(&manifest.ConfigMap{
			Name:      name,
			Namespace: "apps",
			Data:      map[string]any{"VALUE": "supplied"},
		}),
		Obj: &manifest.ConfigMap{
			Name:      name,
			Namespace: "apps",
			Data:      map[string]any{"VALUE": "supplied"},
		},
		Origin: manifest.SuppliedDeclaration,
	}
}

func substitutionValue(t *testing.T, res *Result, id manifest.NamedResource) string {
	t.Helper()
	docs := outputByParent(res.Manifests)[id]
	assert.FatalIf(t, len(docs) != 1, "expected one output for %s, got %d", id, len(docs))
	value, _ := docs[0]["data"].(map[string]any)["value"].(string)
	return value
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestSnapshotSubstitutions_OwnsInputs(t *testing.T) {
	t.Run("supplied custom scope", testSuppliedCustomScope)
	source := suppliedSubstitution("external")
	cm := source.Obj.(*manifest.ConfigMap)
	raw := []byte("owned")
	cm.Data["BINARY"] = raw
	source.Doc = testutil.ObjectDocument(cm)
	secretData, secretStringData := []byte("b3duZWQ="), []byte("owned")
	secret := &manifest.Secret{
		Name:       "secret",
		Namespace:  "apps",
		Data:       map[string]any{"ENCODED": secretData},
		StringData: map[string]any{"VALUE": secretStringData},
	}
	allowed := []string{manifest.KindSecret}
	customDoc := map[string]any{
		"apiVersion": "example.io/v1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"name":      "custom",
			"namespace": "apps",
		},
		"spec":  map[string]any{"payload": map[string]any{"value": "owned"}},
		"extra": map[string]any{"retained": true},
	}
	sources := []store.Declaration{
		source,
		{
			Doc:       testutil.ObjectDocument(secret),
			Obj:       secret,
			InputPath: "secret-values.yaml",
			Origin:    manifest.SuppliedDeclaration,
		},
		{Doc: customDoc},
	}

	o := newSubstitutionOrchestrator(t, treeConfig{Supply: sources, AllowMissingKinds: allowed, Path: t.TempDir()})
	customDoc["spec"].(map[string]any)["payload"].(map[string]any)["value"] = "changed"
	customDoc["extra"].(map[string]any)["retained"] = false
	id := manifest.NamedResource{Group: "example.io", Kind: "Widget", Namespace: "apps", Name: "custom"}
	custom := o.store.GetObject(id).(*manifest.RawObject)
	assert.Equal(t, custom.Spec["payload"].(map[string]any)["value"], any("owned"))
	for _, child := range o.cfg.Supply {
		if manifest.DocKind(child.Doc) == "Widget" {
			assert.Equal(t, child.Doc["extra"].(map[string]any)["retained"], any(true))
		}
	}
	cm.Name, cm.Data["VALUE"] = "changed", "changed"
	raw[0], secretData[0], secretStringData[0] = 'x', 'x', 'x'
	sources[0].InputPath, allowed[0] = "changed", manifest.KindConfigMap
	facts := o.store.ReferenceFacts(manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: "apps",
		Name:      "external",
	},
		nil,
		false,
		manifest.NamedResource{})
	assert.Equal(t, o.cfg.Supply[0].InputPath, "ci-values.yaml")
	for key, want := range map[string]string{"VALUE": "supplied", "BINARY": "owned"} {
		got, _, err := facts.Data.Value(key)
		assert.NoError(t, err)
		assert.Equal(t, got, want)
	}
	secretFacts := o.store.ReferenceFacts(secret.Named(), nil, false, manifest.NamedResource{})
	for _, key := range []string{"ENCODED", "VALUE"} {
		got, _, err := secretFacts.Data.Value(key)
		assert.NoError(t, err)
		assert.Equal(t, got, "owned")
	}
	assert.Diff(t, o.cfg.AllowMissingKinds, []string{manifest.KindSecret})
	plain := newSubstitutionOrchestrator(t, treeConfig{Config: Config{}, Path: t.TempDir()})
	if plain.cfg.AllowMissingKinds != nil || plain.cfg.Supply != nil {
		t.Fatal("zero-input path allocated state")
	}
}

func TestRenderTrees_SubstitutionValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		object manifest.BaseManifest
	}{
		{"typed nil ConfigMap", (*manifest.ConfigMap)(nil)},
		{"missing name", &manifest.ConfigMap{Namespace: "apps"}},
		{"missing namespace", &manifest.Secret{Name: "external"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := RenderTrees(
				t.Context(),
				[]Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: t.TempDir()}},
				Config{
					Supply: []store.Declaration{
						{Obj: tc.object, Doc: testutil.ObjectDocument(tc.object), Origin: manifest.SuppliedDeclaration},
					},
				},
			)
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "expected input error, got %v", err)
		})
	}
	for _, injected := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache/injected=%t", injected), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			cfg := treeConfig{
				CacheDir: root,
				Supply: []store.Declaration{
					{
						Obj:    &manifest.ConfigMap{Namespace: "apps"},
						Doc:    testutil.ObjectDocument(&manifest.ConfigMap{Namespace: "apps"}),
						Origin: manifest.SuppliedDeclaration,
					},
				},
				Path: t.TempDir(),
			}
			if injected {
				owner := cachetest.New(t, root, sourcecache.New)
				cfg.Cache = owner
			}
			cfg.ID = manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}
			_, err := RenderTrees(t.Context(), []Tree{cfg.Tree}, cfg.Config)
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || strings.Contains(err.Error(), "private-fixture-value"),
				"invalid or unredacted SDK supply error: %v",
				err,
			)
			if cfg.Cache != nil {
				_, _, _, localErr := cfg.Cache.Blob(
					t.Context(),
					sourcecache.BlobKey{Content: "after-constructor-failure"},
					sourcecache.Policy{MaxAge: sourcecache.Forever},
					func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, localErr := io.WriteString(w, "caller-owned")
						return sourcecache.ProducerResult{}, localErr
					},
				)
				{
					assert.FatalIf(t, localErr != nil, "constructor released borrowed owner: %v", localErr)
				}
				assert.NoError(t, cfg.Cache.Close())
			}
			res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
			assert.FatalIf(
				t,
				err != nil || len(res.Errors) != 0,
				"constructor retained owner share: result=%+v err=%v",
				res,
				err,
			)
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_SubstitutionOwnershipAndIsolation(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}")
	source := suppliedSubstitution("external")
	cfg := treeConfig{Supply: []store.Declaration{source}, StrictSubstitutions: true, Path: root}
	first := newSubstitutionOrchestrator(t, cfg)
	second := newSubstitutionOrchestrator(t, cfg)
	source.Obj.(*manifest.ConfigMap).Data["VALUE"] = "caller-change"
	for _, o := range []*Orchestrator{first, second} {
		res, err := o.render(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		id := testutil.KustomizationID("apps", "consumer")
		if got := substitutionValue(t, res, id); got != "supplied" {
			t.Fatalf("owned values changed: %q", got)
		}
		external := source.Obj.Named()
		if !o.store.ReportHidden(external) {
			t.Fatal("supplied declaration gained production visibility")
		}
		if record, ok := o.index.Lookup(external); !ok || record.Declaration == nil {
			t.Fatal("supplied declaration missing from existence index")
		}
		if _, ok := filesForTest(o.index)[external]; ok {
			t.Fatal("supplied object entered file/change index")
		}
		if declarations := testutil.Declarations(
			o.store.Productions(external, nil),
		); len(declarations) > 0 ||
			len(renderersForTest(o.index, external)) != 0 {
			t.Fatal("supplied object entered producer indexes")
		}
		stored, _ := o.store.Get[*manifest.Kustomization](id)
		if len(stored.PostBuild.Substitute) != 0 {
			t.Fatal("preparation mutated stored Kustomization")
		}
		if info, _ := o.store.GetStatus(id); info.Status != store.StatusReady {
			t.Fatalf("consumer not ready: %+v", info)
		}
	}
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

			renderedSingleton1, err := RenderTrees(
				t.Context(),
				[]Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
				Config{
					CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: 2,
					Supply: tt.sources,
				},
			)
			assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
			res := renderedSingleton1[0].Result
			{
				_, ok := errors.AsType[*FailuresError](err)
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

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBootstrap_SubstitutionCollisions(t *testing.T) {
	for _, kind := range []string{"stored", "indexed file", "known Kustomization producer", "known Secret producer", "known Secret producer without declared keys"} {
		t.Run(kind, func(t *testing.T) {
			testBootstrapSubstitutionCollisionsCase(t, &kind)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestSupply_UnreadObjectsHaveNoPrecedenceWarning(t *testing.T) {
	sources := []store.Declaration{suppliedSubstitution("z"), suppliedSubstitution("a"), suppliedSubstitution("a")}
	o := newSubstitutionOrchestrator(t, treeConfig{Supply: sources, Path: t.TempDir()})
	for _, source := range sources {
		o.store.AddObject(source.Obj)
	}
	assert.NoError(t, o.bootstrap(t.Context()))
	assert.Diff(t, o.store.AdvisoryEvidence(), store.AdvisoryEvidence{})
	for _, warning := range o.store.Warnings() {
		if warning.Category == manifest.WarnSuppliedPrecedence {
			t.Fatal("unconsumed supply emitted precedence warning")
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_SubstitutionLateProducer(t *testing.T) {
	var canonical []byte
	for _, producerFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("producer_first_%v", producerFirst), func(t *testing.T) {
			testRenderSubstitutionLateProducerCase(
				t,
				testRenderSubstitutionLateProducerCaseInputs{canonical: &canonical, producerFirst: &producerFirst},
			)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_SubstitutionLateConsumer(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}")
	testutil.WriteFile(t, root, "flux.yaml", "null\n")
	scan := filepath.Join(root, "bootstrap")
	{
		err := os.MkdirAll(scan, 0o755)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	testutil.WriteFile(t, scan, "kustomization.yaml", "resources: []\n")
	o := newSubstitutionOrchestrator(
		t,
		treeConfig{
			Supply:              []store.Declaration{suppliedSubstitution("external")},
			StrictSubstitutions: true,
			Path:                scan,
			RepoRoot:            root,
		},
	)
	{
		err := o.bootstrap(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	child := testutil.KustomizationID("apps", "consumer")
	assert.FatalIf(t, o.store.GetObject(child) != nil, "%v", "consumer existed at bootstrap")
	rs := &manifest.ResourceSet{
		Name:      "generator",
		Namespace: "apps",
		ResourcesTemplate: substitutionKS(
			"consumer",
			"app",
			"  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: external}]\n",
		),
	}
	o.store.AddObject(rs)
	res, err := o.collectResult(o.run(t.Context()))
	assert.FatalIf(t, err != nil, "%v", err)
	{
		got := substitutionValue(t, res, child)
		assert.FatalIf(t, got != "supplied", "late consumer ignored accepted source: %q", got)
	}
}

func testBootstrapSubstitutionCollisionsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}")
	input := suppliedSubstitution("external")
	want := "repository"
	if (*testCaseInput) == "indexed file" {
		testutil.WriteFile(t, root, "producer.yaml", substitutionKS("producer", "producer", ""))
		testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [input.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"producer/input.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: external, namespace: apps}\ndata: {VALUE: repository}\n",
		)
	}
	if (*testCaseInput) == "known Kustomization producer" {
		testutil.WriteFile(t, root, "producer.yaml", substitutionKS("producer", "producer", ""))
		testutil.WriteFile(t, root, "producer/kustomization.yaml", "namespace: apps\nresources: [input.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"producer/input.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: external}\ndata: {VALUE: repository}\n",
		)
	}
	if (*testCaseInput) == "known Secret producer" ||
		(*testCaseInput) == "known Secret producer without declared keys" {
		testutil.WriteFile(t, root, "producer.yaml", substitutionKS("producer", "producer", ""))
		testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [external-secret.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"flux.yaml",
			substitutionKS(
				"consumer",
				"app",
				"  postBuild:\n    substituteFrom: [{kind: Secret, name: external}]\n",
			),
		)
		data := "  data:\n    - secretKey: VALUE\n      remoteRef: {key: fixture}\n"
		want = "private-fixture-value"
		if (*testCaseInput) == "known Secret producer without declared keys" {
			data = "  dataFrom:\n    - extract: {key: fixture}\n"
			want = "private-fixture-value"
		}
		testutil.WriteFile(
			t,
			root,
			"producer/external-secret.yaml",
			"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer, namespace: apps}\nspec:\n  target: {name: external}\n"+data,
		)
		input.Obj = &manifest.Secret{
			Name:       "external",
			Namespace:  "apps",
			StringData: map[string]any{"VALUE": "private-fixture-value"},
		}
		input.Doc = testutil.ObjectDocument(input.Obj)
	}
	if (*testCaseInput) != "stored" {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, producer.yaml]\n")
	}
	cfg := treeConfig{Supply: []store.Declaration{input}, Path: root}
	if (*testCaseInput) == "known Kustomization producer" {
		cfg.ExternalChanges = change.NewSet([]string{"app/cm.yaml"})
	}
	o := newSubstitutionOrchestrator(t, cfg)
	if (*testCaseInput) == "stored" {
		o.store.AddObject(
			&manifest.ConfigMap{
				Name:      "external",
				Namespace: "apps",
				Data:      map[string]any{"VALUE": "repository"},
			},
		)
	}
	{
		err := o.bootstrap(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	if (*testCaseInput) == "known Secret producer without declared keys" {
		id := input.Obj.Named()
		{
			declarations := testutil.Declarations(
				o.store.Productions(id, nil),
			)
			assert.FatalIf(t, len(declarations) == 0 ||
				!o.store.ReportHidden(id), "%v", "test requires a known producer without a synthesized Secret")
		}
	}
	assert.FatalIf(
		t,
		o.store.ReferenceFacts(input.Obj.Named(), nil, false, manifest.NamedResource{}).Data == nil,
		"%v",
		"repository evidence discarded supplied snapshot",
	)
	assert.FatalIf(
		t, (*testCaseInput) ==
			"indexed file" && !o.store.ReportHidden(input.Obj.Named()), "%v",
		"test requires unpromoted file identity",
	)
	if (*testCaseInput) == "known Kustomization producer" {
		producer := testutil.KustomizationID("apps", "producer")
		assert.FatalIf(t, !o.filter.ShouldReconcile(producer) ||
			!slices.Contains(
				testutil.Owners(o.store.Productions(input.Obj.Named(), o.index)),
				producer,
			), "%v", "known producer selection/edge lost")
	}
	{
		err := o.bootstrap(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	res, err := o.collectResult(o.run(t.Context()))
	assert.FatalIf(t, err != nil, "%v", err)
	id := testutil.KustomizationID("apps", "consumer")
	{
		got := substitutionValue(t, res, id)
		assert.FatalIf(t, got != want, "repository precedence got %q, want %q", got, want)
	}
	var wantOverrides []store.SuppliedOverride
	if !strings.HasPrefix((*testCaseInput), "known Secret producer") {
		wantOverrides = []store.SuppliedOverride{
			{Resource: input.Obj.Named(), SupplyPath: "ci-values.yaml", Keys: []string{"VALUE"}},
		}
	}
	assert.Diff(t, o.store.AdvisoryEvidence().Overrides, wantOverrides)
}

type testRenderSubstitutionLateProducerCaseInputs struct {
	canonical     *[]byte
	producerFirst *bool
}

func testRenderSubstitutionLateProducerCase(t *testing.T, testCaseInput testRenderSubstitutionLateProducerCaseInputs) {
	t.Helper()

	root := t.TempDir()
	writeSubstitutionCluster(t, root, "late-settings", "${VALUE}")
	producerDeps := ""
	if *testCaseInput.producerFirst {
		testutil.WriteFile(
			t,
			root,
			"flux.yaml",
			substitutionKS(
				"consumer",
				"app",
				"  dependsOn: [{name: producer}]\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: late-settings}]\n",
			),
		)
	} else {
		producerDeps = "  dependsOn: [{name: consumer}]\n"
	}
	testutil.WriteFile(t, root, "producer.yaml", substitutionKS("producer", "producer", producerDeps))
	testutil.WriteFile(t, root, "producer/kustomization.yaml", "namePrefix: late-\nresources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"producer/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n",
	)
	testutil.WriteFile(t, root, "independent.yaml", substitutionKS("independent", "independent", ""))
	testutil.WriteFile(t, root, "independent/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"independent/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent, namespace: apps}\ndata: {value: independent}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		"resources: [flux.yaml, producer.yaml, independent.yaml]\n",
	)
	source := suppliedSubstitution("late-settings")
	cfg := treeConfig{
		CacheDir:            t.TempDir(),
		WipeSecrets:         true,
		Concurrency:         2,
		Supply:              []store.Declaration{source},
		StrictSubstitutions: true,
		Path:                root,
		RepoRoot:            root,
	}
	o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		o.stop()
	})
	{
		{
			localErr := o.bootstrap(t.Context())
			assert.FatalIf(t, localErr != nil, "%v", localErr)
		}
	}
	external := source.Obj.Named()
	assert.FatalIf(
		t,
		o.store.ReferenceFacts(external, nil, false, manifest.NamedResource{}).Data == nil || !o.store.ReportHidden(external) ||
			len(renderersForTest(o.index, external)) != 0,
		"%v",
		"test requires producer unknown at bootstrap",
	)
	var mu sync.Mutex
	var order []string
	unsub := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		info, ok := payload.(store.StatusInfo)
		if ok && info.Status == store.StatusReady && id.Kind == manifest.KindKustomization &&
			(id.Name == "producer" || id.Name == "consumer") {
			mu.Lock()
			order = append(order, id.Name)
			mu.Unlock()
		}
	}, false)
	t.Cleanup(unsub)
	res, err := o.collectResult(o.run(t.Context()))
	assert.FatalIf(t, err != nil, "%v", err)
	consumer := testutil.KustomizationID("apps", "consumer")
	{
		got := substitutionValue(t, res, consumer)
		assert.FatalIf(t, got != "repository", "late repository production did not supersede supply: %q", got)
	}
	expected := []string{"consumer", "producer"}
	if *testCaseInput.producerFirst {
		expected = []string{"producer", "consumer"}
	}
	assert.FatalIf(
		t,
		len(order) < 2 || slices.Index(order, expected[0]) > slices.Index(order, expected[1]),
		"did not exercise required order: %v",
		order,
	)
	t.Logf("observed Ready order=%v", order)
	repository, _ := o.store.Get[*manifest.ConfigMap](external)
	assert.FatalIf(
		t,
		repository == nil || repository.Data["VALUE"] != "repository",
		"%v",
		"supplied input entered store or late producer did not render",
	)
	workloads := *res
	workloads.Manifests = slices.DeleteFunc(
		slices.Clone(res.Manifests),
		func(batch manifest.DocumentBatch) bool {
			return batch.Parent.Named() == o.entryBinding.Producer
		},
	)
	encoded, err := json.Marshal(canonicalBatches(&workloads))
	assert.FatalIf(t, err != nil, "%v", err)
	if (*testCaseInput.canonical) == nil {
		(*testCaseInput.canonical) = encoded
	} else if !bytes.Equal((*testCaseInput.canonical), encoded) {
		t.Fatalf(
			"output varies with late-producer order (-want +got): %s",
			cmp.Diff(string((*testCaseInput.canonical)), string(encoded)),
		)
	}
}
