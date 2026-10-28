package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
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
	testutil.WriteFile(t, root, "flux.yaml", substitutionKS("consumer", "app", "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: "+reference+"}]\n"))
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: '"+expression+"'}\n")
}

func newSubstitutionOrchestrator(t *testing.T, cfg treeConfig) *Orchestrator {
	t.Helper()
	if cfg.RepoRoot == "" {
		cfg.RepoRoot = cfg.Path
	}
	cfg.CacheDir = t.TempDir()
	cfg.Concurrency = 4
	cfg.WipeSecrets = true
	o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		o.stop()
	})
	return o
}

func suppliedSubstitution(name string) store.SuppliedObject {
	return store.SuppliedObject{Path: "ci-values.yaml", Object: &manifest.ConfigMap{Name: name, Namespace: "apps", Data: map[string]any{"VALUE": "supplied"}}}
}

func substitutionValue(t *testing.T, res *Result, id manifest.NamedResource) string {
	t.Helper()
	docs := outputByParent(res.Manifests)[id]
	if len(docs) != 1 {
		t.Fatalf("expected one output for %s, got %d", id, len(docs))
	}
	value, _ := docs[0]["data"].(map[string]any)["value"].(string)
	return value
}

func TestSnapshotSubstitutions_OwnsInputs(t *testing.T) {
	source := suppliedSubstitution("external")
	cm := source.Object.(*manifest.ConfigMap)
	raw := []byte("owned")
	cm.Data["BINARY"] = raw
	secretData, secretStringData := []byte("b3duZWQ="), []byte("owned")
	secret := &manifest.Secret{Name: "secret", Namespace: "apps", Data: map[string]any{"ENCODED": secretData}, StringData: map[string]any{"VALUE": secretStringData}}
	allowed := []string{manifest.KindSecret}
	sources := []store.SuppliedObject{source, {Object: secret, Path: "secret-values.yaml"}}
	o := newSubstitutionOrchestrator(t, treeConfig{Supply: sources, AllowMissingKinds: allowed, Path: t.TempDir()})
	cm.Name, cm.Data["VALUE"] = "changed", "changed"
	raw[0], secretData[0], secretStringData[0] = 'x', 'x', 'x'
	sources[0].Path, allowed[0] = "changed", manifest.KindConfigMap
	facts := o.store.ReferenceFacts(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "external"}, nil)
	assert.Equal(t, facts.SupplyPath, "ci-values.yaml")
	for key, want := range map[string]string{"VALUE": "supplied", "BINARY": "owned"} {
		got, _, err := facts.Supplied.Value(key)
		assert.NoError(t, err)
		assert.Equal(t, got, want)
	}
	secretFacts := o.store.ReferenceFacts(secret.Named(), nil)
	for _, key := range []string{"ENCODED", "VALUE"} {
		got, _, err := secretFacts.Supplied.Value(key)
		assert.NoError(t, err)
		assert.Equal(t, got, "owned")
	}
	assert.Diff(t, o.cfg.AllowMissingKinds, []string{manifest.KindSecret})
	plain := newSubstitutionOrchestrator(t, treeConfig{Config: Config{}, Path: t.TempDir()})
	if plain.cfg.AllowMissingKinds != nil || plain.cfg.supplied != nil || plain.cfg.Supply != nil {
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
			_, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: t.TempDir()}}, Config{Supply: []store.SuppliedObject{{Object: tc.object}}})
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("expected input error, got %v", err)
			}
		})
	}
	for _, injected := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache/injected=%t", injected), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			cfg := treeConfig{CacheDir: root, Supply: []store.SuppliedObject{{Object: &manifest.ConfigMap{Namespace: "apps"}}}, Path: t.TempDir()}
			if injected {
				owner := cachetest.New(t, root, sourcecache.New)
				cfg.Cache = owner
			}
			cfg.ID = manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}
			_, err := RenderTrees(t.Context(), []Tree{cfg.Tree}, cfg.Config)
			if !errors.Is(err, manifest.ErrInput) || strings.Contains(err.Error(), "private-fixture-value") {
				t.Fatalf("invalid or unredacted SDK supply error: %v", err)
			}
			if cfg.Cache != nil {
				_, _, _, err := cfg.Cache.Blob(t.Context(), sourcecache.BlobKey{Content: "after-constructor-failure"}, sourcecache.Policy{MaxAge: sourcecache.Forever},
					func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, err := io.WriteString(w, "caller-owned")
						return sourcecache.ProducerResult{}, err
					})
				if err != nil {
					t.Fatalf("constructor released borrowed owner: %v", err)
				}
				assert.NoError(t, cfg.Cache.Close())
			}
			res, err := sourcecache.Sweep(t.Context(), root, sourcecache.SweepOpts{})
			if err != nil || len(res.Errors) != 0 {
				t.Fatalf("constructor retained owner share: result=%+v err=%v", res, err)
			}
		})
	}
}

func TestRender_SubstitutionOwnershipAndIsolation(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}")
	source := suppliedSubstitution("external")
	cfg := treeConfig{Supply: []store.SuppliedObject{source}, StrictSubstitutions: true, Path: root}
	first := newSubstitutionOrchestrator(t, cfg)
	second := newSubstitutionOrchestrator(t, cfg)
	source.Object.(*manifest.ConfigMap).Data["VALUE"] = "caller-change"
	for _, o := range []*Orchestrator{first, second} {
		res, err := o.render(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		id := testutil.KustomizationID("apps", "consumer")
		if got := substitutionValue(t, res, id); got != "supplied" {
			t.Fatalf("owned values changed: %q", got)
		}
		external := source.Object.Named()
		if o.store.GetObject(external) != nil {
			t.Fatal("supplied object entered store")
		}
		if _, ok := o.index.Lookup(external); ok {
			t.Fatal("supplied object entered existence index")
		}
		if _, ok := filesForTest(o.index)[external]; ok {
			t.Fatal("supplied object entered file/change index")
		}
		if declarations := testutil.Declarations(o.store.Productions(external, nil)); len(declarations) > 0 || len(renderersForTest(o.index, external)) != 0 {
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
		sources       []store.SuppliedObject
		missingSecret string
	}{
		{
			name:       "unrelated supplied Secret",
			references: "[{kind: Secret, name: absent}]",
			sources: []store.SuppliedObject{{
				Object: &manifest.Secret{Name: "missing", Namespace: "flux-system", StringData: map[string]any{"OTHER": "unrelated"}},
				Path:   "ci.yaml",
			}},
			missingSecret: "absent",
		},
		{
			name:       "same name in another namespace",
			references: "[{kind: Secret, name: missing}]",
			sources: []store.SuppliedObject{{
				Object: &manifest.Secret{Name: "missing", Namespace: "elsewhere", StringData: map[string]any{"OTHER": "unrelated"}},
				Path:   "ci.yaml",
			}},
			missingSecret: "missing",
		},
		{
			name:       "supplied and missing references",
			references: "[{kind: Secret, name: given}, {kind: Secret, name: missing}]",
			sources: []store.SuppliedObject{{
				Object: &manifest.Secret{Name: "given", Namespace: "flux-system", StringData: map[string]any{"OTHER": "unrelated"}},
				Path:   "ci.yaml",
			}},
			missingSecret: "missing",
		},
		{
			name:       "only supplied reference",
			references: "[{kind: Secret, name: missing}]",
			sources: []store.SuppliedObject{{
				Object: &manifest.Secret{Name: "missing", Namespace: "flux-system", StringData: map[string]any{"OTHER": "unrelated"}},
				Path:   "ci.yaml",
			}},
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
			testutil.WriteFile(t, dir, "charts/req/Chart.yaml", "apiVersion: v2\nname: req\nversion: 0.1.0\n")
			testutil.WriteFile(t, dir, "charts/req/templates/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  g: {{ required \"greeting is required\" .Values.greeting | quote }}\n")

			renderedSingleton1, err := RenderTrees(t.Context(), []Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: 2,
				Supply: tt.sources})
			if len(renderedSingleton1) != 1 {
				t.Fatalf("missing render: %v", err)
			}
			res := renderedSingleton1[0].Result
			if _, ok := errors.AsType[*FailuresError](err); !ok {
				t.Errorf("expected reconcile failure, got %v", err)
			}
			if res == nil {
				t.Fatal("missing render result")
			}
			demo := testutil.HelmReleaseID("apps", "demo")
			if tt.missingSecret == "" {
				if info, failed := res.Failed[demo]; !failed || !strings.Contains(info.Message, "greeting is required") {
					t.Errorf("required chart value failure lost: %+v", info)
				}
			} else {
				consumer := testutil.KustomizationID("flux-system", "apps")
				target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: consumer.Namespace, Name: tt.missingSecret}
				if !slices.Contains(res.Blocked[consumer], target) {
					t.Errorf("missing required reference admitted: %+v", res)
				}
			}
			if docs := outputByParent(res.Manifests)[demo]; len(docs) != 0 {
				t.Errorf("failed input published chart output: %v", docs)
			}
			for _, warning := range res.Warnings {
				if warning.Category == manifest.WarnUnresolvedSubstitution {
					t.Errorf("failed input reported successful substitution: %+v", warning)
				}
			}

		})
	}
}

func TestBootstrap_SubstitutionCollisions(t *testing.T) {
	for _, kind := range []string{"stored", "indexed file", "known Kustomization producer", "known Secret producer", "known Secret producer without declared keys"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			writeSubstitutionCluster(t, root, "external", "${VALUE}")
			input := suppliedSubstitution("external")
			want := "repository"
			if kind == "indexed file" {
				testutil.WriteFile(t, root, "producer.yaml", substitutionKS("producer", "producer", ""))
				testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [input.yaml]\n")
				testutil.WriteFile(t, root, "producer/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: external, namespace: apps}\ndata: {VALUE: repository}\n")
			}
			if kind == "known Kustomization producer" {
				testutil.WriteFile(t, root, "producer.yaml", substitutionKS("producer", "producer", ""))
				testutil.WriteFile(t, root, "producer/kustomization.yaml", "namespace: apps\nresources: [input.yaml]\n")
				testutil.WriteFile(t, root, "producer/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: external}\ndata: {VALUE: repository}\n")
			}
			if kind == "known Secret producer" || kind == "known Secret producer without declared keys" {
				testutil.WriteFile(t, root, "producer.yaml", substitutionKS("producer", "producer", ""))
				testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [external-secret.yaml]\n")
				testutil.WriteFile(t, root, "flux.yaml", substitutionKS("consumer", "app", "  postBuild:\n    substituteFrom: [{kind: Secret, name: external}]\n"))
				data := "  data:\n    - secretKey: VALUE\n      remoteRef: {key: fixture}\n"
				want = "private-fixture-value"
				if kind == "known Secret producer without declared keys" {
					data = "  dataFrom:\n    - extract: {key: fixture}\n"
					want = "private-fixture-value"
				}
				testutil.WriteFile(t, root, "producer/external-secret.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer, namespace: apps}\nspec:\n  target: {name: external}\n"+data)
				input.Object = &manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "private-fixture-value"}}
			}
			cfg := treeConfig{Supply: []store.SuppliedObject{input}, Path: root}
			if kind == "known Kustomization producer" {
				cfg.ExternalChanges = change.NewSet([]string{"app/cm.yaml"})
			}
			o := newSubstitutionOrchestrator(t, cfg)
			if kind == "stored" {
				o.store.AddObject(&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "repository"}})
			}
			if err := o.bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			if kind == "known Secret producer without declared keys" {
				id := input.Object.Named()
				if declarations := testutil.Declarations(o.store.Productions(id, nil)); len(declarations) == 0 || o.store.GetObject(id) != nil {
					t.Fatal("test requires a known producer without a synthesized Secret")
				}
			}
			if o.store.ReferenceFacts(input.Object.Named(), nil).Supplied == nil {
				t.Fatal("repository evidence discarded supplied snapshot")
			}
			if kind == "indexed file" && o.store.GetObject(input.Object.Named()) != nil {
				t.Fatal("test requires unpromoted file identity")
			}
			if kind == "known Kustomization producer" {
				producer := testutil.KustomizationID("apps", "producer")
				if !o.filter.ShouldReconcile(producer) || !slices.Contains(testutil.Owners(o.store.Productions(input.Object.Named(), o.index)), producer) {
					t.Fatal("known producer selection/edge lost")
				}
			}
			if err := o.bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			res, err := o.collectResult(o.run(t.Context()))
			if err != nil {
				t.Fatal(err)
			}
			id := testutil.KustomizationID("apps", "consumer")
			if got := substitutionValue(t, res, id); got != want {
				t.Fatalf("repository precedence got %q, want %q", got, want)
			}
			var wantOverrides []store.SuppliedOverride
			if !strings.HasPrefix(kind, "known Secret producer") {
				wantOverrides = []store.SuppliedOverride{{Resource: input.Object.Named(), SupplyPath: "ci-values.yaml", Keys: []string{"VALUE"}}}
			}
			assert.Diff(t, o.store.AdvisoryEvidence().Overrides, wantOverrides)
		})
	}
}

func TestSupply_UnreadObjectsHaveNoPrecedenceWarning(t *testing.T) {
	t.Parallel()
	sources := []store.SuppliedObject{suppliedSubstitution("z"), suppliedSubstitution("a"), suppliedSubstitution("a")}
	o := newSubstitutionOrchestrator(t, treeConfig{Supply: sources, Path: t.TempDir()})
	for _, source := range sources {
		o.store.AddObject(source.Object)
	}
	assert.NoError(t, o.bootstrap(t.Context()))
	assert.Diff(t, o.store.AdvisoryEvidence(), store.AdvisoryEvidence{})
	for _, warning := range o.store.Warnings() {
		if warning.Category == manifest.WarnSuppliedPrecedence {
			t.Fatal("unconsumed supply emitted precedence warning")
		}
	}
}

func TestRender_SubstitutionLateProducer(t *testing.T) {
	var canonical []byte
	for _, producerFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("producer_first_%v", producerFirst), func(t *testing.T) {
			root := t.TempDir()
			writeSubstitutionCluster(t, root, "late-settings", "${VALUE}")
			producerDeps := ""
			if producerFirst {
				testutil.WriteFile(t, root, "flux.yaml", substitutionKS("consumer", "app", "  dependsOn: [{name: producer}]\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: late-settings}]\n"))
			} else {
				producerDeps = "  dependsOn: [{name: consumer}]\n"
			}
			testutil.WriteFile(t, root, "producer.yaml", substitutionKS("producer", "producer", producerDeps))
			testutil.WriteFile(t, root, "producer/kustomization.yaml", "namePrefix: late-\nresources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "producer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n")
			testutil.WriteFile(t, root, "independent.yaml", substitutionKS("independent", "independent", ""))
			testutil.WriteFile(t, root, "independent/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "independent/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent, namespace: apps}\ndata: {value: independent}\n")
			source := suppliedSubstitution("late-settings")
			cfg := treeConfig{CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: 2, Supply: []store.SuppliedObject{source}, StrictSubstitutions: true, Path: root, RepoRoot: root}
			o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				o.stop()
			})
			if err := o.bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			external := source.Object.Named()
			if o.store.ReferenceFacts(external, nil).Supplied == nil || o.store.GetObject(external) != nil || len(renderersForTest(o.index, external)) != 0 {
				t.Fatal("test requires producer unknown at bootstrap")
			}
			var mu sync.Mutex
			var order []string
			unsub := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				info, ok := payload.(store.StatusInfo)
				if ok && info.Status == store.StatusReady && id.Kind == manifest.KindKustomization && (id.Name == "producer" || id.Name == "consumer") {
					mu.Lock()
					order = append(order, id.Name)
					mu.Unlock()
				}
			}, false)
			t.Cleanup(unsub)
			res, err := o.collectResult(o.run(t.Context()))
			if err != nil {
				t.Fatal(err)
			}
			consumer := testutil.KustomizationID("apps", "consumer")
			if got := substitutionValue(t, res, consumer); got != "repository" {
				t.Fatalf("late repository production did not supersede supply: %q", got)
			}
			expected := []string{"consumer", "producer"}
			if producerFirst {
				expected = []string{"producer", "consumer"}
			}
			if len(order) < 2 || slices.Index(order, expected[0]) > slices.Index(order, expected[1]) {
				t.Fatalf("did not exercise required order: %v", order)
			}
			t.Logf("observed Ready order=%v", order)
			repository, _ := o.store.Get[*manifest.ConfigMap](external)
			if repository == nil || repository.Data["VALUE"] != "repository" {
				t.Fatal("supplied input entered store or late producer did not render")
			}
			encoded, err := json.Marshal(canonicalBatches(res))
			if err != nil {
				t.Fatal(err)
			}
			if canonical == nil {
				canonical = encoded
			} else if !bytes.Equal(canonical, encoded) {
				t.Fatalf("output varies with late-producer order (-want +got): %s", cmp.Diff(string(canonical), string(encoded)))
			}
		})
	}
}

func TestRender_SubstitutionLateConsumer(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}")
	testutil.WriteFile(t, root, "flux.yaml", "null\n")
	scan := filepath.Join(root, "bootstrap")
	if err := os.MkdirAll(scan, 0o755); err != nil {
		t.Fatal(err)
	}
	o := newSubstitutionOrchestrator(t, treeConfig{Supply: []store.SuppliedObject{suppliedSubstitution("external")}, StrictSubstitutions: true, Path: scan, RepoRoot: root})
	if err := o.bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	child := testutil.KustomizationID("apps", "consumer")
	if o.store.GetObject(child) != nil {
		t.Fatal("consumer existed at bootstrap")
	}
	rs := &manifest.ResourceSet{Name: "generator", Namespace: "apps", ResourcesTemplate: substitutionKS("consumer", "app", "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: external}]\n")}
	o.store.AddObject(rs)
	res, err := o.collectResult(o.run(t.Context()))
	if err != nil {
		t.Fatal(err)
	}
	if got := substitutionValue(t, res, child); got != "supplied" {
		t.Fatalf("late consumer ignored accepted source: %q", got)
	}
}

func TestRenderTrees_SubstitutionAdmissionAndDiff(t *testing.T) {
	baseRoot, headRoot := t.TempDir(), t.TempDir()
	for i, root := range []string{baseRoot, headRoot} {
		expression := "${VALUE}-base"
		if i == 1 {
			expression = "${VALUE}-head"
		}
		writeSubstitutionCluster(t, root, "external", expression)
		testutil.WriteFile(t, root, "stable.yaml", substitutionKS("stable", "stable", "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: external}]\n"))
		testutil.WriteFile(t, root, "stable/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "stable/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: stable, namespace: apps}\ndata: {value: '${VALUE}'}\n")
	}
	source := suppliedSubstitution("external")
	cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 4, WipeSecrets: true, Supply: []store.SuppliedObject{source}, StrictSubstitutions: true, Tree: Tree{}}
	renderedTrees1, err := RenderTrees(t.Context(), []Tree{{RepoRoot: baseRoot, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: cfg.BaseInput, BaseInput: cfg.Input, ExternalChanges: cfg.ExternalChanges}, {RepoRoot: headRoot, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: cfg.Input, BaseInput: cfg.BaseInput, ExternalChanges: cfg.ExternalChanges}}, cfg.Config)
	base, head := renderedTrees1[0], renderedTrees1[1]
	if err != nil {
		t.Fatal(err)
	}
	consumer := testutil.KustomizationID("apps", "consumer")
	stable := testutil.KustomizationID("apps", "stable")
	for i, side := range []Rendered{base, head} {
		want := "supplied-base"
		if i == 1 {
			want = "supplied-head"
		}
		if got := substitutionValue(t, side.Result, consumer); got != want {
			t.Fatalf("changed consumer got %q, want %q", got, want)
		}
		if !side.filter.ShouldReconcile(consumer) || side.filter.ShouldReconcile(stable) || side.Store().GetArtifact(stable, store.ArtifactContent) != nil || side.Store().GetObject(source.Object.Named()) != nil {
			t.Fatal("selected-consumer admission or supplied-object isolation failed")
		}
	}
	for _, side := range []Rendered{base, head} {
		batches := canonicalBatches(side.Result)
		if len(batches) != 1 || batches[0].Parent != consumer || len(batches[0].Documents) != 1 {
			t.Fatalf("unexpected producer batches: %+v", batches)
		}
		doc := batches[0].Documents[0]
		meta, _ := doc["metadata"].(map[string]any)
		if doc["kind"] != manifest.KindConfigMap || meta["name"] != "rendered" {
			t.Fatalf("supplied input entered output: %+v", doc)
		}
	}
}

func TestRender_SubstitutionChangesDoNotSelectConsumers(t *testing.T) {
	baseRoot, headRoot := t.TempDir(), t.TempDir()
	for _, root := range []string{baseRoot, headRoot} {
		writeSubstitutionCluster(t, root, "external", "${VALUE}")
	}
	for _, value := range []string{"first", "second"} {
		source := suppliedSubstitution("external")
		source.Object.(*manifest.ConfigMap).Data["VALUE"] = value
		o := newSubstitutionOrchestrator(t, treeConfig{Supply: []store.SuppliedObject{source}, Path: headRoot, BaseInput: testDirectoryInput(t, baseRoot)})
		res, err := o.render(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		id := testutil.KustomizationID("apps", "consumer")
		if o.filter.ShouldReconcile(id) || len(res.Manifests) != 0 {
			t.Fatal("external input selected unchanged consumer")
		}
	}
}

func TestRenderTrees_SuppliedTransitionsMatchFull(t *testing.T) {
	t.Parallel()
	for _, transition := range [][2]string{{"absent", "real"}, {"wiped", "real"}, {"real", "absent"}} {
		t.Run(strings.Join(transition[:], "-"), func(t *testing.T) {
			t.Parallel()
			roots := []string{t.TempDir(), t.TempDir()}
			for i, root := range roots {
				writeSubstitutionCluster(t, root, "settings", "${VALUE}")
				if transition[i] != "absent" {
					value := "repository"
					if transition[i] == "wiped" {
						value = manifest.ValuePlaceholder("VALUE")
					}
					testutil.WriteFile(t, root, "settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: '"+value+"'}\n")
				}
			}
			cfg := Config{CacheDir: t.TempDir(), Concurrency: 2, WipeSecrets: true, Supply: []store.SuppliedObject{suppliedSubstitution("settings")}}
			consumer := testutil.KustomizationID("apps", "consumer")
			for range 2 {
				paired, err := RenderTrees(t.Context(), []Tree{{RepoRoot: roots[0], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}, {RepoRoot: roots[1], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, cfg)
				assert.NoError(t, err)
				for i, root := range roots {
					full, err := RenderTrees(t.Context(), []Tree{{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, cfg)
					assert.NoError(t, err)
					want := "supplied"
					if transition[i] == "real" {
						want = "repository"
					}
					assert.Equal(t, substitutionValue(t, full[0].Result, consumer), want)
					assert.Equal(t, substitutionValue(t, paired[i].Result, consumer), want)
					var wantWarnings []manifest.Warning
					if transition[i] == "real" {
						wantWarnings = []manifest.Warning{{Category: manifest.WarnSuppliedPrecedence, Message: "repository data takes precedence over supplied data", Detail: []string{"ConfigMap/apps/settings: VALUE (ci-values.yaml)"}, Count: 1}}
					}
					assert.Diff(t, full[0].Result.ClusterWarnings, wantWarnings)
					if i == 0 {
						assert.Equal(t, len(paired[i].Result.ClusterWarnings), 0)
					} else {
						assert.Diff(t, paired[i].Result.ClusterWarnings, []manifest.Warning{{Category: manifest.WarnSuppliedPrecedence, Message: "repository data takes precedence over supplied data", Detail: []string{"ConfigMap/apps/settings: VALUE (ci-values.yaml)"}, Count: 1}})
					}
				}
			}
		})
	}
}

func TestBootstrap_SubstitutionDuplicatesReplaceWholeObjects(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}-${OLD:-gone}")
	first := suppliedSubstitution("external")
	first.Object.(*manifest.ConfigMap).Data["OLD"] = "old"
	last := suppliedSubstitution("external")
	last.Object.(*manifest.ConfigMap).Data["VALUE"] = "last"
	o := newSubstitutionOrchestrator(t, treeConfig{Supply: []store.SuppliedObject{first, last}, Path: root})
	res, err := o.render(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id := testutil.KustomizationID("apps", "consumer")
	if got := substitutionValue(t, res, id); got != "last-gone" {
		t.Fatalf("duplicate maps merged: %q", got)
	}
	if !maps.Equal(o.store.ReferenceFacts(last.Object.Named(), nil).Supplied.(*manifest.ConfigMap).Data, last.Object.(*manifest.ConfigMap).Data) {
		t.Fatal("accepted input differs from last whole object")
	}
}

func TestSupply_IndexedIdentityDoesNotPublish(t *testing.T) {
	for _, namespace := range []string{"apps", "", "other"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			source := suppliedSubstitution("external")
			o := newSubstitutionOrchestrator(t, treeConfig{Supply: []store.SuppliedObject{source}, Path: t.TempDir()})
			id := source.Object.Named()
			indexed := id
			indexed.Namespace = namespace
			o.store.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{indexed: {Path: "repository.yaml"}}})
			if o.store.ReferenceFacts(id, nil).Supplied == nil || o.store.GetObject(id) != nil {
				t.Fatal("indexed evidence changed supply or published it")
			}
		})
	}
}

func canonicalBatches(res *Result) []struct {
	Parent    manifest.NamedResource
	Documents []map[string]any
} {
	var batches []struct {
		Parent    manifest.NamedResource
		Documents []map[string]any
	}
	for _, id := range slices.SortedFunc(maps.Keys(outputByParent(res.Manifests)), manifest.NamedResource.Compare) {
		batches = append(batches, struct {
			Parent    manifest.NamedResource
			Documents []map[string]any
		}{Parent: id, Documents: outputByParent(res.Manifests)[id]})
	}
	return batches
}

func TestRenderTrees_RequiredPostBuildNotWaived(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		for _, allowed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allowed=%t", kind, allowed), func(t *testing.T) {
				t.Parallel()
				roots := []string{t.TempDir(), t.TempDir()}
				trees := make([]Tree, 2)
				for i, root := range roots {
					testutil.WriteFile(t, root, "flux.yaml", substitutionKS("consumer", "app", "  postBuild: {substituteFrom: [{kind: "+kind+", name: absent}]}\n"))
					testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
					testutil.WriteFile(t, root, "app/cm.yaml", fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\ndata: {value: '${VALUE}-%d'}\n", i))
					trees[i] = Tree{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(i)}}
				}
				cfg := Config{Concurrency: 2, WipeSecrets: true}
				if allowed {
					cfg.AllowMissingKinds = []string{manifest.KindSecret, manifest.KindConfigMap, manifest.KindCustomResourceDefinition}
				}
				runs, err := RenderTrees(t.Context(), trees, cfg)
				if err == nil || len(runs) != 2 {
					t.Fatalf("paired required input admitted: %+v %v", runs, err)
				}
				for _, run := range runs {
					id := testutil.KustomizationID("apps", "consumer")
					if run.Result == nil || run.Result.Failed[id].Status != store.StatusFailed || len(outputByParent(run.Result.Manifests)[id]) != 0 {
						t.Fatalf("required input admitted on side %v: %+v", run.ID, run.Result)
					}
				}
			})
		}
	}
}
