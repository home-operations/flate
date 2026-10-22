package orchestrator

import (
	"bytes"
	"context"
	"encoding/base64"
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

func newSubstitutionOrchestrator(t *testing.T, cfg Config) *Orchestrator {
	t.Helper()
	if cfg.RepoRoot == "" {
		cfg.RepoRoot = cfg.Path
	}
	cfg.CacheDir = t.TempDir()
	cfg.Concurrency = 4
	cfg.WipeSecrets = true
	o, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := o.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return o
}

func suppliedSubstitution(name string) SubstitutionSource {
	return SubstitutionSource{Path: "ci-values.yaml", Object: &manifest.ConfigMap{Name: name, Namespace: "apps", Data: map[string]any{"VALUE": "supplied"}}}
}

func substitutionValue(t *testing.T, res *Result, id manifest.NamedResource) string {
	t.Helper()
	docs := res.Manifests[id]
	if len(docs) != 1 {
		t.Fatalf("expected one output for %s, got %d", id, len(docs))
	}
	value, _ := docs[0]["data"].(map[string]any)["value"].(string)
	return value
}

func TestNew_SubstitutionSnapshot(t *testing.T) {
	source := suppliedSubstitution("external")
	cm := source.Object.(*manifest.ConfigMap)
	cm.Data["nested"] = map[string]any{"items": []any{"owned"}}
	raw := []byte("b3duZWQ=")
	cm.Data["BINARY"] = raw
	const cipher = "ENC[AES256_GCM,data:fixture,type:str]"
	cm.Data["CIPHER"] = cipher
	secretData, secretStringData := []byte("b3duZWQ="), []byte("owned")
	secret := &manifest.Secret{Name: "secret", Namespace: "apps", Data: map[string]any{"VALUE": "b3duZWQ=", "BINARY": secretData}, StringData: map[string]any{"VALUE": "owned", "BINARY": secretStringData}}
	secret.Data["ENCODED"] = base64.StdEncoding.EncodeToString([]byte(cipher))
	secret.StringData["CIPHER"] = cipher
	overlay := map[string]string{"VALUE": "owned"}
	sources := []SubstitutionSource{source, {Object: secret, Path: "secret-values.yaml"}}
	o := newSubstitutionOrchestrator(t, Config{Path: t.TempDir(), SubstituteFrom: sources, Substitute: overlay})
	cm.Name = "changed"
	cm.Data["VALUE"] = "changed"
	cm.Data["nested"].(map[string]any)["items"].([]any)[0] = "changed"
	raw[0] = 'A'
	secretData[0], secretStringData[0] = 'A', 'A'
	secret.Data["VALUE"] = "changed"
	secret.StringData["VALUE"] = "changed"
	sources[0].Path = "changed"
	overlay["VALUE"] = "changed"
	owned := o.cfg.SubstituteFrom[0].Object.(*manifest.ConfigMap)
	if owned.Name != "external" || owned.Data["VALUE"] != "supplied" || owned.Data["nested"].(map[string]any)["items"].([]any)[0] != "owned" || string(owned.Data["BINARY"].([]byte)) != "b3duZWQ=" || o.cfg.SubstituteFrom[0].Path != "ci-values.yaml" || o.cfg.Substitute["VALUE"] != "owned" {
		t.Fatal("SDK inputs alias caller memory")
	}
	ownedSecret := o.cfg.SubstituteFrom[1].Object.(*manifest.Secret)
	if ownedSecret.Data["VALUE"] != "b3duZWQ=" || ownedSecret.StringData["VALUE"] != "owned" || string(ownedSecret.Data["BINARY"].([]byte)) != "b3duZWQ=" || string(ownedSecret.StringData["BINARY"].([]byte)) != "owned" {
		t.Fatal("Secret bags alias caller memory")
	}
	decoded, _, err := ownedSecret.Value("ENCODED")
	if err != nil || !manifest.HasValuePlaceholder(decoded) || !manifest.HasValuePlaceholder(owned.Data["CIPHER"].(string)) || !manifest.HasValuePlaceholder(ownedSecret.StringData["CIPHER"].(string)) || cm.Data["CIPHER"] != cipher || secret.StringData["CIPHER"] != cipher {
		t.Fatal("snapshot sanitation lost ciphertext handling or mutated SDK input")
	}
	plain := newSubstitutionOrchestrator(t, Config{Path: t.TempDir()})
	if plain.cfg.Substitute != nil || plain.cfg.SubstituteFrom != nil || plain.substitutionSources != nil {
		t.Fatal("zero-option path allocated substitution state")
	}
}

func TestNew_SubstitutionValidation(t *testing.T) {
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
			_, err := New(t.Context(), Config{Path: t.TempDir(), SubstituteFrom: []SubstitutionSource{{Object: tc.object}}})
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("expected input error, got %v", err)
			}
		})
	}
	for _, injected := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache/injected=%t", injected), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			cfg := Config{Path: t.TempDir(), CacheDir: root, Substitute: map[string]string{"bad-name": "private-fixture-value"}}
			if injected {
				owner := cachetest.New(t, root, sourcecache.New)
				cfg.Cache = owner
			}
			_, err := New(t.Context(), cfg)
			if !errors.Is(err, manifest.ErrInput) || strings.Contains(err.Error(), "private-fixture-value") {
				t.Fatalf("invalid or unredacted SDK overlay error: %v", err)
			}
			if cfg.Cache != nil {
				r, _, _, err := cfg.Cache.Blob(t.Context(), sourcecache.BlobKey{Content: "after-constructor-failure"}, sourcecache.Policy{MaxAge: sourcecache.Forever},
					func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, err := io.WriteString(w, "caller-owned")
						return sourcecache.ProducerResult{}, err
					})
				if err != nil {
					t.Fatalf("constructor released borrowed owner: %v", err)
				}
				if err := errors.Join(r.Close(), cfg.Cache.Close()); err != nil {
					t.Fatal(err)
				}
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
	cfg := Config{Path: root, SubstituteFrom: []SubstitutionSource{source}, Substitute: map[string]string{"VALUE": "overlay"}, StrictSubstitutions: true}
	first := newSubstitutionOrchestrator(t, cfg)
	second := newSubstitutionOrchestrator(t, cfg)
	cfg.Substitute["VALUE"] = "caller-change"
	source.Object.(*manifest.ConfigMap).Data["VALUE"] = "caller-change"
	for _, o := range []*Orchestrator{first, second} {
		res, err := o.Render(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		id := testutil.KustomizationID("apps", "consumer")
		if got := substitutionValue(t, res, id); got != "overlay" {
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

func TestRender_SubstitutionSuppliedSecretDoesNotRescueHelmRelease(t *testing.T) {
	tests := []struct {
		name          string
		references    string
		sources       []SubstitutionSource
		missingSecret string
	}{
		{
			name:       "unrelated supplied Secret",
			references: "[{kind: Secret, name: absent}]",
			sources: []SubstitutionSource{{
				Object: &manifest.Secret{Name: "missing", Namespace: "flux-system", StringData: map[string]any{"OTHER": "unrelated"}},
				Path:   "ci.yaml",
			}},
			missingSecret: "absent",
		},
		{
			name:       "same name in another namespace",
			references: "[{kind: Secret, name: missing}]",
			sources: []SubstitutionSource{{
				Object: &manifest.Secret{Name: "missing", Namespace: "elsewhere", StringData: map[string]any{"OTHER": "unrelated"}},
				Path:   "ci.yaml",
			}},
			missingSecret: "missing",
		},
		{
			name:       "supplied and missing references",
			references: "[{kind: Secret, name: given}, {kind: Secret, name: missing}]",
			sources: []SubstitutionSource{{
				Object: &manifest.Secret{Name: "given", Namespace: "flux-system", StringData: map[string]any{"OTHER": "unrelated"}},
				Path:   "ci.yaml",
			}},
			missingSecret: "missing",
		},
		{
			name:       "only supplied reference",
			references: "[{kind: Secret, name: missing}]",
			sources: []SubstitutionSource{{
				Object: &manifest.Secret{Name: "missing", Namespace: "flux-system", StringData: map[string]any{"OTHER": "unrelated"}},
				Path:   "ci.yaml",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rescued := tt.missingSecret != ""
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

			o, err := New(t.Context(), Config{
				Path: dir, RepoRoot: dir, CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: 2,
				SubstituteFrom: tt.sources,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			res, err := o.Render(t.Context())
			if rescued {
				if err != nil {
					t.Errorf("expected rescued render, got %v", err)
				}
			} else if _, ok := errors.AsType[*FailuresError](err); !ok {
				t.Errorf("expected reconcile failure, got %v", err)
			}
			if res == nil {
				t.Fatal("missing render result")
			}
			demo := testutil.HelmReleaseID("apps", "demo")
			if _, failed := res.Failed[demo]; failed == rescued {
				t.Errorf("HelmRelease failed = %v, want %v", failed, !rescued)
			}
			placeholder := false
			for id, docs := range res.Manifests {
				for _, doc := range docs {
					if manifest.ContainsValuePlaceholder(doc) {
						placeholder = true
						if !rescued {
							t.Errorf("unexpected rescue placeholder in %s: %v", id, doc)
						}
					}
				}
			}
			if rescued && !placeholder {
				t.Error("rescued render must contain a value placeholder")
			}
			rescueWarning, missingWarning := false, false
			for _, warning := range res.Warnings {
				if warning.Resource == demo && warning.Category == manifest.WarnUnresolvedSubstitution && slices.Contains(warning.Detail, "greeting") {
					rescueWarning = true
					if !rescued {
						t.Errorf("unexpected rescue warning: %+v", warning)
					}
				}
				if warning.Category == manifest.WarnUnresolvedSubstitution && strings.Contains(warning.Message, fmt.Sprintf("Secret %q", tt.missingSecret)) {
					missingWarning = true
				}
			}
			if rescued && (!rescueWarning || !missingWarning) {
				t.Errorf("missing unresolved-substitution warnings: rescue = %v, missing Secret = %v", rescueWarning, missingWarning)
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
				want = "..PLACEHOLDER_VALUE.."
				if kind == "known Secret producer without declared keys" {
					data = "  dataFrom:\n    - extract: {key: fixture}\n"
					want = "${VALUE}"
				}
				testutil.WriteFile(t, root, "producer/external-secret.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer, namespace: apps}\nspec:\n  target: {name: external}\n"+data)
				input.Object = &manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "private-fixture-value"}}
			}
			cfg := Config{Path: root, SubstituteFrom: []SubstitutionSource{input}}
			if kind == "known Kustomization producer" {
				cfg.ExternalChanges = change.NewSet([]string{"app/cm.yaml"})
			}
			o := newSubstitutionOrchestrator(t, cfg)
			if kind == "stored" {
				o.store.AddObject(&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "repository"}})
			}
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			if kind == "known Secret producer without declared keys" {
				id := input.Object.Named()
				if declarations := testutil.Declarations(o.store.Productions(id, nil)); len(declarations) == 0 || o.store.GetObject(id) != nil {
					t.Fatal("test requires a known producer without a synthesized Secret")
				}
			}
			if o.substitutionSources[input.Object.Named()] != nil {
				t.Fatal("bootstrap collision accepted supplied source")
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
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			res, err := o.Render(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			id := testutil.KustomizationID("apps", "consumer")
			if got := substitutionValue(t, res, id); got != want {
				t.Fatalf("repository precedence got %q, want %q", got, want)
			}
			var collisions []manifest.Warning
			for _, warning := range res.Warnings {
				if warning.Category == manifest.WarnSubstitutionPrecedence {
					collisions = append(collisions, warning)
				}
			}
			wantWarning := manifest.Warning{Resource: input.Object.Named(), Category: manifest.WarnSubstitutionPrecedence,
				Message: "repository substitution source takes precedence", Detail: []string{"ci-values.yaml"}, Count: 1}
			assert.Diff(t, collisions, []manifest.Warning{wantWarning})
		})
	}
}

func TestBootstrap_SubstitutionWarningsSortedAndDeduplicated(t *testing.T) {
	t.Parallel()
	sources := []SubstitutionSource{suppliedSubstitution("z"), suppliedSubstitution("a"), suppliedSubstitution("a")}
	sources[1].Path = "first.yaml"
	sources[2].Path = "last.yaml"
	o := newSubstitutionOrchestrator(t, Config{Path: t.TempDir(), SubstituteFrom: sources})
	for _, s := range sources {
		o.store.AddObject(s.Object)
	}
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	warnings := o.store.Warnings()
	if len(warnings) != 2 || warnings[0].Resource.Name != "a" || warnings[1].Resource.Name != "z" || warnings[0].Detail[0] != "last.yaml" {
		t.Fatalf("warnings not sorted/final-identity scoped: %+v", warnings)
	}
	for _, warning := range warnings {
		assert.Equal(t, warning.Category, manifest.WarnSubstitutionPrecedence)
		assert.Equal(t, warning.Count, 1)
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
			cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: 2, SubstituteFrom: []SubstitutionSource{source}, StrictSubstitutions: true}
			o, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			})
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			external := source.Object.Named()
			if o.substitutionSources[external] == nil || o.store.GetObject(external) != nil || len(renderersForTest(o.index, external)) != 0 {
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
			res, err := o.Render(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			consumer := testutil.KustomizationID("apps", "consumer")
			if got := substitutionValue(t, res, consumer); got != "supplied" {
				t.Fatalf("late producer revoked accepted source: %q", got)
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
	o := newSubstitutionOrchestrator(t, Config{Path: scan, RepoRoot: root, SubstituteFrom: []SubstitutionSource{suppliedSubstitution("external")}, StrictSubstitutions: true})
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	child := testutil.KustomizationID("apps", "consumer")
	if o.store.GetObject(child) != nil {
		t.Fatal("consumer existed at bootstrap")
	}
	rs := &manifest.ResourceSet{Name: "generator", Namespace: "apps", ResourcesTemplate: substitutionKS("consumer", "app", "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: external}]\n")}
	o.store.AddObject(rs)
	res, err := o.Render(t.Context())
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
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 4, WipeSecrets: true, SubstituteFrom: []SubstitutionSource{source}, Substitute: map[string]string{"VALUE": "overlay"}, StrictSubstitutions: true}
	base, head, err := RenderTrees(t.Context(), Tree{RepoRoot: baseRoot}, Tree{RepoRoot: headRoot}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	consumer := testutil.KustomizationID("apps", "consumer")
	stable := testutil.KustomizationID("apps", "stable")
	for i, side := range []Rendered{base, head} {
		want := "overlay-base"
		if i == 1 {
			want = "overlay-head"
		}
		if got := substitutionValue(t, side.Result, consumer); got != want {
			t.Fatalf("changed consumer got %q, want %q", got, want)
		}
		if !side.filter.ShouldReconcile(consumer) || side.filter.ShouldReconcile(stable) || side.Store().GetArtifact(stable) != nil || side.Store().GetObject(source.Object.Named()) != nil {
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
		o := newSubstitutionOrchestrator(t, Config{Path: headRoot, PathOrig: baseRoot, SubstituteFrom: []SubstitutionSource{source}, Substitute: map[string]string{"VALUE": value}})
		res, err := o.Render(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		id := testutil.KustomizationID("apps", "consumer")
		if o.filter.ShouldReconcile(id) || len(res.Manifests) != 0 {
			t.Fatal("external input/overlay selected unchanged consumer")
		}
	}
}

func TestBootstrap_SubstitutionDuplicatesReplaceWholeObjects(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}-${OLD:-gone}")
	first := suppliedSubstitution("external")
	first.Object.(*manifest.ConfigMap).Data["OLD"] = "old"
	last := suppliedSubstitution("external")
	last.Object.(*manifest.ConfigMap).Data["VALUE"] = "last"
	o := newSubstitutionOrchestrator(t, Config{Path: root, SubstituteFrom: []SubstitutionSource{first, last}})
	res, err := o.Render(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id := testutil.KustomizationID("apps", "consumer")
	if got := substitutionValue(t, res, id); got != "last-gone" {
		t.Fatalf("duplicate maps merged: %q", got)
	}
	if !maps.Equal(o.substitutionSources[last.Object.Named()].(*manifest.ConfigMap).Data, last.Object.(*manifest.ConfigMap).Data) {
		t.Fatal("accepted input differs from last whole object")
	}
}

func TestFreezeSubstitutions_ExactFileIdentity(t *testing.T) {
	for _, namespace := range []string{"apps", "", "other"} {
		t.Run("namespace_"+namespace, func(t *testing.T) {
			source := suppliedSubstitution("external")
			o := newSubstitutionOrchestrator(t, Config{Path: t.TempDir(), SubstituteFrom: []SubstitutionSource{source}})
			o.index = indexForTest(nil)
			id := source.Object.Named()
			id.Namespace = namespace
			o.index.Records[id] = manifest.DiscoveryRecord{Path: "repository.yaml"}
			o.freezeSubstitutions()
			if got := o.substitutionSources[source.Object.Named()] != nil; got != (namespace != "apps") {
				t.Fatalf("accepted=%v for indexed namespace %q", got, namespace)
			}
			if o.store.GetObject(source.Object.Named()) != nil {
				t.Fatal("eligibility promoted file/source into store")
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
	for _, id := range slices.SortedFunc(maps.Keys(res.Manifests), manifest.NamedResource.Compare) {
		batches = append(batches, struct {
			Parent    manifest.NamedResource
			Documents []map[string]any
		}{Parent: id, Documents: res.Manifests[id]})
	}
	return batches
}
