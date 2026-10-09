package orchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func substitutionKS(name, path, postBuild string) string {
	return "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: " + name + ", namespace: apps}\nspec:\n  interval: 1m\n  path: ./" + path + "\n" + postBuild
}

func writeSubstitutionCluster(t *testing.T, root, reference, expression string) {
	t.Helper()
	testutil.WriteFile(t, root, "flux.yaml", substitutionKS("consumer", "app", "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: "+reference+"}]\n"))
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: '"+expression+"'}\n")
}

func newSubstitutionOrchestrator(t *testing.T, cfg Config) *Orchestrator {
	t.Helper()
	cfg.RepoRoot = cfg.Path
	cfg.CacheDir = t.TempDir()
	cfg.Concurrency = 4
	cfg.WipeSecrets = true
	o, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Stop)
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
	cm.BinaryData = map[string]any{"BINARY": raw}
	secret := &manifest.Secret{Name: "secret", Namespace: "apps", Data: map[string]any{"VALUE": "b3duZWQ="}, StringData: map[string]any{"VALUE": "owned"}}
	overlay := map[string]string{"VALUE": "owned"}
	sources := []SubstitutionSource{source, {Object: secret, Path: "secret-values.yaml"}}
	o := newSubstitutionOrchestrator(t, Config{Path: t.TempDir(), SubstituteFrom: sources, Substitute: overlay})
	cm.Name = "changed"
	cm.Data["VALUE"] = "changed"
	cm.Data["nested"].(map[string]any)["items"].([]any)[0] = "changed"
	raw[0] = 'A'
	secret.Data["VALUE"] = "changed"
	secret.StringData["VALUE"] = "changed"
	sources[0].Path = "changed"
	overlay["VALUE"] = "changed"
	owned := o.cfg.SubstituteFrom[0].Object.(*manifest.ConfigMap)
	if owned.Name != "external" || owned.Data["VALUE"] != "supplied" || owned.Data["nested"].(map[string]any)["items"].([]any)[0] != "owned" || string(owned.BinaryData["BINARY"].([]byte)) != "b3duZWQ=" || o.cfg.SubstituteFrom[0].Path != "ci-values.yaml" || o.cfg.Substitute["VALUE"] != "owned" {
		t.Fatal("SDK inputs alias caller memory")
	}
	ownedSecret := o.cfg.SubstituteFrom[1].Object.(*manifest.Secret)
	if ownedSecret.Data["VALUE"] != "b3duZWQ=" || ownedSecret.StringData["VALUE"] != "owned" {
		t.Fatal("Secret bags alias caller memory")
	}
	plain := newSubstitutionOrchestrator(t, Config{Path: t.TempDir()})
	if plain.cfg.Substitute != nil || plain.cfg.SubstituteFrom != nil || plain.substitutionSources != nil {
		t.Fatal("zero-option path allocated substitution state")
	}
}

func TestNew_SubstitutionValidation(t *testing.T) {
	var nilCM *manifest.ConfigMap
	var nilSecret *manifest.Secret
	for _, tc := range []struct {
		name   string
		object manifest.BaseManifest
	}{
		{"nil", nil}, {"typed nil ConfigMap", nilCM}, {"typed nil Secret", nilSecret},
		{"wrong type", &manifest.RawObject{Kind: manifest.KindConfigMap}},
		{"missing name", &manifest.ConfigMap{Namespace: "apps"}},
		{"missing namespace", &manifest.Secret{Name: "external"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(Config{Path: t.TempDir(), SubstituteFrom: []SubstitutionSource{{Object: tc.object}}})
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("expected input error, got %v", err)
			}
		})
	}
	_, err := New(Config{Path: t.TempDir(), Substitute: map[string]string{"bad-name": "private-fixture-value"}})
	if !errors.Is(err, manifest.ErrInput) || strings.Contains(err.Error(), "private-fixture-value") {
		t.Fatalf("invalid or unredacted SDK overlay error: %v", err)
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
		id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
		if got := substitutionValue(t, res, id); got != "overlay" {
			t.Fatalf("owned values changed: %q", got)
		}
		external := source.Object.Named()
		if o.store.GetObject(external) != nil {
			t.Fatal("supplied object entered store")
		}
		if _, ok := o.existence.Get(external); ok {
			t.Fatal("supplied object entered existence index")
		}
		if _, ok := o.sourceFiles[external]; ok {
			t.Fatal("supplied object entered file/change index")
		}
		if _, ok := o.producers.Producer(external); ok || len(o.selfProduce.ProducedBy(external)) != 0 {
			t.Fatal("supplied object entered producer indexes")
		}
		stored, _ := o.store.Get[*manifest.Kustomization](id)
		if len(stored.PostBuildSubstitute) != 0 || len(stored.PostBuild.Substitute) != 0 {
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
		for _, workers := range []int{2, 4} {
			t.Run(fmt.Sprintf("%s/workers_%d", tt.name, workers), func(t *testing.T) {
				rescued := tt.missingSecret != ""
				dir := t.TempDir()
				testutil.WriteFile(t, dir, "flux/apps.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.test/cluster.git
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substitute: {KEEP: "ok"}
    substituteFrom: `+tt.references+"\n")
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

				o, err := New(Config{
					Path: dir, RepoRoot: dir, CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: workers,
					SubstituteFrom: tt.sources,
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(o.Stop)
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
				demo := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "demo"}
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
					want = ""
				}
				testutil.WriteFile(t, root, "producer/external-secret.yaml", "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: producer, namespace: apps}\nspec:\n  target: {name: external}\n"+data)
				input.Object = &manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": "private-fixture-value"}}
			}
			var log bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&log, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
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
				if _, produced := o.producers.Producer(id); !produced || o.store.GetObject(id) != nil {
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
				producer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "producer"}
				if !o.filter.ShouldReconcile(producer) || !slices.Contains(o.filter.ProducersFor(input.Object.Named()), producer) {
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
			id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
			if got := substitutionValue(t, res, id); got != want {
				t.Fatalf("repository precedence got %q, want %q", got, want)
			}
			text := log.String()
			if strings.Count(text, "repository substitution source takes precedence") != 1 || !strings.Contains(text, "ci-values.yaml") || !strings.Contains(text, input.Object.Named().String()) || strings.Contains(text, "private-fixture-value") {
				t.Fatalf("collision warning incorrect: %s", text)
			}
		})
	}
}

func TestBootstrap_SubstitutionWarningsSortedAndDeduplicated(t *testing.T) {
	var log bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&log, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
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
	text := log.String()
	if strings.Count(text, "repository substitution source takes precedence") != 2 || strings.Index(text, "ConfigMap/apps/a") > strings.Index(text, "ConfigMap/apps/z") || strings.Contains(text, "first.yaml") || !strings.Contains(text, "last.yaml") {
		t.Fatalf("warnings not sorted/final-identity scoped: %s", text)
	}
}

func TestRender_SubstitutionLateProducer(t *testing.T) {
	var canonical []byte
	for _, workers := range []int{2, 4} {
		for _, producerFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("workers_%d_producer_first_%v", workers, producerFirst), func(t *testing.T) {
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
				cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: workers, SubstituteFrom: []SubstitutionSource{source}, StrictSubstitutions: true}
				o, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(o.Stop)
				if err := o.Bootstrap(t.Context()); err != nil {
					t.Fatal(err)
				}
				external := source.Object.Named()
				if o.substitutionSources[external] == nil || o.store.GetObject(external) != nil || len(o.selfProduce.ProducedBy(external)) != 0 {
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
				consumer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
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
				t.Logf("concurrency=%d observed Ready order=%v", workers, order)
				repository, _ := o.store.Get[*manifest.ConfigMap](external)
				if repository == nil || repository.Data["VALUE"] != "repository" {
					t.Fatal("supplied input entered store or late producer did not render")
				}
				encoded, err := json.Marshal(diff.DocsFromManifests(res.Manifests, nil))
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
}

func TestRender_SubstitutionLateConsumer(t *testing.T) {
	root := t.TempDir()
	writeSubstitutionCluster(t, root, "external", "${VALUE}")
	testutil.WriteFile(t, root, "flux.yaml", "null\n")
	o := newSubstitutionOrchestrator(t, Config{Path: root, SubstituteFrom: []SubstitutionSource{suppliedSubstitution("external")}, StrictSubstitutions: true})
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
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
	consumer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
	stable := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "stable"}
	for i, side := range []Rendered{base, head} {
		want := "overlay-base"
		if i == 1 {
			want = "overlay-head"
		}
		if got := substitutionValue(t, side.Result, consumer); got != want {
			t.Fatalf("changed consumer got %q, want %q", got, want)
		}
		if !side.Filter().ShouldReconcile(consumer) || side.Filter().ShouldReconcile(stable) || side.Store().GetArtifact(stable) != nil || side.Store().GetObject(source.Object.Named()) != nil {
			t.Fatal("selected-consumer admission or supplied-object isolation failed")
		}
	}
	changes := diff.Changes(diff.DocsFromManifests(base.Result.Manifests, nil), diff.DocsFromManifests(head.Result.Manifests, nil), diff.Options{})
	if len(changes) != 1 || changes[0].Kind != manifest.KindConfigMap || changes[0].Name != "rendered" || changes[0].Status != diff.StatusChanged {
		t.Fatalf("supplied input appeared in diff: %+v", changes)
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
		id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
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
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
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
			o.existence = loader.NewExistenceIndex()
			id := source.Object.Named()
			id.Namespace = namespace
			o.existence.Record(id, "repository.yaml")
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
