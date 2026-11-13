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
	"github.com/home-operations/flate/pkg/loader"
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
	const cipher = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	source := suppliedSubstitution("external")
	cm := source.Obj.(*manifest.ConfigMap)
	raw := []byte("owned")
	cm.Data["BINARY"] = raw
	cm.Data["Z"], cm.Data["A"], cm.Data["EMPTY"] = cipher, cipher, ""
	source.Doc = testutil.ObjectDocument(cm)
	secretData, secretStringData := []byte("b3duZWQ="), []byte("owned")
	secret := &manifest.Secret{
		Name:       "secret",
		Namespace:  "apps",
		Data:       map[string]any{"ENCODED": secretData, "SECRET": cipher},
		StringData: map[string]any{"VALUE": secretStringData, "EMPTY": ""},
	}
	secretChild, outcome, _ := loader.Prepare(
		manifest.DocOccurrence{Document: testutil.ObjectDocument(secret)},
		manifest.ParseDocOptions{RequiredIdentity: true},
	)
	assert.NoError(t, outcome.Err)
	allowed := []string{manifest.KindSecret}
	customDoc := map[string]any{
		"apiVersion": "example.io/v1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "custom", "namespace": "apps"},
		"spec":       map[string]any{"payload": map[string]any{"value": "owned"}},
		"extra":      map[string]any{"retained": true},
	}
	sources := []store.Declaration{
		source,
		{
			Doc:       secretChild.Doc,
			Obj:       secretChild.Obj,
			InputPath: "secret-values.yaml",
			Origin:    manifest.SuppliedDeclaration,
			Warnings:  secretChild.Warnings,
		},
		{Doc: customDoc},
	}

	o := newSubstitutionOrchestrator(t, treeConfig{Supply: sources, AllowMissingKinds: allowed, Path: t.TempDir()})
	assert.Equal(t, cm.Data["A"], any(cipher))
	assert.Equal(t, source.Doc["data"].(map[string]any)["Z"], any(cipher))
	assert.Equal(t, secret.Data["SECRET"], any(cipher))
	secretChild.Warnings[0].Detail[0] = "caller-change"
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
	facts := o.store.ReferenceFacts(
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "external"},
		nil,
		manifest.NamedResource{},
	)
	assert.Equal(t, o.cfg.Supply[0].InputPath, "ci-values.yaml")
	for key, want := range map[string]string{"VALUE": "supplied", "BINARY": "owned", "EMPTY": ""} {
		result3, err := facts.Data.Value(key)
		got, present := result3.Value, result3.Found
		assert.NoError(t, err)
		assert.Equal(t, present, true)
		assert.Equal(t, got, want)
	}
	for _, key := range []string{"A", "Z"} {
		result4, err := facts.Data.Value(key)
		present := result4.Found
		assert.NoError(t, err)
		assert.Equal(t, present, false)
	}
	secretFacts := o.store.ReferenceFacts(secret.Named(), nil, manifest.NamedResource{})
	for _, key := range []string{"ENCODED", "VALUE"} {
		result5, err := secretFacts.Data.Value(key)
		got := result5.Value
		assert.NoError(t, err)
		assert.Equal(t, got, "owned")
	}
	result1, err := secretFacts.Data.Value("SECRET")
	present := result1.Found
	assert.NoError(t, err)
	assert.Equal(t, present, false)
	result2, err := secretFacts.Data.Value("EMPTY")
	empty, present := result2.Value, result2.Found
	assert.NoError(t, err)
	assert.Equal(t, present, true)
	assert.Equal(t, empty, "")
	warnings := o.store.Warnings()
	assert.Equal(t, len(warnings), 2)
	for _, warning := range warnings {
		assert.Equal(t, warning.Category, manifest.WarnSOPSUnreadable)
		assert.Equal(t, warning.Count, 1)
		want := []string{"SECRET"}
		if warning.Resource.Kind == manifest.KindConfigMap {
			want = []string{"A", "Z"}
		}
		assert.Diff(t, warning.Detail, want)
	}
	prepared := o.cfg
	assert.NoError(t, snapshotSubstitutions(&prepared.Config))
	repeated := store.New()
	for _, declaration := range prepared.Supply {
		repeated.AddDeclaration(declaration)
	}
	assert.Diff(t, repeated.Warnings(), warnings)
	assert.Diff(t, o.cfg.AllowMissingKinds, []string{manifest.KindSecret})
	assert.Equal(t, o.cfg.allowMissingSecrets, true)
	assert.Equal(t, o.loader.Options.Missing == o.cfg.missing, true)
	plain := newSubstitutionOrchestrator(t, treeConfig{Config: Config{}, Path: t.TempDir()})
	assert.Equal(t, plain.cfg.missing != nil, true)
	assert.Equal(t, plain.cfg.allowMissingSecrets, false)
	assert.FatalIf(
		t,
		plain.cfg.AllowMissingKinds != nil || plain.cfg.Supply != nil,
		"%s",
		"zero-input path allocated state",
	)
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
			_, err := renderTreesForTest(t.Context(), t,
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
			_, err := renderTreesForTest(t.Context(), t, []Tree{cfg.Tree}, cfg.Config)
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
					&sourcecache.Policy{MaxAge: sourcecache.Forever},
					func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, localErr := io.WriteString(w, "caller-owned")
						return sourcecache.ProducerResult{}, localErr
					},
				)

				assert.FatalIf(t, localErr != nil, "constructor released borrowed owner: %v", localErr)

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
		res, err := o.render(t.Context(), t)
		assert.FatalIf(t, err != nil, "%s", err)

		id := testutil.KustomizationID("apps", "consumer")
		if got := substitutionValue(t, res, id); got != "supplied" {
			t.Fatalf("owned values changed: %q", got)
		}
		external := source.Obj.Named()
		assert.FatalIf(t, !o.store.ReportHidden(external), "%s", "supplied declaration gained production visibility")

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
		assert.FatalIf(t, len(stored.PostBuild.Substitute) != 0, "%s", "preparation mutated stored Kustomization")

		if info, _ := o.store.GetStatus(id); info.Status != store.StatusReady {
			t.Fatalf("consumer not ready: %+v", info)
		}
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
		assert.FatalIf(
			t,
			warning.Category == manifest.WarnSuppliedPrecedence,
			"%s",
			"unconsumed supply emitted precedence warning",
		)
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
	res, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(res.Close)
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
		o.store.ReferenceFacts(input.Obj.Named(), nil, manifest.NamedResource{}).Data == nil,
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
		assert.FatalIf(t, !o.selection.Snapshot().Reconcile(producer) ||
			!slices.Contains(
				testutil.Owners(o.store.Productions(input.Obj.Named(), o.index)),
				producer,
			), "%v", "known producer selection/edge lost")
	}
	{
		err := o.bootstrap(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	res, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(res.Close)
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
		localErr := o.bootstrap(t.Context())
		assert.FatalIf(t, localErr != nil, "%v", localErr)
	}

	external := source.Obj.Named()
	supplied := o.store.ReferenceFacts(external, nil, manifest.NamedResource{}).Data
	assert.FatalIf(
		t,
		supplied == nil || !o.store.ReportHidden(external) ||
			o.store.HasInventory(testutil.KustomizationID("apps", "producer")),
		"%v",
		"producer output must remain absent at bootstrap",
	)
	assert.Diff(t, supplied.Snapshot(), source.Obj.(manifest.Data).Snapshot())
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
	res, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(res.Close)
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
