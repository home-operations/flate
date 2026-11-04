package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRS_EmissionGrouping(t *testing.T) {
	t.Parallel()

	for _, reverse := range []bool{false, true} {
		t.Run(strconv.FormatBool(reverse), func(t *testing.T) {
			t.Parallel()
			testRSEmissionGroupingCase(t, &reverse)
		},
		)
	}
}

func TestOrchestrator_Render(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)

	renderedSingleton3, err := RenderTrees(
		context.Background(),
		[]Tree{{Path: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{},
	)
	assert.FatalIf(t, len(renderedSingleton3) != 1, "missing render: %v", err)
	res := renderedSingleton3[0].Result
	assert.FatalIf(t, err != nil, "Render: %v", err)
	assert.FatalIf(t, res == nil, "%v", "Render returned nil Result")
	ksID := testutil.KustomizationID("flux-system", "apps")
	mans, ok := outputByParent(res.Manifests)[ksID]
	assert.FatalIf(t, !ok,
		"Result.Manifests missing %s; keys=%v", ksID, keysOf(outputByParent(res.Manifests)))

	assert.ErrorIf(t, len(mans) == 0, "expected rendered docs for %s; got empty", ksID)
	assert.ErrorIf(t, len(res.Failed) != 0,
		"expected no failures; got %v", res.Failed)
	assert.ErrorIf(t, len(res.Orphans) != 0,
		"expected no orphans; got %v", res.Orphans)

	again, err := RenderTrees(
		t.Context(),
		[]Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: dir}},
		Config{Concurrency: 2},
	)
	assert.NoError(t, err)
	assert.FatalIf(t, len(again) != 1, "%v", "second invocation lost tree")
	assert.Diff(t, canonicalBatches(again[0].Result), canonicalBatches(res))
}

func TestOrchestrator_TypedNullErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ field, diagnostic string }{
		{"patches", "must specify one of patch and path"},
		{"components", "new root cannot be empty"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			t.Parallel()
			dir := testutil.WriteTree(t, map[string]string{
				"ks.yaml": testutil.KustomizationYAML(
					"apps",
					"flux-system",
					"  path: ./apps\n  "+tc.field+": [null]\n",
				),
				"apps/kustomization.yaml": "resources: [cm.yaml]\n",
				"apps/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\n",
			})
			renderedSingleton1, err := RenderTrees(
				t.Context(),
				[]Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
				Config{CacheDir: t.TempDir(), Concurrency: 2},
			)
			assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
			res := renderedSingleton1[0].Result
			{
				_, ok := errors.AsType[*FailuresError](err)
				assert.FatalIf(t, !ok, "render error = %v, want resource failure", err)
			}
			assert.FatalIf(t, !strings.Contains(err.Error(), tc.diagnostic), "missing upstream diagnostic: %v", err)
			assert.FatalIf(t, res == nil, "%v", "missing render result")
			id := testutil.KustomizationID("flux-system", "apps")
			{
				info, ok := res.Failed[id]
				assert.FatalIf(
					t,
					!ok || !strings.Contains(info.String(), tc.diagnostic),
					"Kustomization failure = %+v, present = %t",
					info,
					ok,
				)
			}
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_Render_RSExtensionAttributedToParentKS(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", `namespace: flux-system
resources: [rsip.yaml]
generatorOptions:
  disableNameSuffixHash: true
configMapGenerator:
  - name: acl
    literals: [seed=value]
patches:
  - target: {kind: ConfigMap, name: acl}
    patch: |
      - op: replace
        path: /apiVersion
        value: fluxcd.controlplane.io/v1
      - op: replace
        path: /kind
        value: ResourceSet
      - op: remove
        path: /data
      - op: add
        path: /spec
        value:
          inputsFrom:
            - apiVersion: fluxcd.controlplane.io/v1
              kind: ResourceSetInputProvider
              selector:
                matchLabels: {role: db}
          resourcesTemplate: |
            apiVersion: external-secrets.io/v1
            kind: ExternalSecret
            metadata: {name: acl, namespace: flux-system}
            spec:
              target: {name: acl}
`)
	testutil.WriteFile(t, dir, "apps/rsip.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata:
  name: rsip
  namespace: flux-system
  labels: {role: db}
spec:
  type: Static
  defaultValues: {user: alice}
`)

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{Concurrency: 2})
	assert.FatalIf(t, err != nil, "New: %v", err)
	{
		localErr := o.bootstrap(t.Context())
		assert.FatalIf(t, localErr != nil, "%v", localErr)
	}
	rsID := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      manifest.KindResourceSet,
		Namespace: "flux-system",
		Name:      "acl",
	}
	{
		_, ok := o.index.Lookup(rsID)
		assert.FatalIf(t, ok, "%v", "fixture ResourceSet must only exist in render output")
	}
	res, err := o.collectResult(o.run(t.Context()))
	assert.FatalIf(t, err != nil, "Render: %v", err)
	ksID := testutil.KustomizationID("flux-system", "apps")
	{
		owners := testutil.Owners(o.Store().Productions(rsID, nil))
		assert.FatalIf(t, !slices.Contains(owners, ksID), "missing independent contributor: %v", owners)
	}
	mans := outputByParent(res.Manifests)[ksID]
	var found bool
	for _, doc := range mans {
		md, _ := doc["metadata"].(map[string]any)
		name, _ := md["name"].(string)
		kind, _ := doc["kind"].(string)
		if kind == "ExternalSecret" && name == "acl" {
			found = true
			break
		}
	}
	assert.ErrorIf(t, !found,

		"expected ExternalSecret/acl in outputByParent(res.Manifests)[%s] (the RS-rendered child); kinds in stream: %v",
		ksID,
		kindsOf(mans))
}

func kindsOf(docs []map[string]any) []string {
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		k, _ := d["kind"].(string)
		out = append(out, k)
	}
	return out
}

func TestOrchestrator_AllowMissingSecretsPropagates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	privateRegistryResult1 := privateRegistry(t)
	registryURL, registryConfig := privateRegistryResult1.url, privateRegistryResult1.credentials
	testutil.WriteFile(t, dir, "oci.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: private-app
  namespace: default
spec:
  interval: 5m
  insecure: true
  url: %s/private/app
  secretRef:
    name: ghcr-creds
`, registryURL))
	testutil.WriteFile(
		t,
		dir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"private-app",
			"default",
			"  interval: 5m\n  path: ./\n  sourceRef:\n    kind: OCIRepository\n    name: private-app\n    namespace: default\n",
		),
	)

	renderedSingleton4, err := RenderTrees(
		context.Background(),
		[]Tree{{Path: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{
			RegistryConfig:    registryConfig,
			AllowMissingKinds: []string{manifest.KindSecret, manifest.KindConfigMap},
		},
	)
	assert.FatalIf(t, len(renderedSingleton4) != 1, "missing render: %v", err)
	o := renderedSingleton4[0].Orchestrator
	res := renderedSingleton4[0].Result
	assert.FatalIf(t, err != nil, "Render: %v", err)

	ociID := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindOCIRepository,
		Namespace: "default",
		Name:      "private-app",
	}
	ksID := testutil.KustomizationID("default", "private-app")

	if _, failed := res.Failed[ociID]; failed {
		t.Errorf(
			"OCIRepository must not be in Failed when --allow-missing-kinds Secret is on; got %v",
			res.Failed[ociID],
		)
	}
	if _, failed := res.Failed[ksID]; failed {
		t.Errorf("dependent KS must propagate skip, not fail; got %v", res.Failed[ksID])
	}

	ociInfo, ok := o.Store().GetStatus(ociID)
	assert.ErrorIf(
		t,
		!ok || ociInfo.Outcome() != store.OutcomeSkipped,
		"OCIRepository should be Ready+skipped; got %+v",
		ociInfo,
	)
	ksInfo, ok := o.Store().GetStatus(ksID)
	assert.ErrorIf(
		t,
		!ok || ksInfo.Outcome() != store.OutcomeSkipped,
		"KS should be Ready+skipped; got %+v",
		ksInfo,
	)
}

func TestOrchestrator_AllowMissingSecretsHRSkipsBeforePull(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	privateRegistryResult2 := privateRegistry(t)
	registryURL, registryConfig := privateRegistryResult2.url, privateRegistryResult2.credentials
	testutil.WriteFile(t, dir, "oci.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: app-template
  namespace: flux-system
spec:
  interval: 5m
  insecure: true
  url: %s/private/chart
  secretRef:
    name: ghcr-creds
`, registryURL))
	testutil.WriteFile(t, dir, "hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: demo
  namespace: default
spec:
  interval: 5m
  chartRef:
    kind: OCIRepository
    name: app-template
    namespace: flux-system
`)

	renderedSingleton5, err := RenderTrees(
		context.Background(),
		[]Tree{{Path: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{
			RegistryConfig:    registryConfig,
			AllowMissingKinds: []string{manifest.KindSecret, manifest.KindConfigMap},
		},
	)
	assert.FatalIf(t, len(renderedSingleton5) != 1, "missing render: %v", err)
	o := renderedSingleton5[0].Orchestrator
	res := renderedSingleton5[0].Result
	assert.FatalIf(t, err != nil, "Render: %v", err)

	hrID := testutil.HelmReleaseID("default", "demo")
	if _, failed := res.Failed[hrID]; failed {
		t.Errorf("HR must propagate skip before helm.Template runs; got %v", res.Failed[hrID])
	}
	hrInfo, ok := o.Store().GetStatus(hrID)
	assert.ErrorIf(
		t,
		!ok || hrInfo.Outcome() != store.OutcomeSkipped,
		"HR should be Ready+skipped (chart source skipped); got %+v. "+
			"If status is Failed with a registry-pull error, the pre-check is missing and "+
			"helm tried an anonymous pull — exactly the silent-downgrade the pre-check exists to prevent.",
		hrInfo,
	)
}

func keysOf[V any](m map[manifest.NamedResource]V) []manifest.NamedResource {
	out := make([]manifest.NamedResource, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_ChangedOnlyKeepsSubstituteFromProducer(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"kustomization.yaml",
		"resources: [kubernetes/flux/cluster-apps.yaml, kubernetes/flux/cluster-vars.yaml]\n",
	)

	testutil.WriteFile(
		t,
		dir,
		"kubernetes/flux/cluster-apps.yaml",
		testutil.KustomizationYAML("cluster-apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`),
	)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/flux/cluster-vars.yaml",
		testutil.KustomizationYAML(
			"cluster-vars",
			"flux-system",
			"  interval: 10m\n  path: ./kubernetes/flux/vars\n  prune: true\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)
	testutil.WriteFile(t, dir, "kubernetes/flux/vars/kustomization.yaml", `namespace: flux-system
components:
  - ../../components/cluster-settings
`)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/components/cluster-settings/kustomization.yaml",
		`apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources:
  - cluster-settings.yaml
  - retained.yaml
`,
	)
	testutil.WriteFile(t, dir, "kubernetes/components/cluster-settings/cluster-settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
data:
  CLUSTER_DOMAIN: example.test
`)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/components/cluster-settings/retained.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: retained}\ndata: {value: kept}\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/consumed.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumed, namespace: flux-system}\ndata: {domain: '${CLUSTER_DOMAIN}'}\n",
	)
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", `resources:
  - communication/ntfy/ks.yaml
  - consumed.yaml
`)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/communication/ntfy/ks.yaml",
		testutil.KustomizationYAML("ntfy", "communication", `  interval: 10m
  path: ./kubernetes/apps/communication/ntfy/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`),
	)
	testutil.WriteFile(t, dir, "kubernetes/apps/communication/ntfy/app/kustomization.yaml", `resources:
  - helmrelease.yaml
`)
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/communication/ntfy/app/helmrelease.yaml",
		`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: ntfy
  namespace: communication
spec:
  interval: 10m
  suspend: true
  chartRef:
    kind: OCIRepository
    name: ntfy
    namespace: flux-system
`,
	)

	input := &countedProducerFS{FS: os.DirFS(dir)}
	artifact := &store.SourceArtifact{FS: input, LocalPath: dir, Key: "fixture-pin"}
	cacheDir := t.TempDir()
	prime, err := newTestOrchestrator(
		t,
		Tree{Path: dir, RepoRoot: dir, Input: artifact},
		Config{CacheDir: cacheDir, Concurrency: 2},
	)
	assert.NoError(t, err)
	assert.NoError(t, prime.bootstrap(t.Context()))
	assert.NoError(t, prime.run(t.Context()))
	prime.stop()
	coldReads := input.reads.Swap(0)

	o, err := newTestOrchestrator(t, Tree{
		Path: dir, RepoRoot: dir, Input: artifact,

		ExternalChanges: change.NewSet([]string{
			"kubernetes/apps/communication/ntfy/app/helmrelease.yaml",
		}),
	}, Config{
		CacheDir: cacheDir, Concurrency: 2,
	},
	)
	assert.FatalIf(t, err != nil, "New: %v", err)
	t.Cleanup(func() { o.stop() })
	{
		err := o.bootstrap(context.Background())
		assert.FatalIf(t, err != nil, "Bootstrap: %v", err)
	}

	producerID := testutil.KustomizationID("flux-system", "cluster-vars")
	f := o.filter
	assert.FatalIf(t, f == nil, "filter returned nil; ExternalChanges should have activated changed-only mode")
	assert.FatalIf(t, !f.ShouldReconcile(producerID),
		"producer %s must be in keep set; keep=%v", producerID, f.KeepNames())

	if err := o.run(context.Background()); err != nil {
		assert.FatalIf(
			t,
			strings.Contains(err.Error(), "ConfigMap/flux-system/cluster-settings: dependency not found"),
			"changed-only skipped unchanged substituteFrom producer: %v",
			err,
		)
		t.Fatalf("Run: %v", err)
	}

	{
		got := o.Store().GetArtifact(producerID, store.ArtifactContent)
		assert.ErrorIf(
			t,
			got == nil,
			"producer %s artifact missing after Run; producer was kept but never reconciled",
			producerID,
		)
	}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	{
		got := o.Store().GetObject(cmID)
		assert.ErrorIf(
			t,
			got == nil,
			"rendered %s missing after Run; producer reconcile did not materialize the substituteFrom CM",
			cmID,
		)
	}
	consumedID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "consumed"}
	consumed, ok := o.Store().GetObject(consumedID).(*manifest.ConfigMap)
	assert.FatalIf(
		t,
		!ok || consumed.Data["domain"] != "example.test",
		"producer data did not reach changed-only consumer: %#v",
		consumed,
	)
	retainedID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "retained"}
	assert.FatalIf(t, o.Store().GetObject(retainedID) == nil, "%v", "persisted producer lost its retained child")
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason == "" {
		{
			warmReads := input.reads.Load()
			assert.FatalIf(
				t,
				warmReads >= coldReads,
				"persisted producer rebuilt: cold reads=%d, warm reads=%d",
				coldReads,
				warmReads,
			)
		}
		t.Logf("persisted producer skipped source reads: cold=%d warm=%d", coldReads, input.reads.Load())
	} else {
		t.Logf("persisted publication proof ineligible: %s", reason)
	}
}

type countedProducerFS struct {
	fs.FS

	reads atomic.Int32
}

func testRSEmissionGroupingCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	s := store.New()
	parent := testutil.KustomizationID("flux-system", "parent")
	s.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
	a := &manifest.ResourceSet{Name: "a", Namespace: "apps"}
	b := &manifest.ResourceSet{Name: "b", Namespace: "apps"}
	s.AddObject(a)
	s.AddObject(b)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			a.Named(): {Parent: parent},
			b.Named(): {Parent: parent},
		},
	}
	o := &Orchestrator{store: s, index: index}
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "shared", "namespace": "apps"},
		"data":       map[string]any{"value": "equal"},
	}
	parseChildrenForTestResult3 := parseChildrenForTest(parent, []map[string]any{doc}, manifest.ParseDocOptions{})
	children, docs := parseChildrenForTestResult3.Children, parseChildrenForTestResult3.Documents
	ids := []manifest.NamedResource{a.Named(), b.Named()}
	if *testCaseInput {
		slices.Reverse(ids)
	}
	for _, id := range ids {
		s.RecordEmitted(
			id,
			&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true},
			nil,
		)
	}
	assert.Diff(
		t,
		outputByParent(o.collectManifests()),
		map[manifest.NamedResource][]map[string]any{parent: docs},
	)
	second := manifest.DeepCopyMap(doc)
	second["metadata"].(map[string]any)["name"] = "second"
	for _, ordered := range [][]map[string]any{{doc, second}, {second, doc}} {
		parseChildrenForTestResult4 := parseChildrenForTest(b.Named(), ordered, manifest.ParseDocOptions{})
		entries, authoritative := parseChildrenForTestResult4.Children, parseChildrenForTestResult4.Documents
		s.RecordEmitted(
			b.Named(),
			&store.Emission{
				Children:  entries,
				Manifests: testutil.DocumentOccurrences(authoritative),
				Complete:  true,
			},
			nil,
		)
		{
			got := outputByParent(o.collectManifests())[parent]
			assert.FatalIf(t, len(got) != 2 ||
				resourceset.DedupKey(
					got[0],
				) != resourceset.DedupKey(
					ordered[0],
				), "authoritative document order lost: %v", got)
		}
	}
	unequal := manifest.DeepCopyMap(doc)
	unequal["data"] = map[string]any{"value": "different"}
	parseChildrenForTestResult5 := parseChildrenForTest(parent, []map[string]any{unequal}, manifest.ParseDocOptions{})
	changed, changedDocs := parseChildrenForTestResult5.Children, parseChildrenForTestResult5.Documents
	for _, id := range ids {
		if id == a.Named() {
			s.RecordEmitted(
				id,
				&store.Emission{
					Children:  children,
					Manifests: testutil.DocumentOccurrences(docs),
					Complete:  true,
				},
				nil,
			)
		} else {
			s.RecordEmitted(
				id,
				&store.Emission{
					Children:  changed,
					Manifests: testutil.DocumentOccurrences(changedDocs),
					Complete:  true,
				},
				nil,
			)
		}
	}
	assert.Diff(
		t,
		outputByParent(o.collectManifests()),
		map[manifest.NamedResource][]map[string]any{parent: changedDocs},
	)
}
