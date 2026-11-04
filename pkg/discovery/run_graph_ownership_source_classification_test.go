package discovery_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRun_GraphOwnershipSourceClassification(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"bootstrap", "self", "self_unavailable", "external", "external_ancestor", "missing_oci", "missing_bucket", "declared_oci", "declared_bucket"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testRunGraphOwnershipSourceClassificationCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func TestRun_ValuesFromOnlyChangeSelectsConsumer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ namespace, apiVersion string }{
		{"apps", "v1"}, {"other", "v1"}, {"apps", "foreign.example/v1"},
	} {
		t.Run(tc.namespace+"/"+tc.apiVersion, func(t *testing.T) {
			t.Parallel()
			ns := tc.namespace
			root := t.TempDir()
			for _, file := range []struct{ path, body string }{
				{"flux/producer.yaml", testutil.KustomizationYAML("producer", "flux-system", "  {path: ./producer}\n")},
				{"flux/consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  {path: ./consumer}\n")},
				{"producer/kustomization.yaml", "namespace: apps\nresources: [values.yaml]\n"},
				{"producer/values.yaml", "apiVersion: " + tc.apiVersion + "\nkind: ConfigMap\nmetadata: {name: values}\ndata: {values.yaml: fixture}\n"},
				{"consumer/kustomization.yaml", "namespace: " + ns + "\nresources: [hr.yaml]\n"},
				{"consumer/hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer}\nspec:\n  chartRef: {kind: OCIRepository, name: chart, namespace: flux-system}\n  valuesFrom: [{kind: ConfigMap, name: values}]\n"},
			} {
				testutil.WriteFile(t, root, file.path, file.body)
			}
			st := store.New()
			res, err := discovery.Run(
				t.Context(),
				discovery.Config{Trees: testTrees(t), Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st},
			)
			assert.FatalIf(t, err != nil, "%v", err)
			filter := admissiontest.Filter(
				t,
				admissiontest.Config{
					Changes:   change.NewSet([]string{"producer/values.yaml"}),
					Index:     res.Index,
					Inventory: st,
					Root:      "",
				},
			)
			id := testutil.HelmReleaseID(ns, "consumer")
			{
				got := filter.ShouldReconcile(id)
				assert.FatalIf(t, got != (ns == "apps" && tc.apiVersion == "v1"), "consumer %s selected=%t", id, got)
			}
		})
	}
}

func TestRun_SubstituteFromOnlyChangeSelectsConsumer(t *testing.T) {
	t.Parallel()
	for _, optional := range []bool{false, true} {
		t.Run(strconv.FormatBool(optional), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, file := range []struct{ path, body string }{
				{"flux/producer.yaml", testutil.KustomizationYAML("producer", "apps", "  {path: ./producer}\n")},
				{"flux/consumer.yaml", fmt.Sprintf(testutil.KustomizationYAML("consumer", "apps", "  path: ./consumer\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: values, optional: %t}]\n"), optional)},
				{"producer/kustomization.yaml", "namespace: apps\nresources: [values.yaml]\n"},
				{"producer/values.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values}\ndata: {VALUE: fixture}\n"},
				{"consumer/kustomization.yaml", "resources: []\n"},
			} {
				testutil.WriteFile(t, root, file.path, file.body)
			}
			st := store.New()
			res, err := discovery.Run(
				t.Context(),
				discovery.Config{Trees: testTrees(t), Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st},
			)
			assert.FatalIf(t, err != nil, "%v", err)
			filter := admissiontest.Filter(
				t,
				admissiontest.Config{
					Changes:   change.NewSet([]string{"producer/values.yaml"}),
					Index:     res.Index,
					Inventory: st,
					Root:      "",
				},
			)
			id := testutil.KustomizationID("apps", "consumer")
			{
				got := filter.ShouldReconcile(id)
				assert.FatalIf(t, !got, "optional=%t consumer selected=%t", optional, got)
			}
		})
	}
}

func TestRun_SourceValuesReaders(t *testing.T) {
	t.Parallel()
	for _, form := range []string{"inline", "chartRef"} {
		t.Run(form, func(t *testing.T) {
			t.Parallel()
			testRunSourceValuesReadersCase(t, &form)
		},
		)
	}
}

func testTrees(t *testing.T) *kustomize.TreeCache {
	t.Helper()
	trees := kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2))
	t.Cleanup(trees.Close)
	return trees
}

func TestRun_ListOwnedChildren(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources: [bundle.yaml]\n")
	testutil.WriteFile(t, dir, "app/bundle.yaml", `apiVersion: v1
kind: List
items:
- apiVersion: kustomize.toolkit.fluxcd.io/v1
  kind: Kustomization
  metadata: {name: owner, namespace: flux-system}
  spec:
    path: ./app
    sourceRef: {kind: GitRepository, name: flux-system}
- apiVersion: helm.toolkit.fluxcd.io/v2
  kind: HelmRelease
  metadata: {name: demo, namespace: apps}
  spec:
    chart:
      spec:
        chart: charts/demo
        sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	st := store.New()
	result, err := discovery.Run(
		t.Context(),
		discovery.Config{Trees: testTrees(t), Path: filepath.Join(dir, "app"), RepoRoot: dir, Store: st},
	)
	assert.NoError(t, err)
	id := testutil.HelmReleaseID("apps", "demo")
	owner := testutil.KustomizationID("flux-system", "owner")
	assert.FatalIf(t, !result.Binding.Producer.ReportHidden() || st.GetObject(result.Binding.Producer) == nil,
		"list member replaced the literal entry: %+v",
		result.Binding)

	for _, member := range []manifest.NamedResource{id, owner} {
		record, ok := result.Index.Lookup(member)
		assert.FatalIf(t, !ok || record.Builder != "app/kustomization.yaml" || record.File != record.Builder ||
			record.BuilderOwner != result.Binding.Producer ||
			record.Parent != result.Binding.Producer, "list member lacks entry ownership: %s: %+v", member, record)
		assert.FatalIf(
			t,
			record.Path != "" || record.Authored != (manifest.NamedResource{}) || record.Declaration == nil ||
				record.Declaration.Named() != member,
			"list member lost its prepared declaration provenance: %s: %+v",
			member,
			record,
		)
		assert.FatalIf(t, !slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
			return p.Owner == result.Binding.Producer && p.Source == result.Binding.Source && p.File == record.Builder
		}), "list member lacks its ordinary entry production: %s: %+v", member, record)
		assert.FatalIf(
			t,
			st.GetObject(member) != nil || st.EmissionOwner(member) != (manifest.NamedResource{}),
			"list member was promoted as an orphan before runtime emission: %s",
			member,
		)
	}
}

func TestRun_StandaloneListPackage(t *testing.T) {
	t.Parallel()
	for _, invalid := range []bool{false, true} {
		t.Run(strconv.FormatBool(invalid), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "app/kustomization.yaml", "namespace: apps\nresources: [bundle.yaml]\n")
			body := "apiVersion: v1\nkind: List\nitems:\n- " + strings.ReplaceAll(
				strings.TrimSpace(
					testutil.KustomizationYAML(
						"child",
						"",
						"  path: ./missing\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
					),
				),
				"\n",
				"\n  ",
			) + "\n"
			if invalid {
				body = "apiVersion: v1\nkind: List\nitems: [invalid]\n"
			}
			testutil.WriteFile(t, dir, "app/bundle.yaml", body)
			st := store.New()
			result, err := discovery.Run(
				t.Context(),
				discovery.Config{Trees: testTrees(t), Path: filepath.Join(dir, "app"), RepoRoot: dir, Store: st},
			)
			if invalid {
				assert.FatalIf(
					t,
					err != nil || result == nil || st.GetObject(result.Binding.Producer) == nil,
					"invalid standalone package = %v, %v",
					result,
					err,
				)
				return
			}
			assert.NoError(t, err)
			id := testutil.KustomizationID("apps", "child")
			record, ok := result.Index.Lookup(id)
			assert.FatalIf(
				t,
				!ok || record.Builder != "app/kustomization.yaml" || record.Parent != result.Binding.Producer,
				"prepared member lacks producer ownership: %+v",
				record,
			)
			assert.FatalIf(
				t,
				st.GetObject(id) != nil || st.EmissionOwner(id) != (manifest.NamedResource{}),
				"%v",
				"preparation published a child before its ordinary producer",
			)
		})
	}
}

func testRunGraphOwnershipSourceClassificationCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	dir := t.TempDir()
	sourceName := "flux-system"
	kind := manifest.KindGitRepository
	if strings.HasSuffix((*testCaseInput), "oci") {
		kind = manifest.KindOCIRepository
	} else if strings.HasSuffix((*testCaseInput), "bucket") {
		kind = manifest.KindBucket
	}
	external := strings.HasPrefix((*testCaseInput), "external") ||
		(*testCaseInput) == "self_unavailable" ||
		strings.HasPrefix((*testCaseInput), "declared_")
	if strings.HasPrefix((*testCaseInput), "self") ||
		strings.HasPrefix((*testCaseInput), "external") {
		sourceName = "source"
		testutil.WriteFile(t, dir, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: source, namespace: flux-system}
spec: {url: "https://example.invalid/cluster.git"}
`)
	}
	ownerPath := "./apps"
	if (*testCaseInput) == "external_ancestor" {
		ownerPath = "."
	}
	testutil.WriteFile(
		t,
		dir,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"owner",
			"flux-system",
			"  {path: "+ownerPath+", sourceRef: {kind: "+kind+", name: "+sourceName+"}}\n",
		),
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/kustomization.yaml",
		"resources: [cm.yaml, child.yaml, hr.yaml, repositories]\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/repositories/kustomization.yaml",
		"namespace: repositories\nresources: [repository.yaml]\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/repositories/repository.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: additional}\nspec: {url: https://fixture.invalid/charts}\n",
	)

	testutil.WriteFile(
		t,
		dir,
		"apps/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"flux-system",
			"  {path: ./apps/leaf, sourceRef: {kind: "+kind+", name: "+sourceName+"}}\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/leaf/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, dir, "apps/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: flux-system}
spec: {chartRef: {kind: OCIRepository, name: chart}}
`)
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: config, namespace: flux-system}
data: {value: fixture}
`)
	cfg := discovery.Config{
		Trees:    testTrees(t),
		Path:     filepath.Join(dir, "flux"),
		RepoRoot: dir,
		Store:    store.New(),
	}
	var authored manifest.BaseManifest
	if strings.HasPrefix((*testCaseInput), "declared_") {
		spec := map[string]any{
			"url":    "oci://registry.invalid/artifact",
			"ignore": "excluded/",
			"ref":    map[string]any{"tag": "stable"},
		}
		if kind == manifest.KindBucket {
			spec = map[string]any{
				"endpoint":   "minio.invalid",
				"bucketName": "actual-bucket",
				"prefix":     "published/",
				"ignore":     "excluded/",
			}
		}
		var err error
		parsed, parsedErr := manifest.ParseDoc(
			map[string]any{
				"apiVersion": manifest.SourceDomain + "/v1",
				"kind":       kind,
				"metadata":   map[string]any{"name": sourceName, "namespace": "flux-system"},
				"spec":       spec,
			},
			manifest.ParseDocOptions{},
		)
		authored, err = parsed.Object, parsedErr
		assert.FatalIf(t, err != nil, "%v", err)
		cfg.Store.AddObject(authored)
	}
	authoredContent := manifest.Fingerprint(authored)

	foreignSource := &manifest.RawObject{
		APIVersion: "foreign.example/v1",
		Kind:       kind,
		Namespace:  "flux-system",
		Name:       sourceName,
	}
	foreignOwner := &manifest.RawObject{
		APIVersion: "foreign.example/v1",
		Kind:       manifest.KindKustomization,
		Namespace:  "flux-system",
		Name:       "owner",
		Spec:       map[string]any{"path": "./shared"},
	}
	cfg.Store.AddObjects([]manifest.BaseManifest{foreignSource, foreignOwner})
	if strings.HasPrefix((*testCaseInput), "self") {
		cfg.SelfURLs = []string{"https://example.invalid/cluster.git"}
	}
	if (*testCaseInput) == "self_unavailable" {
		testutil.WriteFile(t, dir, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: source, namespace: flux-system}
spec: {url: "https://example.invalid/cluster.git", ref: {branch: unavailable}}
`)
	}
	res, err := discovery.Run(t.Context(), cfg)
	assert.FatalIf(t, err != nil, "%v", err)
	assert.FatalIf(t, cfg.Store.GetObject(foreignSource.Named()) != foreignSource ||
		cfg.Store.GetArtifact(
			foreignSource.Named(),
			store.ArtifactContent,
		) != nil, "%v", "foreign source acquired the bootstrap alias")
	{
		_, indexed := res.Index.Lookup(foreignOwner.Named())
		assert.FatalIf(t, indexed, "%v", "foreign owner borrowed a real claim")
	}
	repository := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmRepository,
		Namespace: "repositories",
		Name:      "additional",
	}
	{
		_, indexed := res.Index.Lookup(repository)
		assert.FatalIf(
			t, (*testCaseInput) ==
				"external_ancestor" && !indexed, "%v",
			"source-qualified graph lost the nested repository namespace",
		)
	}

	if strings.HasPrefix((*testCaseInput), "missing_") {
		id := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      kind,
			Namespace: "flux-system",
			Name:      sourceName,
		}
		assert.FatalIf(
			t,
			cfg.Store.GetObject(id) != nil || cfg.Store.GetArtifact(id, store.ArtifactContent) != nil,
			"%v",
			"missing non-Git source became available",
		)
		assert.FatalIf(
			t,
			cfg.Store.GetObject(testutil.KustomizationID("flux-system", "child")) != nil,
			"%v",
			"missing source exposed a decoy child",
		)
		{
			owners := ownersForTest(res.Index, "apps/cm.yaml")
			assert.FatalIf(t, len(owners) != 0, "missing source acquired owners: %v", owners)
		}
		return
	}
	child := testutil.KustomizationID("flux-system", "child")
	assert.FatalIf(
		t,
		cfg.Store.GetObject(child) == nil,
		"%v",
		"entry path did not discover its child Kustomization",
	)
	release := testutil.HelmReleaseID("flux-system", "release")
	{
		got := cfg.Store.GetObject(release) != nil
		assert.ErrorIf(t, got != external, "entry path HelmRelease promotion = %v, want %v", got, external)
	}
	owners := ownersForTest(res.Index, "apps/cm.yaml")
	if external {
		assert.ErrorIf(t, len(owners) != 0, "external KS owns local file: %v", owners)
	} else {
		assert.ErrorIf(t, len(owners) != 1 || owners[0].Name != "owner", "local graph owners = %v", owners)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "config"}
	{
		got, want := cfg.Store.GetObject(id) != nil, external
		assert.ErrorIf(t, got != want, "standalone promotion = %v, want %v", got, want)
	}
	src := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      kind,
		Namespace: "flux-system",
		Name:      sourceName,
	}
	assert.ErrorIf(t, cfg.Store.GetObject(src) == nil, "%v", "source must remain file-loaded")
	assert.FatalIf(t, authored != nil &&
		(cfg.Store.GetObject(src) != authored || manifest.Fingerprint(authored) != authoredContent), "%v", "declared source object or spec changed")
	{
		got, want := cfg.Store.GetArtifact(src, store.ArtifactContent) != nil, !external
		assert.ErrorIf(t, got != want, "source alias = %v, want %v", got, want)
	}
}

func testRunSourceValuesReadersCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [parent.yaml]\n")
	testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML("parent", "flux-system",
		"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [owner.yaml]\n")
	testutil.WriteFile(t, root, "apps/owner.yaml", testutil.KustomizationYAML(
		"owner",
		"flux-system",
		"  path: ./workloads/demo\n  targetNamespace: apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
	))
	spec := `  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: cluster}
      valuesFiles: [./config/prod.yaml, ./config/prod.yaml]
      ignoreMissingValuesFiles: true
`
	chart := `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: chart}
spec:
  chart: charts/demo
  sourceRef: {kind: GitRepository, name: cluster}
  valuesFiles: [./config/prod.yaml, ./config/prod.yaml]
  ignoreMissingValuesFiles: true
`
	resources := "resources: [hr.yaml, sibling.yaml]\nnamespace: apps\n"
	if (*testCaseInput) == "chartRef" {
		resources = "resources: [hr.yaml, sibling.yaml, chart.yaml]\nnamespace: apps\n"
		spec = "  chartRef: {kind: HelmChart, name: chart}\n"
		testutil.WriteFile(t, root, "workloads/demo/chart.yaml", chart)
	}
	testutil.WriteFile(t, root, "workloads/demo/kustomization.yaml", resources)
	hr := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: authored}\nspec:\n" + spec
	testutil.WriteFile(t, root, "workloads/demo/hr.yaml", hr)
	testutil.WriteFile(t, root, "workloads/demo/sibling.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: sibling}
spec:
  chartRef: {kind: OCIRepository, name: unrelated}
`)
	demo := testutil.HelmReleaseID("apps", "demo")
	sibling := testutil.HelmReleaseID("apps", "sibling")
	parent := testutil.KustomizationID("flux-system", "parent")
	owner := testutil.KustomizationID("flux-system", "owner")
	discover := func(scan string) (*discovery.Result, *store.Store) {
		t.Helper()
		st := store.New()
		st.AddObject(
			&manifest.GitRepository{
				Name:      "cluster",
				Namespace: "apps",
				URL:       "https://example.invalid/self.git",
			},
		)
		result, err := discovery.Run(t.Context(), discovery.Config{
			Trees: testTrees(t),
			Path:  filepath.Join(root, scan), RepoRoot: root, Store: st,
			SelfURLs: []string{"https://example.invalid/self.git"},
		})
		assert.FatalIf(t, err != nil, "%v", err)
		return result, st
	}
	var previous *manifest.DiscoveryIndex
	for _, state := range []string{"absent", "present", "deleted"} {
		if state == "present" {
			testutil.WriteFile(t, root, "config/prod.yaml", "greeting: production\n")
		}
		if state == "deleted" {
			assert.NoError(t, os.Remove(filepath.Join(root, "config", "prod.yaml")))
		}
		for range 2 {
			result, st := discover("flux")
			source := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "cluster",
			}
			want := []manifest.FileReader{{Reader: demo, Source: source, Exact: true}}
			if (*testCaseInput) == "chartRef" {
				want = append(
					want,
					manifest.FileReader{
						Reader: manifest.NamedResource{
							Group:     manifest.SourceDomain,
							Kind:      manifest.KindHelmChart,
							Namespace: "apps",
							Name:      "chart",
						},
						Source: source,
						Exact:  true,
					},
				)
			}
			slices.SortFunc(want, func(a, b manifest.FileReader) int { return a.Reader.Compare(b.Reader) })
			assert.Diff(t, result.Index.Readers["config/prod.yaml"], want)
			record, ok := result.Index.Lookup(demo)
			assert.Equal(t, ok, true)
			assert.Equal(t, record.Parent, owner)
			assert.Equal(t, st.GetObject(demo) == nil, true)
			filter := admissiontest.Filter(
				t,
				admissiontest.Config{
					Changes:   change.NewSet([]string{"config/prod.yaml"}),
					Index:     result.Index,
					Inventory: st,
					Root:      "",
				},
			)
			for _, id := range []manifest.NamedResource{demo, owner, parent} {
				assert.Equal(t, filter.ShouldReconcile(id), true)
			}
			assert.Equal(t, filter.ShouldReconcile(sibling), false)
			candidate := loader.New(st)
			candidate.Index = result.Index
			candidate.Options.Filter, candidate.Options.Selection = filter, manifest.Selection{Changed: true}
			candidate.Options.Changes = change.NewSet([]string{"config/prod.yaml"})
			proposal, err := candidate.Prepare(
				t.Context(),
				store.Child{
					Obj:         &manifest.HelmRelease{Name: "sibling", Namespace: "apps"},
					Productions: []manifest.Production{{Owner: owner}},
				},
				nil,
				nil,
			)
			assert.NoError(t, err)
			added, retry := filter.AddEmitted(
				proposal.Needed,
				proposal.Selected,
				proposal.Dirty,
				proposal.Revision,
			)
			assert.Equal(t, retry, false)
			assert.Equal(t, len(added), 0)
			assert.Equal(t, filter.ShouldReconcile(sibling), false)
			assert.Equal(t, len(result.Index.Claims["config/"]), 0)
			if previous != nil {
				{
					diff := cmp.Diff(
						previous.Records,
						result.Index.Records,
						cmp.Comparer(manifest.Equal),
					)
					assert.FatalIf(t, diff != "", "records changed (-want +got):\n%s", diff)
				}
			}
			previous = result.Index
		}
	}
	if (*testCaseInput) == "inline" {
		testutil.WriteFile(
			t,
			root,
			"workloads/demo/hr.yaml",
			strings.ReplaceAll(hr, "./config/prod.yaml", "./config/next.yaml"),
		)
	} else {
		testutil.WriteFile(
			t,
			root,
			"workloads/demo/chart.yaml",
			strings.ReplaceAll(chart, "./config/prod.yaml", "./config/next.yaml"),
		)
	}
	next, _ := discover("flux")
	assert.Equal(
		t,
		slices.ContainsFunc(
			next.Index.Readers["config/next.yaml"],
			func(r manifest.FileReader) bool { return r.Reader == demo },
		),
		true,
	)
	assert.Equal(
		t,
		slices.ContainsFunc(
			next.Index.Readers["config/prod.yaml"],
			func(r manifest.FileReader) bool { return r.Reader == demo },
		),
		false,
	)
	assert.Equal(
		t,
		slices.ContainsFunc(
			previous.Readers["config/prod.yaml"],
			func(r manifest.FileReader) bool { return r.Reader == demo },
		),
		true,
	)
	testutil.WriteFile(
		t,
		root,
		"standalone/kustomization.yaml",
		"resources: [hr.yaml, chart.yaml]\nnamespace: apps\n",
	)
	testutil.WriteFile(t, root, "standalone/hr.yaml", strings.Replace(hr, "name: demo", "name: standalone", 1))
	testutil.WriteFile(t, root, "standalone/chart.yaml", chart)
	result, st := discover("standalone")
	standalone := testutil.HelmReleaseID("apps", "standalone")
	assert.Equal(
		t,
		slices.ContainsFunc(
			result.Index.Readers["config/prod.yaml"],
			func(r manifest.FileReader) bool { return r.Reader == standalone },
		),
		true,
	)
	assert.Equal(t, st.GetObject(standalone) == nil, true)
	record, ok := result.Index.Lookup(standalone)
	assert.Equal(t, ok, true)
	assert.Equal(t, record.Path, "")
	assert.Equal(t, record.Authored, manifest.NamedResource{})
	assert.Equal(t, record.Parent, result.Binding.Producer)
}
