package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func TestOrchestratorExistence_PromotionDoesNotAdmitControllerSiblings(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: apps}
data: {value: selected}
`+"---\n"+
		`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: raw-hr, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: source, namespace: apps}
`+"---\n"+
		testutil.KustomizationYAML(
			"raw-ks",
			"apps",
			"  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n",
		)+"---\n"+
		`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: raw-rs, namespace: apps}
spec: {}
`+"---\n"+
		`apiVersion: seaweed.seaweedfs.com/v1
kind: Bucket
metadata: {name: foreign-source, namespace: apps}
spec: {}
`)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	idx := loader.New(store.New())
	idx.Index.Records[id] = manifest.DiscoveryRecord{
		Authored:    id,
		Path:        filepath.Join(dir, "bundle.yaml"),
		Productions: []manifest.Production{{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}},
	}
	st := store.New()
	idx.Store = st
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir, manifest.WorkScope{})
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{
		store: idx.Store, index: idx.Index, cfg: treeConfig{Config: Config{}},
		ksc: &kustomization.Controller{Controller: base.New(st, nil)},
	}}
	{
		promoted, err := e.Promote(t.Context(), selection.PrepareRequest{Reads: nil}, id)
		assert.FatalIf(t, err != nil || !promoted || st.GetObject(id) == nil, "%v", "selected data must be admitted")
	}
	for _, excluded := range []manifest.NamedResource{
		testutil.HelmReleaseID("apps", "raw-hr"),
		testutil.KustomizationID("apps", "raw-ks"),
		{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "apps", Name: "raw-rs"},
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindBucket, Namespace: "apps", Name: "foreign-source"},
	} {
		assert.ErrorIf(t, st.GetObject(excluded) != nil, "runtime promotion admitted %s", excluded)
	}
}

func TestOrchestratorExistence_PromotionPreservesSelectedContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: authored}
data: {values.yaml: "greeting: selected"}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: excluded, namespace: apps}
data: {value: excluded}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: chart, namespace: apps}
spec: {url: https://example.test/chart.git}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: excluded-source, namespace: apps}
spec: {url: https://example.test/excluded.git}
`)
	st := store.New()
	hr := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "chart",
			},
		},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "chart",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       manifest.KindGitRepository,
						Name:       "chart",
						Namespace:  "apps",
						APIVersion: manifest.SourceDomain + "/v1",
					},
				},
			},
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "selected"}},
		},
	}
	st.AddObject(hr)
	selected := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	excluded := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "excluded"}
	chart := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "chart",
	}
	excludedSource := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "excluded-source",
	}
	files := map[manifest.NamedResource]string{
		hr.Named():     "consumer.yaml",
		selected:       "bundle.yaml",
		excluded:       "bundle.yaml",
		chart:          "bundle.yaml",
		excludedSource: "bundle.yaml",
	}
	idx, session := admissiontest.New(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{"consumer.yaml"}),
			Index:     indexForTest(files),
			Inventory: st,
		},
	)
	admissiontest.Commit(t, session, nil)
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir, manifest.WorkScope{})
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	testutil.WriteFile(t, dir, "kustomization.yaml", "namespace: apps\nresources: [bundle.yaml]\n")
	idx.SourceRoot = dir
	idx.Options.Session = session
	_, err = idx.Load(t.Context(), dir, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, idx.FinalizeIndex(t.Context(), dir, true))
	controller := base.New(st, nil)
	controller.Configure(base.Options{Session: session, Index: idx.Index, Loader: idx})
	e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{
		store:     idx.Store,
		index:     idx.Index,
		loader:    idx,
		cfg:       treeConfig{Config: Config{}},
		selection: session,
		ksc:       &kustomization.Controller{Controller: controller},
	}}
	size := len(selectionNamesForTest(session, st, idx.Index))
	reads := &store.ReadSet{}
	reads.Init(st, hr.Named(), nil)
	defer reads.Retire()
	reads.RequireData(chart)
	reads.RequireData(selected)
	{
		promoted, err := e.Promote(
			t.Context(),
			selection.PrepareRequest{Reads: reads},
			selected,
		)
		assert.FatalIf(t, err != nil || !promoted || st.GetObject(selected) == nil ||
			st.GetObject(chart) == nil, "%v", "selected values and required chart source must be admitted")
	}
	{
		got := st.GetObject(selected).(*manifest.ConfigMap).Data["values.yaml"]
		assert.ErrorIf(t, got != "greeting: selected", "selected values = %v", got)
	}
	for _, id := range []manifest.NamedResource{excluded, excludedSource} {
		{
			promoted, err := e.Promote(
				t.Context(),
				selection.PrepareRequest{Reads: nil},
				id,
			)
			assert.ErrorIf(t, err != nil || promoted || st.GetObject(id) != nil ||
				session.Snapshot().Reconcile(id), "excluded sibling %s must remain absent", id)
		}
	}
	assert.ErrorIf(
		t,
		len(selectionNamesForTest(session, st, idx.Index)) != size,
		"%v",
		"ordering promotion must not expand keep",
	)
	st.AddObject(&manifest.ConfigMap{Name: "excluded", Namespace: "apps"})
	{
		promoted, err := e.Promote(t.Context(), selection.PrepareRequest{Reads: nil}, excluded)
		assert.ErrorIf(t, err != nil || promoted, "%v", "availability must not bypass admission")
	}
}

func TestOrchestratorExistence_ExistingDataUsesNewSourceEvidence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := "settings.yaml"
	testutil.WriteFile(
		t,
		dir,
		file,
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {value: selected}\n",
	)
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	data := &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"value": "selected"}}
	st.AddObject(source)
	st.AddObject(data)
	id := data.Named()
	index := indexForTest(map[manifest.NamedResource]string{id: file})
	record := index.Records[id]
	record.Path = filepath.Join(dir, file)
	record.Productions = []manifest.Production{{Authored: id, Source: source.Named(), Path: record.Path, File: file}}
	index.Records[id] = record
	filterLoader, session := admissiontest.New(
		t,
		admissiontest.Config{Changes: change.NewSet([]string{file}), Index: index, Inventory: st, Root: dir},
	)
	admissiontest.Commit(t, session, nil)
	assert.FatalIf(
		t,
		session.Snapshot().Reconcile(id),
		"%v",
		"unavailable source evidence must not admit existing data",
	)
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir, manifest.WorkScope{})
	assert.NoError(t, err)
	st.SetArtifact(source.Named(), input)
	controller := base.New(st, nil)
	controller.Configure(base.Options{Session: session, Loader: filterLoader, Index: index})
	e := &orchestratorExistence{DiscoveryIndex: index, o: &Orchestrator{
		store: st, index: index, selection: session, loader: filterLoader,
		ksc: &kustomization.Controller{Controller: controller},
	}}
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "reader"), nil)
	defer reads.Close()
	reads.RequireData(id)
	{
		promoted, err := e.Promote(t.Context(), selection.PrepareRequest{Reads: &reads}, id)
		assert.FatalIf(
			t,
			err != nil || !promoted || !session.Snapshot().Reconcile(id),
			"%v",
			"new source evidence must admit the existing changed data",
		)
	}
	assert.Equal(t, st.GetObject(id).(*manifest.ConfigMap).Data["value"], "selected")
}

func TestOrchestratorExistence_FileIndexNamespaceConvention(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, recorded, requested string
		raw, want                 bool
	}{
		{"exact", "apps", "apps", true, true},
		{"inherited namespace", "", "apps", true, true},
		{"explicit other namespace", "other", "apps", true, false},
		{"bare query cannot match explicit", "apps", "", true, false},
		{"generated object", "apps", "apps", false, false},
		{"bare generated object", "", "apps", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idx := loader.New(store.New())
			id := testutil.HelmReleaseID(tc.recorded, "target")
			if tc.raw {
				idx.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: "target.yaml"}
			} else {
				idx.Index.Records[id] = manifest.DiscoveryRecord{File: "target.yaml"}
			}
			e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{store: idx.Store, index: idx.Index}}
			id.Namespace = tc.requested
			{
				got := e.IsFileIndexed(id)
				assert.ErrorIf(t, got != tc.want, "IsFileIndexed(%s) = %v, want %v", id, got, tc.want)
			}
		})
	}
}

//nolint:paralleltest,tparallel // The complete fixture sets the process-wide egress policy.
func TestOrchestratorExistence_MissingBootstrapSources(t *testing.T) {
	for _, tc := range []struct {
		name, kind                     string
		absentDeclaration, stale, want bool
	}{
		{name: "oci", kind: manifest.KindOCIRepository, want: true},
		{name: "helm", kind: manifest.KindHelmRepository},
		{name: "bucket", kind: manifest.KindBucket, want: true},
		{name: "external", kind: manifest.KindExternalArtifact},
		{name: "git", kind: manifest.KindGitRepository, want: true},
		{name: "missing requesting declaration", kind: manifest.KindGitRepository, absentDeclaration: true},
		{name: "invalid reads", kind: manifest.KindGitRepository, stale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/decoy", "working tree")
			st := store.New()
			result, err := runDiscovery(t.Context(), discovery.Config{Path: root, Store: st})
			assert.NoError(t, err)
			consumer := &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: tc.kind, Name: "missing", Namespace: "sources",
				},
			}
			if !tc.absentDeclaration {
				st.AddObject(consumer)
			}
			var reads store.ReadSet
			reads.Init(st, consumer.Named(), nil)
			defer reads.Retire()
			if tc.stale {
				marker := &manifest.ConfigMap{Name: "marker", Namespace: "apps"}
				reads.GetObject(marker.Named())
				st.AddObject(marker)
				assert.FatalIf(t, reads.Valid(), "%v", "fixture reads remain valid")
			}
			controller := base.New(st, nil)
			controller.Configure(base.Options{})
			o := &Orchestrator{
				store:     st,
				index:     result.Index,
				admission: result.Admission,
				ksc:       &kustomization.Controller{Controller: controller},
			}
			e := &orchestratorExistence{DiscoveryIndex: result.Index, o: o}
			id := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      tc.kind,
				Namespace: "sources",
				Name:      "missing",
			}
			promoted, err := e.Promote(t.Context(), selection.PrepareRequest{Reads: &reads}, id)
			assert.NoError(t, err)
			assert.Equal(t, promoted, tc.want)
			if promoted {
				assert.NoError(t, constructDiscoveredSource(t.Context(), st, result.Admission, id))
			}
			if !tc.want {
				assert.FatalIf(
					t,
					st.GetObject(id) != nil || st.GetArtifact(id, store.ArtifactContent) != nil,
					"%v",
					"unresolved source was published",
				)
				{
					status, ok := st.GetStatus(id)
					assert.FatalIf(t, ok && status.Status == store.StatusReady, "%v", "unresolved source became ready")
				}
			} else {
				assert.FatalIf(
					t,
					st.GetObject(id) == nil || st.GetArtifact(id, store.ArtifactContent) == nil,
					"%v",
					"manifest source did not publish its alias",
				)
			}
		})
	}
	t.Run("final alias warnings", func(t *testing.T) {
		testOrchestratorExistenceFinalAliasWarnings(t)
	})
}

func testOrchestratorExistenceFinalAliasWarnings(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/decoy", "working tree")
	aliases := []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "sources", Name: "zeta"},
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "sources", Name: "alpha"},
	}
	run := func() *Result {
		o, err := newTestOrchestrator(t, Tree{Path: root, RepoRoot: root}, Config{Concurrency: 2})
		assert.NoError(t, err)
		assert.NoError(t, o.bootstrap(t.Context()))
		assert.Equal(t, o.selection == nil, true)
		assert.Equal(t, o.preparation == nil, true)
		o.configureControllers()
		consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
		o.store.AddObject(consumer)
		existence := &orchestratorExistence{DiscoveryIndex: o.index, o: o}
		for _, id := range aliases {
			consumer = &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: id.Kind, Name: id.Name, Namespace: id.Namespace,
				},
			}
			o.store.AddObject(consumer)
			var reads store.ReadSet
			reads.Init(o.store, consumer.Named(), nil)
			promoted, promoteErr := existence.Promote(t.Context(), selection.PrepareRequest{Reads: &reads}, id)
			reads.Retire()
			assert.NoError(t, promoteErr)
			assert.Equal(t, promoted, true)
			assert.NoError(t, constructDiscoveredSource(t.Context(), o.store, o.admission, id))
			assert.Equal(t, o.store.GetObject(id) != nil, true)
			assert.Equal(t, o.store.GetArtifact(id, store.ArtifactContent) != nil, true)
			status, ok := o.store.GetStatus(id)
			assert.Equal(t, ok, true)
			assert.Equal(t, status.Status, store.StatusReady)
		}
		o.tasks.BlockTillDone()
		result, collectErr := o.collectResult(nil)
		assert.NoError(t, collectErr)
		t.Cleanup(result.Close)
		return result
	}
	first, second := run(), run()
	assert.Diff(t, second.Warnings, first.Warnings)
	var warnings []manifest.Warning
	for i := range first.Warnings {
		warning := &first.Warnings[i]
		if warning.Category == manifest.WarnBootstrapAliases {
			warnings = append(warnings, *warning)
		}
	}
	assert.Diff(t, warnings, []manifest.Warning{
		{
			Category: manifest.WarnBootstrapAliases,
			Message:  "aliased multiple bootstrap sources to the working tree; cross-repo refs render against the wrong tree",
			Detail:   []string{aliases[1].String(), aliases[0].String(), root},
			Count:    1,
		},
	})
}

//nolint:paralleltest // The orchestrator fixture changes the process-wide egress policy.
func TestRender_BootstrapAliasConsumerOrder(t *testing.T) {
	for _, tc := range []struct {
		order string
		valid bool
	}{
		{order: "chart first"},
		{order: "manifests first"},
		{order: "chart only"},
		{order: "chart first", valid: true},
		{order: "manifests first", valid: true},
	} {
		t.Run(fmt.Sprintf("%s/valid=%t", tc.order, tc.valid), func(t *testing.T) {
			root := bootstrapAliasFixture(t, tc.valid)
			first := renderBootstrapAliasOrder(t, root, tc.order, tc.valid)
			defer first.Close()
			second := renderBootstrapAliasOrder(t, root, tc.order, tc.valid)
			defer second.Close()
			firstBody, err := json.Marshal(first.Manifests)
			assert.NoError(t, err)
			secondBody, err := json.Marshal(second.Manifests)
			assert.NoError(t, err)
			assert.Equal(t, string(firstBody), string(secondBody))
			assert.Equal(t, len(second.Failed), len(first.Failed))
			for id, failure := range second.Failed {
				want, exists := first.Failed[id]
				assert.Equal(t, exists, true)
				gotJSON, gotErr := json.Marshal(failure)
				assert.NoError(t, gotErr)
				wantJSON, wantErr := json.Marshal(want)
				assert.NoError(t, wantErr)
				assert.Equal(t, string(gotJSON), string(wantJSON))
			}
			assert.Diff(t, second.Blocked, first.Blocked)
			assert.Diff(t, second.Orphans, first.Orphans)
			assert.Diff(t, second.Warnings, first.Warnings)
		})
	}
}

func bootstrapAliasFixture(t *testing.T, valid bool) string {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/empty.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: entry}\n")
	testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: [hello.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"payload/hello.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\n",
	)
	if valid {
		testutil.WriteFile(t, root, "shared/Chart.yaml", "apiVersion: v2\nname: shared\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			root,
			"shared/templates/result.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-result, namespace: apps}\n",
		)
	}
	return root
}

func renderBootstrapAliasOrder(t *testing.T, root, order string, valid bool) *Result {
	t.Helper()
	input := testDirectoryInput(t, root)
	input.Revision = "1.0.0@sha256:" + strings.Repeat("a", 64)
	input.Digest = "sha256:" + strings.Repeat("a", 64)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, Input: input},
		Config{Concurrency: 2},
	)
	assert.NoError(t, err)
	assert.NoError(t, o.bootstrap(t.Context()))
	o.configureControllers()
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "shared",
	}
	chart := &manifest.HelmRelease{
		Name:      "chart",
		Namespace: "apps",
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name},
	}
	ks := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		Path:      "./payload",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name},
	}
	o.store.AddObject(chart)
	if order != "chart only" {
		o.store.AddObject(ks)
	}
	existence := &orchestratorExistence{DiscoveryIndex: o.index, o: o}
	promote := func(consumer manifest.BaseManifest) bool {
		var reads store.ReadSet
		reads.Init(o.store, consumer.Named(), nil)
		defer reads.Retire()
		promoted, promoteErr := existence.Promote(t.Context(), selection.PrepareRequest{Reads: &reads}, id)
		assert.NoError(t, promoteErr)
		return promoted
	}
	if order != "manifests first" {
		assert.Equal(t, promote(chart), false)
		assert.Equal(t, o.store.GetObject(id) == nil, true)
		assert.Equal(t, o.store.GetArtifact(id, store.ArtifactContent) == nil, true)
		var reads store.ReadSet
		reads.Init(o.store, chart.Named(), nil)
		waiter := depwait.Waiter{Store: &reads, Missing: depwait.NewMissingPolicy(nil)}
		classified := waiter.Classify(
			t.Context(),
			manifest.DependencyRef{NamedResource: id, ArtifactOnly: true},
			manifest.DrainNone,
		)
		assert.Equal(t, classified.Kind, schedule.Blocked)
		if order == "chart only" {
			classified = waiter.Classify(
				t.Context(),
				manifest.DependencyRef{NamedResource: id, ArtifactOnly: true},
				manifest.DrainCascade,
			)
			assert.Equal(t, classified.Kind, schedule.DependencyFailed)
		}
		reads.Retire()
	}
	if order != "chart only" {
		assert.Equal(t, promote(ks), true)
		assert.NoError(t, constructDiscoveredSource(t.Context(), o.store, o.admission, id))
		artifact := o.store.GetArtifact(id, store.ArtifactContent)
		input := artifact.(*store.SourceArtifact)
		content, readErr := fs.ReadFile(input.FS, "payload/hello.yaml")
		assert.NoError(t, readErr)
		assert.Equal(t, string(content), "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\n")
		object := o.store.GetObject(id)
		assert.Equal(t, promote(chart), true)
		assert.Equal(t, o.store.GetArtifact(id, store.ArtifactContent) == artifact, true)
		assert.Equal(t, o.store.GetObject(id) == object, true)
	}
	fetcher := &generatedOCIFetcher{err: errors.New("unexpected remote fetch")}
	o.src.Fetchers[id.Kind] = fetcher
	result, err := o.collectResult(o.run(t.Context()))
	assert.Equal(t, err != nil, !valid)
	assert.Equal(t, fetcher.calls.Load(), int64(0))
	failure, failed := result.Failed[chart.Named()]
	assert.FatalIf(t, valid && failed, "valid shared chart failed: %+v", failure)
	assert.Equal(t, failed, !valid)
	switch {
	case valid:
		assert.Equal(t, len(result.Failed), 0)
		assert.Equal(t, len(result.Blocked), 0)
		assert.Equal(t, configMapNames(result)["chart-result"], true)
		assert.Equal(t, configMapNames(result)["hello"], true)
	case order == "chart only":
		assert.Equal(t, failure.IsDependencyFailure(), true)
	default:
		assert.Equal(t, failure.IsDependencyFailure(), false)
		assert.Equal(t, strings.Contains(failure.Message, "Chart.yaml"), true)
		status, ready := o.store.GetStatus(ks.Named())
		assert.Equal(t, ready, true)
		assert.Equal(t, status.Status, store.StatusReady)
	}
	o.stop()
	return result
}

func TestOrchestratorExistence_BootstrapRoutes(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"full", "selected", "prepared"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			l := loader.New(st)
			t.Cleanup(l.Close)
			result, err := runDiscovery(t.Context(), discovery.Config{Path: t.TempDir(), Store: st, Loader: l})
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			consumer := &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				Path:      "./payload",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "shared"},
			}
			st.AddObject(consumer)
			controller := base.New(st, nil)
			o := &Orchestrator{
				store:     st,
				loader:    l,
				index:     result.Index,
				admission: result.Admission,
				ksc:       &kustomization.Controller{Controller: controller},
			}
			options := base.Options{Loader: l, Index: result.Index}
			if route == "selected" {
				o.selection, err = selection.New(
					st,
					l,
					selection.Request{Roots: []manifest.NamedResource{consumer.Named()}},
				)
				assert.NoError(t, err)
				options.Session = o.selection
			}
			if route == "prepared" {
				o.preparation = &selection.AdmissionResult{}
			}
			controller.Configure(options)
			if route == "selected" {
				accepted, acceptErr := controller.KeepEmitted(
					t.Context(),
					manifest.NamedResource{},
					selection.PrepareRequest{Generation: []store.Child{{Obj: consumer}}},
				)
				assert.NoError(t, acceptErr)
				assert.Equal(t, accepted, true)
			}
			existence := &orchestratorExistence{DiscoveryIndex: result.Index, o: o}
			var reads store.ReadSet
			reads.Init(st, consumer.Named(), nil)
			defer reads.Retire()
			promoted, err := existence.Promote(
				t.Context(),
				selection.PrepareRequest{Reads: &reads},
				consumer.SourceID(),
			)
			assert.NoError(t, err)
			assert.Equal(t, promoted, true)
			assert.NoError(t, constructDiscoveredSource(t.Context(), st, result.Admission, consumer.SourceID()))
			assert.Equal(t, st.GetObject(consumer.SourceID()) != nil, true)
			assert.Equal(t, st.GetArtifact(consumer.SourceID(), store.ArtifactContent) != nil, true)
		})
	}
}
