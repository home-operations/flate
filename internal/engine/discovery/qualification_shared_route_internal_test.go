package discovery

import (
	"errors"
	"io/fs"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/gitrepo"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEntryQualification_SharedRouteWithdrawal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	first := &manifest.ResourceSet{Name: "first", Namespace: "apps"}
	later := &manifest.ResourceSet{Name: "later", Namespace: "apps"}
	shared := &manifest.ResourceSet{Name: "shared", Namespace: "apps"}
	st.AddObject(first)
	st.AddObject(later)
	for _, parent := range []*manifest.ResourceSet{first, later} {
		st.RecordEmitted(parent.Named(), &store.Emission{
			Children: []store.Child{{Obj: shared}}, Complete: true,
		}, nil)
	}
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	qualifyForTest(t, l, first.Named(), later.Named())
	t.Cleanup(l.Retire)
	a := &Admission{input: input, path: filepath.Join(root, "entry"), loader: l, entryDemand: true}
	selected, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, selected, (*manifest.Kustomization)(nil))
	assert.Equal(t, binding, Binding{})
	assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{shared.Named()})
	applier := &manifest.Kustomization{Name: "applier", Namespace: "apps", Path: "./entry"}
	publish := func() {
		var reads store.ReadSet
		reads.Init(st, shared.Named(), nil)
		reads.GetDeclaration(shared.Named())
		output := &store.Emission{Children: []store.Child{{Obj: applier}}, Complete: true}
		reads.Bind(output)
		st.RecordEmitted(shared.Named(), output, nil)
		reads.Accept(store.AssessmentReconciled)
		st.Retire(&reads)
	}
	publish()
	selected, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, selected == nil, "accepted shared output did not propose an entry")
	assert.Equal(t, selected.Named(), applier.Named())
	assert.Equal(t, st.EmissionOwner(shared.Named()), later.Named())
	withdraw := make(chan struct{})
	withdrawn := make(chan struct{})
	go func() {
		defer close(withdrawn)
		<-withdraw
		st.RecordEmitted(first.Named(), &store.Emission{Complete: true}, nil)
	}()
	close(withdraw)
	<-withdrawn
	assert.Equal(t, binding.Reads.Valid(), false)
	assert.Equal(t, acceptEntryForTest(t, a, &binding), Binding{})
	assert.Equal(t, st.EmissionOwner(shared.Named()), later.Named())
	publish()
	selected, binding, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, selected == nil, "remaining shared route lost its accepted output")
	assert.Equal(t, selected.Named(), testutil.KustomizationID("apps", "applier"))
	assert.Equal(t, acceptEntryForTest(t, a, &binding).Producer, applier.Named())
	assert.Equal(t, len(a.EntryProducers()), 0)
	assert.Equal(t, len(st.Warnings()), 0)
}

func TestEntryApplication_InventoryAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		complete, member, stale bool
		accepted, pending       bool
		unknown                 bool
	}{
		{name: "unknown", unknown: true, pending: true},
		{name: "complete", complete: true, member: true, accepted: true},
		{name: "omitted", complete: true},
		{name: "partial", member: true, pending: true},
		{name: "stale", complete: true, member: true, stale: true, pending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
			root := &manifest.Kustomization{Name: "sync", Namespace: "apps", Path: "./entry"}
			settings := &manifest.ConfigMap{
				Name:      "settings",
				Namespace: "apps",
				Data:      map[string]any{"value": "before"},
			}
			st.AddObject(owner)
			st.AddObject(settings)
			var outputReads store.ReadSet
			outputReads.Init(st, owner.Named(), nil)
			defer outputReads.Retire()
			outputReads.GetObject(settings.Named())
			_, _, output := outputReads.GetEmission(owner.Named())
			defer output.Finish()
			output.Complete = tc.complete
			if tc.member {
				output.Children = []store.Child{{Obj: root, VisibleGeneration: true}}
			}
			outputReads.Bind(&output)
			if !tc.unknown {
				st.RecordEmitted(owner.Named(), &output, nil)
			}
			if tc.stale {
				current := *settings
				current.Data = map[string]any{"value": "after"}
				st.AddObject(&current)
			}

			var reads store.ReadSet
			reads.Init(st, manifest.BootstrapSourceID, nil)
			defer reads.Retire()
			evidence := applicationEvidence{object: root, owner: owner.Named()}
			accepted, pending := evidence.ownerAccepts(&reads)
			assert.Equal(t, accepted, tc.accepted)
			assert.Equal(t, pending, tc.pending)
			var required []manifest.NamedResource
			for gate := range reads.Gates() {
				assert.Equal(t, gate.Materializer, true)
				required = append(required, gate.NamedResource)
			}
			if tc.pending {
				assert.Diff(t, required, []manifest.NamedResource{owner.Named()})
			} else {
				assert.Equal(t, len(required), 0)
			}
		})
	}
}

func TestProjectedOperands_ParentCycle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, sync string
	}{
		{name: "GitRepository", sync: "kind: GitRepository, name: sync, url: https://fixture.invalid/repo, ref: refs/heads/main"},
		{name: "OCIRepository", sync: "kind: OCIRepository, name: sync, url: oci://fixture.invalid/repo, ref: latest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testProjectedOperandsParentCycle(t, tc.sync)
		})
	}
}

func testProjectedOperandsParentCycle(t *testing.T, sync string) {
	t.Helper()
	root := t.TempDir()
	files := fstest.MapFS{
		"release.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: generator, namespace: apps}
spec:
  interval: 1h
  chart:
    spec: {chart: ./chart, sourceRef: {kind: GitRepository, name: self}}
`)},
		"chart/Chart.yaml": {Data: []byte("apiVersion: v2\nname: generator\nversion: 1.0.0\n")},
		"chart/templates/child.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: child, namespace: apps}
spec:
  sync: {` + sync + `, path: ./entry}
`)},
		"entry/kustomization.yaml": {Data: []byte("resources: [workload.yaml]\n")},
		"entry/workload.yaml": {Data: []byte(testutil.KustomizationYAML(
			"workload", "apps", "  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
		))},
	}
	st := store.New()
	source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.AddObject(source)
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: files}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	st.SetArtifact(source.Named(), input)
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	t.Cleanup(l.Retire)
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	generator := testutil.HelmReleaseID("apps", "generator")
	projections := l.SourceFacts(manifest.BootstrapSourceID, nil).Context.TemplateProjections
	assert.FatalIf(t, len(projections[generator].Objects) == 0, "fixture did not index the chart projection")
	q := &entryQualification{loader: l}
	a := &Admission{
		input: input, path: root, store: st, loader: l, qualification: q,
		remotes: []string{"https://fixture.invalid/repo"},
		entryTargets: []entryTarget{{
			occurrence: selection.OccurrenceID{File: "entry/workload.yaml"},
			origin:     input.OriginPath(filepath.Join(root, "entry", "workload.yaml")),
		}},
	}
	var reads store.ReadSet
	reads.Init(st, manifest.BootstrapSourceID, nil)
	defer st.Retire(&reads)
	defer reads.ReleaseCapture()
	walk := qualificationWalk{q: q, admission: a, reads: &reads}
	assert.FatalIf(t, !walk.projectedApplication(projections[generator].Objects[0]),
		"fixture projection does not reach the selected declaration")
	operands := walk.projectedOperands(t.Context(), projections, walk.projectedApplication)
	assert.FatalIf(t, len(operands) != 1, "expected generator operand, got %v", operands)
	assert.Equal(t, operands[0].Identity, generator)

	owner, other := testutil.KustomizationID("apps", "owner"), testutil.KustomizationID("apps", "other")
	record, _ := l.Index.Lookup(generator)
	record.Parent = owner
	l.Index.Records[generator] = record
	l.Index.Records[owner] = manifest.DiscoveryRecord{Parent: other}
	l.Index.Records[other] = manifest.DiscoveryRecord{Parent: owner}
	assert.Equal(t, len(walk.projectedOperands(t.Context(), projections, walk.projectedApplication)), 0)
}

func TestProjectedOperands_UnavailableProducerLeavesSharedAncestor(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	blocked := testutil.HelmReleaseID("apps", "blocked")
	viable := testutil.HelmReleaseID("apps", "viable")
	owner := testutil.KustomizationID("apps", "owner")
	indexed := testutil.HelmReleaseID("apps", "indexed")
	missing := testutil.HelmReleaseID("apps", "missing")
	files := fstest.MapFS{
		"blocked.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: blocked, namespace: apps}
spec:
  interval: 1h
  chartRef: {kind: OCIRepository, name: chart}
  dependsOn: [{name: missing}]
`)},
		"viable.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: viable, namespace: apps}
spec:
  interval: 1h
  chartRef: {kind: OCIRepository, name: chart}
  dependsOn: [{name: indexed}]
`)},
		"indexed.yaml": {Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: indexed, namespace: apps}
spec:
  interval: 1h
  chartRef: {kind: OCIRepository, name: chart}
`)},
		"owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./owner\n"))},
	}
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: files}
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)

	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	t.Cleanup(l.Retire)
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	st.DeleteObject(indexed)
	st.UpdateStatus(missing, store.StatusInfo{Status: store.StatusReady})
	assert.Equal(t, l.IsFileIndexed(indexed), true)
	assert.Equal(t, l.IsFileIndexed(missing), false)
	assert.Equal(t, st.GetDeclaration(indexed), nil)
	assert.Equal(t, st.GetDeclaration(missing), nil)
	for _, id := range []manifest.NamedResource{blocked, viable} {
		record, found := l.Index.Lookup(id)
		assert.FatalIf(t, !found, "producer %s was not indexed", id)
		record.Parent = owner
		l.Index.Records[id] = record
	}
	q := &entryQualification{loader: l}
	a := &Admission{input: input, path: root, store: st, loader: l, qualification: q}
	var reads store.ReadSet
	reads.Init(st, manifest.BootstrapSourceID, nil)
	defer reads.Retire()
	walk := qualificationWalk{q: q, admission: a, reads: &reads}
	waiter := depwait.Waiter{Store: &reads, Parent: blocked, Missing: l.Options.Missing}
	gate := manifest.DependencyRef{NamedResource: missing}
	assert.Equal(t, waiter.Classify(t.Context(), gate, manifest.DrainCascade).Kind, schedule.Terminal)
	assert.FatalIf(
		t,
		!walk.unavailableProjection(t.Context(), blocked),
		"ready status replaced the missing declaration",
	)
	assert.FatalIf(t, walk.unavailableProjection(t.Context(), viable), "file-indexed prerequisite blocked the producer")

	orders := [][2]manifest.NamedResource{{blocked, viable}, {viable, blocked}}
	var captured [2]store.ReadSet
	for i, order := range orders {
		captured[i].Init(st, manifest.BootstrapSourceID, nil)
		t.Cleanup(captured[i].Retire)
		walk.reads = &captured[i]
		visits := projectedOperandVisits{
			seen:    make(map[manifest.NamedResource]bool),
			visited: make(map[manifest.NamedResource]bool),
		}
		var operands []selection.OccurrenceFacts
		for _, producer := range order {
			if operand, ok := walk.projectedOperand(t.Context(), producer, input.OriginPath(root), visits); ok {
				operands = append(operands, operand)
			}
		}
		assert.FatalIf(t, len(operands) != 1, "order %v selected %d operands, want one", order, len(operands))
		assert.Equal(t, operands[0].Identity, owner)
		assert.FatalIf(t, !captured[i].Valid(), "order %v started with invalid reads", order)
	}
	st.AddObject(&manifest.HelmRelease{Name: missing.Name, Namespace: missing.Namespace})
	for i, order := range orders {
		assert.FatalIf(t, captured[i].Valid(), "order %v missed the new prerequisite declaration", order)
	}
}

func TestEntryQualification_NamedOperandsAdvanceInOrder(t *testing.T) {
	t.Parallel()
	st := store.New()
	first := &manifest.ResourceSet{Name: "first", Namespace: "apps"}
	later := &manifest.ResourceSet{Name: "later", Namespace: "apps"}
	st.AddObject(first)
	st.AddObject(later)
	q := &entryQualification{packages: [][]selection.OccurrenceFacts{{
		{Identity: first.Named(), Object: first},
		{Identity: later.Named(), Object: later},
	}}}
	var reads store.ReadSet
	reads.Init(st, manifest.BootstrapSourceID, nil)
	defer reads.Retire()
	walk := qualificationWalk{
		q: q, store: st, reads: &reads, admission: &Admission{
			input:        &store.SourceArtifact{LocalPath: t.TempDir()},
			entryTargets: []entryTarget{{occurrence: selection.OccurrenceID{File: "entry/workload.yaml"}}},
		},
	}
	assert.NoError(t, walk.advanceOperands(t.Context()))
	assert.Diff(t, q.active, []manifest.NamedResource{first.Named(), later.Named()})
	assert.Diff(t, q.candidates, []manifest.NamedResource{first.Named()})
}

func TestEntryQualification_OpaqueOrderingOperand(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                               string
		changed, ordinary, projected, want bool
	}{
		{name: "changed evaluation only", changed: true},
		{name: "changed evaluation only with projection", changed: true, projected: true, want: true},
		{name: "changed ordinary", changed: true, ordinary: true, want: true},
		{name: "cold prerequisite", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			l := loader.New(st)
			t.Cleanup(l.Retire)
			configureSelection(t, l, selection.Request{Changed: tc.changed})
			producer := &manifest.HelmRelease{Name: "opaque", Namespace: "apps"}
			if tc.projected {
				producer.Values = map[string]any{"sync": map[string]any{
					"kind": manifest.KindGitRepository, "name": "sync", "url": "https://fixture.invalid/repo",
					"ref": "refs/heads/main", "path": "./entry",
				}}
			}
			id := producer.Named()
			st.AddObject(producer)
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			if tc.ordinary {
				reads.RequireMaterializer(id)
				_, err := admitSelection(t.Context(), t, l, selection.PrepareRequest{Reads: &reads})
				assert.NoError(t, err)
			}
			reads.RequireOrdering(&store.OrderingEdge{Target: id})
			_, err := admitSelection(t.Context(), t, l, selection.PrepareRequest{Reads: &reads})
			assert.NoError(t, err)
			if tc.projected {
				decision := l.Options.Session.Snapshot()
				assert.Equal(t, decision.ReadinessDemand(id), true)
				assert.Equal(t, decision.EvaluationOnly(id), true)
			}
			q := &entryQualification{
				loader: l, changedOnly: tc.changed,
				packages: [][]selection.OccurrenceFacts{{{Identity: id, Object: producer}}},
			}
			walk := qualificationWalk{
				q: q, store: st, reads: &reads,
				admission: &Admission{input: &store.SourceArtifact{LocalPath: t.TempDir()}},
			}
			assert.NoError(t, walk.advanceOperands(t.Context()))
			assert.Equal(t, q.pending, tc.want)
			assert.Equal(t, slices.Contains(q.active, id), tc.want)
		})
	}
}

func TestQualifySelectedEntry_IgnoresAnotherSource(t *testing.T) {
	t.Parallel()
	root, external := t.TempDir(), t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
	st := store.New()
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(
		t,
		l,
		selection.Request{Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "workload")}},
	)
	t.Cleanup(l.Retire)
	d := &Session{loader: l, cfg: Config{Input: input, Path: root, Store: st}}
	a := d.admission()
	foreign := &selection.Demand{
		Artifact:    &store.SourceArtifact{LocalPath: external},
		Application: selection.OccurrenceID{Source: external, File: "workload.yaml"},
	}
	assert.NoError(t, d.qualifySelectedEntry(t.Context(), []*selection.Demand{foreign}, root))
	assert.Equal(t, len(a.entryTargets), 0)
	assert.Equal(t, len(a.entryOccurrences), 0)
}

func TestEntryQualification_DirectSyncAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		declared bool
	}{
		{name: "declared pair", declared: true},
		{name: "unobserved pair"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			instance := &manifest.RawObject{
				APIVersion: "fluxcd.controlplane.io/v1", Kind: "FluxInstance", Namespace: "apps", Name: "instance",
				Spec: map[string]any{"sync": map[string]any{
					"kind": manifest.KindOCIRepository, "name": "sync",
					"url": "oci://fixture.invalid/repo", "ref": "latest", "path": "./entry",
				}},
			}
			st := store.New()
			if tc.declared {
				st.AddObject(instance)
			}
			var reads store.ReadSet
			reads.Init(st, manifest.BootstrapSourceID, nil)
			defer reads.Retire()
			q := &entryQualification{}
			admission := &Admission{input: input, path: filepath.Join(root, "entry"), store: st, qualification: q}
			walk := qualificationWalk{q: q, admission: admission, reads: &reads, store: st}
			matched, err := walk.visitGeneration(t.Context(), &applicationEvidence{object: instance})
			assert.NoError(t, err)
			assert.Equal(t, matched, tc.declared)
		})
	}
}

func TestAdmission_GenerationPairUsesSelectedSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"entry/instance.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: instance, namespace: apps}
spec:
  sync: {kind: OCIRepository, name: sync, url: oci://fixture.invalid/checkout, ref: latest, path: ./workload}
`)},
	}}
	cfg := testConfig(t, Config{
		Input: input, Store: store.New(), RepoRoot: root, Path: filepath.Join(root, "entry"),
	})
	d, err := Begin(t.Context(), cfg)
	assert.NoError(t, err)
	t.Cleanup(d.Close)
	id := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      "FluxInstance",
		Name:      "instance",
		Namespace: "apps",
	}
	occurrence := d.loader.RootEvidence(id, nil).Occurrence
	instance := occurrence.EffectiveObject()
	assert.FatalIf(t, instance == nil, "fixture did not index the FluxInstance")
	app, _ := manifest.ApplicationFor(instance)

	groups := make(map[manifest.NamedResource]*entrySourceGroup)
	assert.NoError(t, d.addGenerationSources(groups, instance, &occurrence))
	assert.FatalIf(t, groups[app.Source].sync == nil, "selected generated source lost its sync pair")
	assert.Equal(t, groups[app.Source].sync.source, groups[app.Source].object)

	other := &manifest.OCIRepository{
		Name:      app.Source.Name,
		Namespace: app.Source.Namespace,
		URL:       "oci://fixture.invalid/other",
	}
	groups[app.Source] = &entrySourceGroup{object: other}
	assert.NoError(t, d.addGenerationSources(groups, instance, &occurrence))
	assert.Equal(t, groups[app.Source].object, manifest.BaseManifest(other))
	assert.FatalIf(t, groups[app.Source].sync != nil, "competing declaration received the generated pair")
}

func TestPreparedEntryReach_CurrentSyncSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
	document := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindOCIRepository,
		"metadata": map[string]any{"name": "sync", "namespace": "apps"},
		"spec":     map[string]any{"url": "oci://fixture.invalid/repo"},
	}
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	source := parsed.Object
	app := manifest.NewEntryKustomization(source.Named(), "./outside", manifest.NamedResource{})
	app.Name, app.Namespace = "sync", "apps"
	reach := preparedEntryReach{
		input: input, entry: filepath.Join(root, "entry"),
		sources: map[manifest.NamedResource]entryResolution{source.Named(): {
			object: source, input: input, projected: true,
			sync: &syncSource{source: source, application: app},
		}},
	}
	equivalent, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.Equal(t, equivalent.Object == source, false)
	children := []store.Child{{Obj: equivalent.Object, VisibleGeneration: true}}
	application, _ := app.EntryApplication()
	assert.Equal(t, reach.applicationPossible(&application, true, children), false)
	disjointPair := application
	disjointPair.Path = "./other"
	assert.Equal(t, reach.applicationPossible(&disjointPair, true, children), true)
	document["spec"] = map[string]any{"url": "oci://fixture.invalid/updated"}
	changed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	children[0].Obj = changed.Object
	assert.Equal(t, reach.applicationPossible(&application, true, children), true)
}

func TestPreparedEntryReach_GeneratedGitSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	assert.NoError(t, repo.Storer.SetReference(
		plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("preview")),
	))
	testutil.WriteFile(t, root, "entry/committed.yaml", "committed")
	commit := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(1, 0)},
	})
	_, err := repo.CreateTag("preview", commit, nil)
	assert.NoError(t, err)
	const target = "entry/workload.yaml"
	testutil.WriteFile(t, root, target, "from the working tree")
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	dir, err := gittree.OpenDirectory(t.Context(), location)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, dir.Close()) })
	input := dir.Artifact()
	const url = "https://fixture.invalid/repo"
	for _, tc := range []struct {
		name     string
		ref      sourcev1.GitRepositoryRef
		provider store.SourceProvider
		reaches  bool
	}{
		{name: "tag", ref: sourcev1.GitRepositoryRef{Tag: "preview"}, provider: store.ProviderCommit},
		{name: "branch", ref: sourcev1.GitRepositoryRef{Branch: "preview"}, provider: store.ProviderAlias, reaches: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			l := loader.New(st)
			t.Cleanup(l.Retire)
			a := &Admission{input: input, path: root, store: st, loader: l, remotes: []string{url}}
			source := &manifest.GitRepository{Name: "sync", Namespace: "apps"}
			source.URL, source.Reference = url, new(tc.ref)
			app := &manifest.EntryApplication{Source: source.Named(), Path: "./entry"}
			children := []store.Child{{Obj: source}}
			current, err := a.resolve(t.Context(), source.Named(), &sourceRequest{
				object: source, scoped: true, projected: true,
			})
			assert.NoError(t, err)
			assert.Equal(t, current.provider, tc.provider)
			assert.FatalIf(t, current.artifact == nil, "source did not resolve")
			_, err = fs.Stat(current.artifact.FS, target)
			if tc.reaches {
				assert.NoError(t, err)
			} else {
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}

			origin := filepath.Join(root, target)
			reach := preparedEntryReach{
				input: input, entry: root, remotes: []string{url}, targets: []entryTarget{{
					origin: origin, occurrence: selection.OccurrenceID{Source: root, File: target},
				}},
				edges: map[[2]string]bool{{filepath.Join(root, "entry"), origin}: true},
			}
			assert.Equal(t, reach.applicationPossible(app, true, children), true)
			disjoint := *app
			disjoint.Path = "./outside"
			assert.Equal(t, reach.applicationPossible(&disjoint, true, children), tc.provider == store.ProviderCommit)
			reach.sources = map[manifest.NamedResource]entryResolution{source.Named(): current}
			assert.Equal(t, reach.applicationPossible(app, true, children), tc.reaches)
			conflict := *source
			conflict.URL = "https://fixture.invalid/other"
			assert.Equal(t, reach.applicationPossible(app, true, []store.Child{{Obj: &conflict}}), tc.reaches)
		})
	}
}
