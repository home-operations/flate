package discovery

import (
	"context"
	"io/fs"
	"iter"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/artifactgenerator"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type retryAdmissionFacts struct {
	*loader.Loader

	children map[manifest.NamedResource]store.Child
}

func (f *retryAdmissionFacts) AdmissionCandidate(id manifest.NamedResource) store.Child {
	if child, known := f.children[id]; known {
		return child
	}
	return f.Loader.AdmissionCandidate(id)
}

func (f *retryAdmissionFacts) RootEvidence(
	id manifest.NamedResource, reads *store.ReadSet, materialize bool,
) selection.RootEvidence {
	facts := f.Loader.RootEvidence(id, reads, materialize)
	if child, known := f.children[id]; known {
		facts.BuiltChild, facts.BuiltCurrent = child, true
	}
	return facts
}

func TestAdvance_PublicationRetryObservations(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: [marker.yaml]\n")},
		"entry/marker.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker, namespace: apps}\n"),
		},
		"payload/kustomization.yaml": {Data: []byte("resources: [target.yaml]\n")},
		"payload/target.yaml": {
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: target, namespace: apps}\nspec: {resources: []}\n",
			),
		},
	}}
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := loader.New(st)
	l.SourceRoot = root
	stable := &manifest.GitRepository{Name: "stable", Namespace: "apps", URL: "https://fixture.invalid/stable"}
	st.AddObject(stable)
	st.SetArtifact(stable.Named(), input)
	retained := &manifest.Kustomization{Name: "a-retained", Namespace: "apps", Path: "payload"}
	retained.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: stable.Name}
	present := &manifest.Kustomization{Name: "present", Namespace: "apps", Path: "present"}
	st.AddObject(present)
	discarded := &manifest.GitRepository{Name: "discarded", Namespace: "apps", URL: "git://fixture.invalid/discarded"}
	introduced := &manifest.GitRepository{
		Name:      "introduced",
		Namespace: "apps",
		URL:       "git://fixture.invalid/introduced",
	}
	anchor := &manifest.Kustomization{Name: "anchor", Namespace: "apps", Path: "anchor"}
	anchor.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: discarded.Name}
	st.AddObject(anchor)
	children := map[manifest.NamedResource]store.Child{}
	for _, object := range []manifest.BaseManifest{retained, present, discarded, introduced} {
		children[object.Named()] = store.Child{
			Obj: object, SourceSupport: manifest.BootstrapSourceID,
			Warnings: []manifest.Warning{{Category: manifest.WarnInputReadFailed, Message: "replaced proposal"}},
		}
		l.Index.Records[object.Named()] = manifest.DiscoveryRecord{Builder: "fixture", Declaration: object}
	}
	target := (&manifest.ResourceSet{Name: "target", Namespace: "apps"}).Named()
	l.Index.Records[target] = manifest.DiscoveryRecord{
		Builder: "payload", Parent: retained.Named(),
		Productions: []manifest.Production{{Owner: retained.Named(), Source: manifest.BootstrapSourceID}},
	}
	facts := &retryAdmissionFacts{Loader: l, children: children}
	var err error
	l.Options.Session, err = selection.New(st, facts, selection.Request{Changed: true})
	assert.NoError(t, err)
	d := newDiscoverer(
		&Config{Input: input, Path: filepath.Join(root, "entry"), Store: st, Loader: l, Trees: testTrees(t)},
	)
	defer d.Close()
	beforeReceipt := &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "before"}
	afterReceipt := &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "after"}
	l.Options.Session.PublishEntry(&selection.EntryRoute{Application: beforeReceipt})
	var preparation *selection.Preparation
	defer func() { preparation.Retire() }()
	var required store.ReadSet
	required.Init(st, manifest.NamedResource{}, nil)
	defer required.Retire()
	for _, id := range []manifest.NamedResource{anchor.Named(), retained.Named(), present.Named(), target} {
		required.RequireData(id)
	}
	request := selection.PrepareRequest{
		Missing: l.Options.Missing,
		Reads:   &required, Preparation: &preparation,
		HistoryOwners: map[manifest.NamedResource][]manifest.SourceEvidence{},
	}
	for _, entry := range []*manifest.EntryApplication{beforeReceipt, afterReceipt} {
		owner := manifest.NewEntryKustomization(entry.Source, entry.Path, manifest.NamedResource{}).Named()
		request.HistoryOwners[owner] = []manifest.SourceEvidence{
			{Source: input.HistoryIdentity(manifest.BootstrapSourceID)},
		}
	}
	initial, err := l.Options.Session.Prepare(t.Context(), request)
	assert.NoError(t, err)
	assert.Equal(
		t,
		slices.ContainsFunc(
			initial.Objects,
			func(child store.Child) bool { return child.Obj.Named() == discarded.Named() },
		),
		true,
	)
	initial.Retire()
	var replaced atomic.Bool
	published, replace := make(chan struct{}), make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		<-published
		changed := anchor.Clone()
		changed.SourceRef.Name = introduced.Name
		st.AddObject(changed)
		for id, child := range children {
			child.Warnings = []manifest.Warning{{Category: manifest.WarnInputReadFailed, Message: "accepted proposal"}}
			children[id] = child
		}
		next := *input
		next.Revision = "successor"
		st.SetArtifact(manifest.BootstrapSourceID, &next)
		l.Options.Session.PublishEntry(&selection.EntryRoute{Application: afterReceipt})
		close(replace)
	}()
	unsubscribe := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == retained.Named() && replaced.CompareAndSwap(false, true) {
			close(published)
			<-replace
		}
	}, false)
	defer unsubscribe()
	accepted, committed, err := l.Options.Session.Admit(
		t.Context(),
		selection.AdmissionOptions{Request: request, Stage: selection.Construction},
	)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.FatalIf(t, !replaced.Load(), "publication did not reach the retained application")
	<-finished
	assert.Equal(t, replaced.Load(), true)
	assert.Equal(t, accepted.EntryReceipt, afterReceipt)
	assert.Equal(t, slices.Contains(accepted.Absent, retained.Named()), true)
	assert.Equal(t, slices.Contains(accepted.Absent, present.Named()), false)
	assert.Equal(t, slices.Contains(accepted.Absent, discarded.Named()), false)
	assert.Equal(
		t,
		slices.ContainsFunc(
			accepted.Objects,
			func(child store.Child) bool { return child.Obj.Named() == retained.Named() },
		),
		true,
	)
	assert.Equal(
		t,
		slices.ContainsFunc(
			accepted.Objects,
			func(child store.Child) bool { return child.Obj.Named() == introduced.Named() },
		),
		true,
	)
	assert.Equal(
		t,
		slices.ContainsFunc(
			accepted.Objects,
			func(child store.Child) bool { return child.Obj.Named() == discarded.Named() },
		),
		false,
	)
	assert.Equal(t, len(st.Warnings()), 0)
	assert.Equal(t, st.GetObject(retained.Named()), manifest.BaseManifest(retained))
	retainedIndex := slices.IndexFunc(
		accepted.Objects,
		func(child store.Child) bool { return child.Obj.Named() == retained.Named() },
	)
	assert.FatalIf(t, retainedIndex < 0, "accepted application is missing")
	observation, err := d.observeRequiredObjects(t.Context(), &selection.AdmissionResult{
		Objects: []store.Child{accepted.Objects[retainedIndex]}, Absent: []manifest.NamedResource{retained.Named()},
	})
	assert.NoError(t, err)
	assert.Equal(t, observation.changed, true)
	assert.Equal(t, observation.refresh, true)
	marker := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "marker"}
	_, indexed := l.Index.Lookup(marker)
	assert.Equal(t, indexed, false)
	advanced, err := d.advanceTest(t.Context(), &accepted)
	assert.NoError(t, err)
	assert.Equal(t, advanced, true)
	assert.Equal(t, d.entryReceiptRequest, afterReceipt)
	_, indexed = l.Index.Lookup(marker)
	assert.Equal(t, indexed, true)
	assert.FatalIf(t, d.prepared[retained.Named()] == nil, "accepted producer demand did not prepare its declaration")
	assert.Equal(t, d.prepared[retained.Named()].registered, true)
	assert.Equal(t, len(d.prepared[retained.Named()].children), 1)
	assert.Equal(t, d.prepared[retained.Named()].children[0].Obj.Named(), target)
	for _, warning := range st.Warnings() {
		assert.Equal(t, warning.Message, "accepted proposal")
		assert.Equal(t, warning.Resource == retained.Named() || warning.Resource == introduced.Named(), true)
	}
	assert.Equal(t, len(st.Warnings()), 2)
	frontier, err := l.Options.Session.RefreshProducers(t.Context(), &accepted, d.request)
	assert.NoError(t, err)
	assert.Equal(t, len(slices.Collect(frontier.Demands())), 0)
	frontier.Retire()
}

func TestAdvance_InitiallyPresentApplicationDoesNotRefresh(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: []\n")},
		"marker.yaml":              {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker}\n")},
	}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	application := &manifest.Kustomization{Name: "present", Namespace: "apps", Path: "payload"}
	st.AddObject(application)
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	d := newDiscoverer(&Config{Input: input, Path: root, Store: st, Loader: l})
	defer d.Close()
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Retire()
	reads.RequireData(application.Named())
	accepted, committed, err := l.Options.Session.Admit(t.Context(), selection.AdmissionOptions{
		Request: selection.PrepareRequest{Missing: l.Options.Missing, Reads: &reads}, Stage: selection.Construction,
	})
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, len(accepted.Absent), 0)
	advanced, err := d.advanceTest(t.Context(), &accepted)
	assert.NoError(t, err)
	assert.Equal(t, advanced, false)
	assert.Equal(t, len(l.Index.Records), 0)
}

func TestAdvance_EmptyRootHistory(t *testing.T) {
	t.Parallel()
	for _, available := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "complete empty"}[available], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "payload"}
			st.AddObject(owner)
			l := loader.New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			l.Options.Session.Include(owner.Named())
			d := newDiscoverer(&Config{Input: input, Path: root, Store: st, Loader: l})
			defer d.Close()
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			reads.RequireData(owner.Named())
			var calls int
			d.request.HistoryOwners = map[manifest.NamedResource][]manifest.SourceEvidence{
				owner.Named(): {{Source: input.HistoryIdentity(manifest.BootstrapSourceID)}},
			}
			if available {
				d.request.ReadHistory = func(_ context.Context, demand *selection.Demand) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
					calls++
					assert.Equal(t, demand.Identity, owner.Named())
					assert.Equal(t, demand.Root, "")
					assert.Equal(t, demand.History, true)
					return func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}, true
				}
			}
			request := d.prepareRequest(&selection.PrepareRequest{Missing: l.Options.Missing, Reads: &reads})
			accepted, committed, err := l.Options.Session.Admit(
				t.Context(),
				selection.AdmissionOptions{Request: request, Stage: selection.Construction},
			)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			history, answered := accepted.Request.History[owner.Named()]
			assert.Equal(t, answered, true)
			assert.Equal(t, history.Complete, available)
			assert.Equal(t, history.Records != nil, available)
			assert.Equal(t, calls, map[bool]int{false: 0, true: 1}[available])
			assert.Equal(t, len(accepted.Demands), 0)
			advanced, err := d.advanceTest(t.Context(), &accepted)
			assert.NoError(t, err)
			assert.Equal(t, advanced, false)
			assert.Equal(t, l.Options.Session.Snapshot().Reconcile(owner.Named()), true)
			assert.Equal(t, l.Options.Session.Snapshot().Output(owner.Named()), false)
			var execution store.ReadSet
			execution.Init(st, manifest.NamedResource{}, nil)
			defer execution.Retire()
			assert.Equal(t, execution.OrdinaryDemand(owner.Named()), false)
			assert.Equal(t, l.Options.Session.Snapshot().OrdinaryDemand(owner.Named()), true)
			assert.Equal(t, d.request.ReadHistory == nil, !available)
			assert.Equal(t, d.request.Preparation, request.Preparation)
		})
	}
}

func TestQualification_EmptyRootHistory(t *testing.T) {
	t.Parallel()
	for _, construction := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "discovery"}[construction], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "payload"}
			st.AddObject(owner)
			l := loader.New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			l.Options.Session.Include(owner.Named())
			q := &entryQualification{loader: l}
			defer func() { q.preparation.Retire() }()
			request := selection.PrepareRequest{
				Missing: l.Options.Missing,
				HistoryOwners: map[manifest.NamedResource][]manifest.SourceEvidence{
					owner.Named(): {{Source: input.HistoryIdentity(manifest.BootstrapSourceID)}},
				},
			}
			if construction {
				d := newDiscoverer(&Config{Input: input, Path: root, Store: st, Loader: l})
				defer d.Close()
				q.session = d
				d.request.HistoryOwners, request.HistoryOwners = request.HistoryOwners, nil
			}
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			reads.RequireQualification(owner.Named())
			request.Reads = &reads
			var publications int
			unsubscribe := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
				if id == owner.Named() {
					publications++
				}
			}, false)
			defer unsubscribe()
			walk := qualificationWalk{q: q, store: st, reads: &reads}
			committed, err := walk.commit(t.Context(), &request)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			assert.Equal(t, l.Options.Session.Snapshot().Reconcile(owner.Named()), true)
			assert.Equal(t, publications, map[bool]int{false: 1, true: 0}[construction])
			assert.Equal(t, l.Options.Session.Snapshot().OrdinaryDemand(owner.Named()), false)
		})
	}
}

//nolint:gocognit // Cold, reused, and replaced proposals MUST compare acquisition, acceptance, and attribution.
func TestAdmit_AcceptedChildWarningsSurvivePublication(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		prepared, replacement bool
	}{
		{name: "cold"},
		{name: "reused preparation", prepared: true},
		{name: "source replacement", prepared: true, replacement: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
				"owner.yaml": {
					Data: []byte(
						"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: apps}\nspec: {interval: 1m, path: ./local, sourceRef: {kind: GitRepository, name: cluster}}\n",
					),
				},
				"local/kustomization.yaml": {Data: []byte("resources: [secret.yaml]\n")},
				"local/secret.yaml": {
					Data: []byte(
						"apiVersion: v1\nkind: Secret\nmetadata: {name: input, namespace: apps}\nstringData: {identity: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n",
					),
				},
			}}
			bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
			st.AddObject(bootstrap)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			source := &manifest.GitRepository{Name: "cluster", Namespace: "apps", URL: "https://example.test/cluster"}
			st.AddObject(source)
			l := loader.New(st)
			defer l.Close()
			l.SourceRoot = root
			l.Options.Changes = change.NewSet([]string{"owner.yaml"})
			configureSelection(t, l, selection.Request{Changed: true})
			d, err := beginTest(t.Context(), Config{
				Input: input, Path: root, Store: st, Loader: l,
				Selection: selection.Request{Changed: true}, SelfURLs: []string{source.URL},
			})
			assert.NoError(t, err)
			defer d.Close()
			st.SetDiscoveryIndex(l.Index)
			id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
			assert.Equal(t, st.GetObject(id), nil)
			supplied := &manifest.Secret{
				Name:       id.Name,
				Namespace:  id.Namespace,
				StringData: map[string]any{"identity": "supplied"},
			}
			st.AddDeclaration(store.Declaration{Obj: supplied, Origin: manifest.SuppliedDeclaration})
			occurrence, known := l.Occurrence(id)
			assert.Equal(t, known, true)
			child, err := l.OccurrenceObject(t.Context(), &occurrence, input)
			assert.NoError(t, err)
			want := []manifest.Warning{
				{
					Resource: id,
					Category: manifest.WarnSOPSUnreadable,
					Message:  "SOPS-encrypted data is unreadable; supply dummy values for references; re-encryption cannot be compared",
					Detail:   []string{"identity"},
				},
			}
			assert.Diff(t, child.Warnings, want)
			var preparation *selection.Preparation
			defer func() { preparation.Retire() }()
			var required store.ReadSet
			required.Init(st, manifest.NamedResource{}, nil)
			defer required.Retire()
			required.RequireData(id)
			request := selection.PrepareRequest{
				Missing:    l.Options.Missing,
				Invocation: true,
				Changes:    l.Options.Changes,
				Reads:      &required,
			}
			if tc.prepared {
				request.Preparation = &preparation
				initial, prepareErr := prepareSelection(t.Context(), t, l, request)
				assert.NoError(t, prepareErr)
				index := slices.IndexFunc(
					initial.Objects,
					func(child store.Child) bool { return child.Obj.Named() == id },
				)
				assert.FatalIf(t, index < 0, "prepared indexed child is missing")
				assert.Diff(t, initial.Objects[index].Warnings, want)
				assert.Equal(t, initial.Objects[index].SourceSupport, manifest.BootstrapSourceID)
				initial.Retire()
			}
			if tc.replacement {
				published, replaced, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
				go func() {
					defer close(finished)
					<-published
					successor := *input
					successor.Content = "replacement"
					st.SetArtifact(manifest.BootstrapSourceID, &successor)
					close(replaced)
				}()
				var once atomic.Bool
				stop := st.AddListener(store.EventObjectAdded, func(object manifest.NamedResource, _ any) {
					if object == (manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "owner"}) &&
						once.CompareAndSwap(false, true) {
						close(published)
						<-replaced
					}
				}, false)
				defer stop()
				defer func() { <-finished }()
			}
			accepted, committed, err := l.Options.Session.Admit(t.Context(), selection.AdmissionOptions{
				Request: request, Stage: selection.Construction,
			})
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			index := slices.IndexFunc(accepted.Objects, func(child store.Child) bool { return child.Obj.Named() == id })
			if tc.replacement {
				assert.Equal(t, index, -1)
			} else {
				assert.FatalIf(t, index < 0, "accepted indexed child is missing")
				assert.Diff(t, accepted.Objects[index].Warnings, want)
				assert.Equal(t, accepted.Objects[index].SourceSupport, manifest.BootstrapSourceID)
			}
			assert.Equal(t, len(st.Warnings()), 0)
			_, err = d.advanceTest(t.Context(), &accepted)
			assert.NoError(t, err)
			if tc.replacement {
				assert.Equal(t, len(st.Warnings()), 0)
			} else {
				want[0].Subject = manifest.DiagnosticIdentity{ID: id, Resource: id}
				want[0].Count = 1
				assert.Diff(t, st.Warnings(), want)
			}
			assert.Equal(t, st.GetObject(id), manifest.BaseManifest(supplied))
		})
	}
}

func TestAdvance_GeneratedSourceProducerAdmission(t *testing.T) {
	t.Parallel()
	for _, declared := range []bool{false, true} {
		name := "missing artifact declaration"
		if declared {
			name = "declaration without content"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system", Path: "factory"}
			target := (&manifest.ExternalArtifact{Name: "bundle", Namespace: "flux-system"}).Named()
			generator := manifest.NamedResource{
				Group:     "source.extensions.fluxcd.io",
				Kind:      "ArtifactGenerator",
				Name:      "generator",
				Namespace: "flux-system",
			}
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "flux-system", Path: "payload"}
			consumer.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: target.Kind, Name: target.Name}
			input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
				"entry/kustomization.yaml":   {Data: []byte("resources: []\n")},
				"factory/kustomization.yaml": {Data: []byte("resources: [generator.yaml, sibling.yaml]\n")},
				"factory/generator.yaml": {
					Data: []byte(
						"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: flux-system}\nspec:\n  sources:\n  - {alias: git, kind: GitRepository, name: flux-system}\n  artifacts:\n  - name: bundle\n    copy:\n    - {from: '@git/payload/**', to: '@artifact/payload/'}\n",
					),
				},
				"factory/sibling.yaml": {
					Data: []byte(
						"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: sibling, namespace: flux-system}\nspec: {interval: 1m, path: other, sourceRef: {kind: GitRepository, name: flux-system}}\n",
					),
				},
				"payload/kustomization.yaml": {Data: []byte("resources: []\n")},
			}}
			bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
			st.AddObject(bootstrap)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			st.AddObject(parent)
			st.AddObject(consumer)
			l := loader.New(st)
			l.SourceRoot = root
			l.Options.Changes = change.NewSet([]string{"consumer.yaml"})
			configureSelection(
				t,
				l,
				selection.Request{Changed: true, Roots: []manifest.NamedResource{consumer.Named()}},
			)
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			assert.Equal(t, l.IsFileIndexed(generator), true)
			assert.Equal(t, st.GetDeclaration(generator), manifest.BaseManifest(nil))
			production := manifest.Production{
				Owner:       parent.Named(),
				Declaration: generator,
				Authored:    generator,
				Source:      manifest.BootstrapSourceID,
				Path:        filepath.Join(root, "factory", "generator.yaml"),
				File:        "factory/generator.yaml",
			}
			l.Index.Records[target] = manifest.DiscoveryRecord{Productions: []manifest.Production{production}}
			record := l.Index.Records[parent.Named()]
			record.Builder = "entry"
			l.Index.Records[parent.Named()] = record
			facts := &retryAdmissionFacts{
				Loader: l,
				children: map[manifest.NamedResource]store.Child{
					parent.Named(): {Obj: parent, SourceSupport: manifest.BootstrapSourceID},
				},
			}
			l.Options.Session, err = selection.New(
				st,
				facts,
				selection.Request{Changed: true, Roots: []manifest.NamedResource{consumer.Named()}},
			)
			assert.NoError(t, err)
			if declared {
				st.AddObject(&manifest.ExternalArtifact{Name: target.Name, Namespace: target.Namespace})
			}
			cacheOwner := cachetest.New(t, "", cache.New)
			trees := kustomize.NewTreeCache(cacheOwner, nil, nil)
			t.Cleanup(trees.Close)
			d := newDiscoverer(
				&Config{
					Input:     input,
					Path:      filepath.Join(root, "entry"),
					Store:     st,
					Loader:    l,
					Cache:     cacheOwner,
					Trees:     trees,
					Selection: selection.Request{Changed: true},
				},
			)
			defer d.Close()
			var preparation *selection.Preparation
			defer func() { preparation.Retire() }()
			var required store.ReadSet
			required.Init(st, consumer.Named(), nil)
			defer required.Retire()
			required.Require(manifest.DependencyRef{NamedResource: target, Artifact: true})
			request := selection.PrepareRequest{
				Missing:     l.Options.Missing,
				Reads:       &required,
				Preparation: &preparation,
				Changes:     l.Options.Changes,
			}
			accepted, committed, err := l.Options.Session.Admit(
				t.Context(),
				selection.AdmissionOptions{Request: request, Stage: selection.Construction},
			)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			snapshot := l.Options.Session.Snapshot()
			frontier, err := l.Options.Session.RefreshProducers(t.Context(), &accepted, request)
			assert.NoError(t, err)
			demands := slices.Collect(frontier.Demands())
			assert.Equal(t, l.Options.Session.Snapshot() == snapshot, true)
			assert.Equal(t, slices.ContainsFunc(demands, func(demand selection.Demand) bool {
				return demand.Identity == parent.Named() && manifest.KindOf(demand.Witness).Structural
			}), true)
			for _, demand := range append(slices.Clone(accepted.Demands), demands...) {
				assert.Equal(t, demand.Identity == generator, false)
			}
			frontier.Retire()
			advanced, err := d.advanceTest(t.Context(), &accepted)
			assert.NoError(t, err)
			assert.Equal(t, advanced, true)
			frame := d.prepared[parent.Named()]
			assert.FatalIf(t, frame == nil, "physical parent demand did not prepare its package")
			assert.Equal(t, frame.registered, true)
			assert.Equal(
				t,
				slices.ContainsFunc(
					frame.children,
					func(child store.Child) bool { return child.Obj.Named() == generator },
				),
				true,
			)
			assert.Equal(t, st.GetArtifact(target, store.ArtifactContent), store.Artifact(nil))
			next, committed, err := l.Options.Session.Admit(
				t.Context(),
				selection.AdmissionOptions{Request: d.request, Previous: &accepted, Stage: selection.Construction},
			)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			assert.Equal(t, slices.Contains(next.Needed, generator), true)
			assert.Equal(t, slices.Contains(next.Ordinary, generator), true)
			assert.Equal(t, l.Options.Session.Snapshot().Reconcile(generator), true)
			assert.Equal(t, l.Options.Session.Snapshot().OrdinaryDemand(generator), true)
			assert.Equal(t, st.GetDeclaration(generator) != nil, true)
			runtime, err := l.Options.Session.RefreshProducers(t.Context(), &next, next.Request)
			assert.NoError(t, err)
			assert.Equal(t, len(slices.Collect(runtime.Demands())), 0)
			assert.Equal(t, len(runtime.Needed), 0)
			assert.Equal(t, len(runtime.Ordinary), 0)
			runtime.Retire()

			sibling := manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Name:      "sibling",
				Namespace: "flux-system",
			}
			assert.Equal(t, l.Options.Session.Snapshot().Output(sibling), false)
			assert.Equal(t, l.Options.Session.Snapshot().Reconcile(sibling), false)
			assert.Equal(t, st.GetArtifact(target, store.ArtifactContent), store.Artifact(nil))
			st.RecordEmitted(parent.Named(), &store.Emission{Complete: true, Children: frame.children}, nil)
			st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
			tasks := task.NewBounded(2)
			controller := artifactgenerator.New(st, tasks, cacheOwner)
			tasks.Go(t.Context(), "ordinary generator", func(ctx context.Context) {
				assessment := controller.ReconcileNode(ctx, generator, manifest.DrainNone, schedule.Result{})
				t.Cleanup(assessment.Close)
				assert.Equal(t, assessment.Outcome(), schedule.Terminal)
			})
			tasks.BlockTillDone()
			assert.Equal(t, tasks.Failures(), 0)
			artifact, ok := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, !ok || artifact.FS == nil, "ordinary generator did not publish readable source content")
			data, err := fs.ReadFile(artifact.FS, "payload/kustomization.yaml")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "resources: []\n")
			supplied, err := l.Options.Session.RefreshProducers(t.Context(), &next, next.Request)
			assert.NoError(t, err)
			assert.Equal(t, len(slices.Collect(supplied.Demands())), 0)
			assert.Equal(t, len(supplied.Needed), 0)
			supplied.Retire()
		})
	}
}
