package discovery

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/fstest"
	"weak"

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEntryQualification_ReceiptInputs(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"identical", "ancestor chart changed", "acquisition failure", "canceled", "supplied unchanged", "supplied changed", "supplied removed", "repository precedence"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testEntryQualificationReceiptInputsCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func testEntryQualificationReceiptInputsCase(t *testing.T, testCaseInput *string) {
	t.Helper()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"entry/child.yaml": {Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./workload\n"))},
	}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	app := testutil.KustomizationYAML(
		"applier",
		"apps",
		"  path: ./entry\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
	)
	documents, err := manifest.DecodeDocs(strings.NewReader(app), manifest.RequiredDocuments)
	assert.NoError(t, err)
	evidence := kustomize.EntryEvidence{
		Producer:    testutil.KustomizationID("apps", "applier"),
		Application: documents[0].Document,
		Receipt:     manifest.NewRenderReceipt("entry", func(a, b any) bool { return a == b }),
	}
	result, err := Run(t.Context(), testConfig(t, Config{
		Input: input, Path: filepath.Join(root, "entry"), Store: st, Selection: selection.Request{Changed: true},
	}))
	assert.NoError(t, err)
	a := result.Admission
	q := a.qualification
	if q == nil {
		t.Fatal("unresolved entry has no receipt qualification")
	}
	for _, name := range []string{"ancestor-chart", "nested-chart"} {
		id := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindOCIRepository,
			Namespace: "apps",
			Name:      name,
		}
		artifact := &store.SourceArtifact{
			Kind:     id.Kind,
			FS:       fstest.MapFS{},
			Key:      "same-family",
			Revision: "same-tag",
			Content:  manifest.Fingerprint(name),
		}
		binding := artifact.Input(store.ArtifactContent | store.ArtifactRevision)
		evidence.Inputs = append(
			evidence.Inputs,
			kustomize.EntryInput{Resource: id, Content: artifact.Content, Binding: binding},
		)
		q.refresh = append(q.refresh, id)
	}
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "settings"}
	data := &manifest.Secret{
		Name:       secret.Name,
		Namespace:  secret.Namespace,
		Data:       nil,
		StringData: map[string]any{"VALUE": "original"},
	}
	switch *testCaseInput {
	case "supplied unchanged", "supplied changed", "supplied removed", "repository precedence":
		evidence.Inputs = append(evidence.Inputs,
			kustomize.EntryInput{
				Resource: secret,
				Content:  manifest.Fingerprint(data.Snapshot()),
			})
		if (*testCaseInput) != "supplied removed" {
			value := "original"
			if (*testCaseInput) == "supplied changed" || (*testCaseInput) == "repository precedence" {
				value = "changed"
			}
			st.AddDeclaration(store.Declaration{
				Obj: &manifest.Secret{
					Name:       secret.Name,
					Namespace:  secret.Namespace,
					Data:       nil,
					StringData: map[string]any{"VALUE": value},
				},
				Role: store.SuppliedDeclaration,
			})
		}
		if (*testCaseInput) == "repository precedence" {
			st.AddObject(data, testutil.ObjectDocument(data))
		}
	}
	provider := &manifest.ResourceSet{Name: "provider", Namespace: "apps"}
	st.AddObject(provider)
	var required store.ReadSet
	required.Init(st, provider.Named(), nil)
	defer required.Retire()
	required.RequireQualification(provider.Named())
	proposal, err := prepareSelection(t.Context(), t, q.loader, selection.PrepareRequest{Reads: &required})
	assert.NoError(t, err)
	committed, err := commitSelection(t.Context(), q.loader, proposal, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, !committed, false)
	evidence.Owner, evidence.Root = provider.Named(), provider.Named()
	a.entryDemand = true
	q.closed = false
	q.previous = evidence
	assertEntryQualificationReceiptRefresh(t, a, &evidence, *testCaseInput)
}

func assertEntryQualificationReceiptRefresh(
	t *testing.T, a *Admission, evidence *kustomize.EntryEvidence, scenario string,
) {
	t.Helper()
	q, st := a.qualification, a.store
	assertPending := func() {
		t.Helper()
		root, binding, bindErr := a.BindEntry(t.Context(), st)
		assert.NoError(t, bindErr)
		assert.Equal(t, root, (*manifest.Kustomization)(nil))
		assert.Equal(t, binding, Binding{})
		assert.Equal(t, a.applications.Load() == nil, true)
		assert.Diff(t, a.EntryProducers(), q.refresh)
		assert.Equal(t, q.waiting != nil, true)
	}
	assertPending()
	publish := func(i int) {
		expected := &evidence.Inputs[i]
		content := expected.Content
		if scenario == "ancestor chart changed" && i == 0 {
			content = manifest.Fingerprint("new chart behind same tag")
		}
		st.SetArtifact(
			expected.Resource,
			&store.SourceArtifact{
				Kind:     expected.Resource.Kind,
				FS:       fstest.MapFS{},
				Key:      "same-family",
				Revision: "same-tag",
				Content:  content,
			},
		)
		st.UpdateStatus(expected.Resource, store.StatusInfo{Status: store.StatusReady})
	}
	if scenario == "identical" {
		var release, settled [2]chan struct{}
		for i := range release {
			release[i], settled[i] = make(chan struct{}), make(chan struct{})
			go func() {
				defer close(settled[i])
				select {
				case <-release[i]:
					publish(i)
				case <-t.Context().Done():
				}
			}()
		}
		close(release[0])
		<-settled[0]
		assertPending()
		close(release[1])
		<-settled[1]
	} else {
		for i := range q.refresh {
			publish(i)
		}
	}
	if scenario == "acquisition failure" {
		st.SetArtifact(q.refresh[0], nil)
		st.UpdateFailure(q.refresh[0], manifest.NewError(manifest.ErrInput, errors.New("acquisition failed"), ""))
	}
	ctx := t.Context()
	if scenario == "canceled" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}
	bound, binding, err := a.BindEntry(ctx, st)
	if binding.Reads != nil {
		defer binding.Reads.Close()
	}
	if scenario == "identical" {
		accepted := acceptEntryForTest(t, a, &binding)
		assert.FatalIf(t, accepted.Entry == nil, "settled sources lost the loaded receipt")
		assert.Equal(t, accepted.Entry.Receipt, evidence.Receipt)
	}
	assert.Equal(t, errors.Is(err, context.Canceled), scenario == "canceled")
	assert.Equal(
		t,
		bound != nil,
		scenario == "identical" || scenario == "supplied unchanged" ||
			scenario == "repository precedence",
	)
	assert.Equal(t, a.applications.Load() == nil, true)
}

func TestEntryQualification_UnrequestedReceiptDoesNotRefresh(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := manifest.NamedResource{Kind: manifest.KindOCIRepository, Name: "cached", Namespace: "apps"}
	q := &entryQualification{
		refresh: []manifest.NamedResource{source},
		previous: kustomize.EntryEvidence{
			Receipt: manifest.NewRenderReceipt("entry", func(a, b any) bool { return a == b }),
		},
	}
	var reads store.ReadSet
	reads.Init(st, manifest.BootstrapSourceID, nil)
	defer reads.Retire()
	walk := qualificationWalk{q: q, admission: &Admission{}, store: st, reads: &reads}
	reused, pending := walk.reusePrevious(t.Context(), q.previous.Owner)
	assert.Equal(t, reused, false)
	assert.Equal(t, pending, false)
	assert.Equal(t, q.receiptPending, false)
}

type namedReceiptCase struct {
	name          string
	publish       bool
	complete      bool
	member        bool
	partial       bool
	reuse         bool
	replay        bool
	publishedRoot bool
	delayed       bool
	invalidate    bool
	stale         bool
	terminal      store.Status
}

func TestEntryQualification_NamedReceiptAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []namedReceiptCase{
		{name: "all targets", publish: true, complete: true, member: true, reuse: true},
		{name: "published sync root", publish: true, complete: true, member: true, publishedRoot: true},
		{name: "delayed sync root", complete: true, member: true, publishedRoot: true, delayed: true},
		{name: "stale sync root", complete: true, member: true, publishedRoot: true, delayed: true, stale: true},
		{
			name: "resumed proposal invalidated", complete: true, member: true, publishedRoot: true,
			delayed: true, invalidate: true,
		},
		{name: "terminal omission", complete: true, publishedRoot: true, delayed: true, terminal: store.StatusReady},
		{name: "terminal failure", publishedRoot: true, delayed: true, terminal: store.StatusFailed},
		{name: "partial reach", publish: true, complete: true, member: true, partial: true},
		{name: "missing inventory", replay: true},
		{name: "complete omission", publish: true, complete: true},
		{name: "partial inventory", publish: true, member: true, replay: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.test(t)
		})
	}
}

func (tc namedReceiptCase) test(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: [first.yaml]\n")},
		"entry/first.yaml": {
			Data: []byte(testutil.KustomizationYAML("first", "apps", "  path: ./workload\n")),
		},
		"other/second.yaml": {
			Data: []byte(testutil.KustomizationYAML("second", "apps", "  path: ./workload\n")),
		},
	}}
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	var owner manifest.BaseManifest = &manifest.Kustomization{Name: "factory", Namespace: "apps", Path: "./factory"}
	if tc.publishedRoot && !tc.delayed {
		owner = &manifest.HelmRelease{Name: "factory", Namespace: "apps"}
	}
	docs, err := manifest.DecodeDocs(strings.NewReader(testutil.KustomizationYAML(
		"sync", "apps", "  path: ./entry\n",
	)), manifest.RequiredDocuments)
	assert.NoError(t, err)
	document := docs[0].Document
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	st.AddObject(owner)
	publish := func(complete, member, stale bool) {
		output := &store.Emission{Complete: complete}
		if member {
			child := store.Child{Obj: parsed.Object, Doc: document, VisibleGeneration: true}
			if tc.publishedRoot {
				child.Productions = []manifest.Production{{Owner: owner.Named()}}
			}
			output.Children = []store.Child{child}
		}
		if !stale {
			st.RecordEmitted(owner.Named(), output, nil)
			return
		}
		input := &manifest.ConfigMap{Name: "sync-input", Namespace: "apps"}
		st.AddObject(input)
		var reads store.ReadSet
		reads.Init(st, owner.Named(), nil)
		defer reads.Retire()
		reads.GetObject(input.Named())
		reads.Bind(output)
		st.RecordEmitted(owner.Named(), output, nil)
		st.DeleteObject(input.Named())
	}
	if tc.publish {
		publish(tc.complete, tc.member, false)
	}
	l := loader.New(st)
	l.SourceRoot = root
	t.Cleanup(l.Retire)
	q := &entryQualification{loader: l, trees: testTrees(t), previous: kustomize.EntryEvidence{
		Application: document, Owner: owner.Named(), Root: owner.Named(),
		Receipt: manifest.NewRenderReceipt("entry", func(a, b any) bool { return a == b }),
	}}
	a := &Admission{input: input, path: root, entryDemand: true, qualification: q}
	files := []string{"entry/first.yaml"}
	if tc.partial {
		files = append(files, "other/second.yaml")
	}
	for _, file := range files {
		a.entryTargets = append(a.entryTargets, entryTarget{
			occurrence: selection.OccurrenceID{Source: root, File: file},
			origin:     filepath.Join(root, file),
		})
	}
	if tc.publishedRoot {
		tc.testPublished(t, a, parsed.Object.Named(), publish)
		return
	}
	var reads store.ReadSet
	reads.Init(st, manifest.BootstrapSourceID, nil)
	defer reads.Retire()
	walk := qualificationWalk{q: q, admission: a, store: st, reads: &reads}
	reused, pending := walk.reusePrevious(t.Context(), q.previous.Owner)
	assert.Equal(t, pending, tc.replay)
	assert.Equal(t, reused, tc.reuse)
	assert.Equal(t, q.replay != (manifest.NamedResource{}), tc.replay)
	if tc.replay {
		assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{owner.Named()})
	}
	assert.Equal(t, len(q.matches) != 0 && q.matches[0].receipt != nil, tc.reuse)
	if tc.reuse {
		assert.Equal(t, q.winners[a.entryTargets[0].occurrence], parsed.Object.Named())
	}
}

func (tc namedReceiptCase) testPublished(
	t *testing.T, a *Admission, syncRoot manifest.NamedResource, publish func(bool, bool, bool),
) {
	t.Helper()
	q := a.qualification
	q.previous.Root, q.previous.Producer = syncRoot, syncRoot
	q.active = []manifest.NamedResource{q.previous.Owner}
	a.loader, a.path = q.loader, filepath.Join(a.path, "entry")
	if tc.delayed {
		configureSelection(t, q.loader, selection.Request{Changed: true})
		qualifyForTest(t, q.loader, q.previous.Owner)
		tc.assertPendingReceipt(t, a, q.previous.Owner, publish)
		publish(tc.complete, tc.member, false)
		if tc.terminal != "" {
			q.loader.Store.UpdateStatus(q.previous.Owner, store.StatusInfo{Status: tc.terminal})
		}
	}
	var invalidate func()
	if tc.invalidate {
		invalidate = func() { publish(true, false, false) }
	}
	assertPublishedReceipt(t, a, tc.terminal, invalidate)
}

func assertPublishedReceipt(t *testing.T, a *Admission, terminal store.Status, invalidate func()) {
	t.Helper()
	st := a.loader.Store
	loaded := a.qualification.previous
	selected, proposal, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	if terminal == store.StatusFailed {
		assert.Equal(t, selected, (*manifest.Kustomization)(nil))
		assert.Equal(t, proposal, Binding{})
		assert.Equal(t, a.loader.Options.Session.EntryReplay(loaded.Owner), false)
		return
	}
	assert.FatalIf(t, selected == nil, "qualification did not propose an entry")
	if invalidate != nil {
		assert.FatalIf(t, proposal.Entry == nil, "qualification lost the resumed receipt")
		invalidate()
		rejected, err := a.AcceptEntry(t.Context(), proposal)
		assert.NoError(t, err)
		assert.Equal(t, rejected, Binding{})
		_, fresh, err := a.BindEntry(t.Context(), st)
		assert.NoError(t, err)
		assert.Equal(t, fresh.Entry, (*kustomize.EntryEvidence)(nil))
		fresh.Reads.ReleaseCapture()
		st.Retire(fresh.Reads)
		return
	}
	accepted := acceptEntryForTest(t, a, &proposal)
	if terminal == store.StatusReady {
		assert.Equal(t, accepted.Entry, (*kustomize.EntryEvidence)(nil))
		assert.Equal(t, a.loader.Options.Session.EntryReplay(loaded.Owner), false)
		return
	}
	assert.Equal(t, accepted.Producer, loaded.Producer)
	assert.FatalIf(t, accepted.Entry == nil, "the accepted sync root lost its loaded receipt")
	assert.Diff(t, accepted.Entry.Application, loaded.Application)
	assert.Equal(t, accepted.Entry.Receipt.Equal(loaded.Receipt), true)
}

func (tc namedReceiptCase) assertPendingReceipt(
	t *testing.T, a *Admission, owner manifest.NamedResource, publish func(bool, bool, bool),
) {
	t.Helper()
	st := a.loader.Store
	assertPending := func() {
		t.Helper()
		selected, proposal, err := a.BindEntry(t.Context(), st)
		assert.NoError(t, err)
		assert.Equal(t, selected, (*manifest.Kustomization)(nil))
		assert.Equal(t, proposal, Binding{})
		assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{owner})
		assert.Equal(t, a.loader.Options.Session.EntryReplay(owner), true)
	}
	assertPending()
	waiting := a.qualification.waiting
	assert.FatalIf(t, waiting == nil, "qualification did not retain its pending capture")
	st.AddObject(&manifest.ConfigMap{Name: "unrelated", Namespace: "apps"})
	assertPending()
	assert.Equal(t, a.qualification.waiting, waiting)
	publish(false, true, false)
	assertPending()
	if tc.stale {
		st.UpdateStatus(owner, store.StatusInfo{Status: store.StatusReady})
		publish(true, true, true)
		output, _ := st.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
		assert.FatalIf(t, output == nil || !output.Complete, "the fixture did not publish complete inventory")
		assert.Equal(t, output.OutputReads.Valid(), false)
		assertPending()
	}
}

func TestRun_EntryQualificationLifetime(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"bound", "unresolved changed-only", "nil trees"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := fstest.MapFS{
				"entry/child.yaml": {Data: []byte(testutil.KustomizationYAML("child", "apps", "  path: ./workload\n"))},
			}
			if scenario == "bound" {
				input["entry/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [child.yaml]\n")}
			}
			st := store.New()
			l := loader.New(st)
			cfg := testConfig(t, Config{
				Store:  st,
				Loader: l,
				Input:  &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: input},
				Path:   filepath.Join(root, "entry"),
			})
			if scenario == "unresolved changed-only" {
				cfg.Selection = selection.Request{Changed: true}
			}
			if scenario == "nil trees" {
				cfg.Trees = nil
			}
			session, err := beginTest(t.Context(), cfg)
			assert.NoError(t, err)
			result, err := session.Freeze(t.Context())
			assert.NoError(t, err)
			a := result.Admission
			assert.Equal(t, a.entryBound.Load(), scenario == "bound")
			assert.Equal(t, a.qualification != nil, scenario == "unresolved changed-only")
			assert.Equal(t, session.entryState.loader == nil, scenario != "unresolved changed-only")
			if scenario != "unresolved changed-only" {
				assert.Equal(t, session.entryState.loader == nil, true)
				return
			}
			q := a.qualification
			assert.Equal(t, q, &session.entryState)
			assert.Equal(t, q.loader, l)
			applier := testutil.KustomizationYAML(
				"applier",
				"apps",
				"  path: ./entry\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			)
			documents, err := manifest.DecodeDocs(strings.NewReader(applier), manifest.RequiredDocuments)
			assert.NoError(t, err)
			parsed, err := manifest.ParseDoc(documents[0].Document, manifest.ParseDocOptions{})
			object := parsed.Object
			assert.NoError(t, err)
			st.AddObject(object)
			a.AddApplication(st, object)
			bound, binding, err := a.BindEntry(t.Context(), st)
			assert.NoError(t, err)
			assert.Equal(t, bound != nil, true)
			assert.Equal(t, binding.Producer, object.Named())
			if binding.Reads != nil {
				acceptEntryForTest(t, a, &binding)
			}
			assert.Equal(t, a.entryBound.Load(), true)
			assert.Equal(t, a.qualification == nil, true)
			assert.Equal(t, q.loader == nil, true)
		})
	}
	t.Run("private package", testRunPrivateEntryQualificationLifetime)
}

func testRunPrivateEntryQualificationLifetime(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"cancellation", "invocation close"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			st := store.New()
			bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
			st.AddObject(bootstrap)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			l := loader.New(st)
			l.SourceRoot = root
			t.Cleanup(l.Close)
			operand := &manifest.HelmRelease{Name: "private", Namespace: "apps"}
			id := operand.Named()
			q := &entryQualification{loader: l, packages: [][]selection.OccurrenceFacts{{{
				Identity: id, Object: operand,
			}}}}
			a := &Admission{input: input, path: root, loader: l, entryDemand: true, qualification: q}
			result := &Result{Admission: a, loader: l}
			t.Cleanup(result.Close)
			unsubscribe := st.AddListener(store.EventObjectAdded, func(_ manifest.NamedResource, payload any) {
				if object, ok := payload.(manifest.BaseManifest); ok {
					a.AddApplication(st, object)
				}
			}, false)
			defer unsubscribe()
			assert.Equal(t, st.GetDeclaration(id), manifest.BaseManifest(nil))
			for range 2 {
				selected, binding, err := a.BindEntry(t.Context(), st)
				assert.NoError(t, err)
				assert.Equal(t, selected, (*manifest.Kustomization)(nil))
				assert.Equal(t, binding, Binding{})
			}
			assert.Equal(t, st.GetDeclaration(id), manifest.BaseManifest(operand))
			assert.Diff(t, a.PrivateEntryOperands(), []manifest.NamedResource{id})
			assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{id})
			assert.FatalIf(t, q.arrivals == nil, "missing private operand capture")
			assert.Equal(t, q.arrivals.object, manifest.BaseManifest(operand))
			captured := weak.Make(q.arrivals)

			if phase == "cancellation" {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				_, _, err := a.BindEntry(ctx, st)
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			} else {
				result.Close()
			}
			assert.Equal(t, len(a.EntryProducers()), 0)
			assert.Equal(t, len(a.PrivateEntryOperands()), 0)
			runtime.GC()
			assert.Equal(t, captured.Value(), (*applicationEvidence)(nil))
		})
	}
}
