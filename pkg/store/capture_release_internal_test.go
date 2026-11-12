package store

import (
	"fmt"
	"maps"
	"runtime"
	"slices"
	"testing"
	"weak"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_AssessmentOrigins(t *testing.T) {
	t.Parallel()
	st := New()
	origin := &manifest.Kustomization{Name: "entry", Namespace: "apps"}
	renderInput := &manifest.ConfigMap{Name: "render", Namespace: "apps", Data: map[string]any{"value": "first"}}
	bindingInput := &manifest.ConfigMap{Name: "binding", Namespace: "apps", Data: map[string]any{"value": "first"}}
	st.AddObjects([]manifest.BaseManifest{origin, renderInput, bindingInput})
	renderWarning := manifest.Warning{
		Resource: origin.Named(),
		Category: manifest.WarnUnresolvedSubstitution,
		Message:  "render",
		Count:    1,
	}
	bindingWarning := manifest.Warning{
		Resource: origin.Named(),
		Category: manifest.WarnConflictingProducers,
		Message:  "binding",
		Count:    1,
	}
	var render, binding ReadSet
	render.Init(st, origin.Named(), nil)
	t.Cleanup(render.Close)
	binding.Init(st, origin.Named(), nil)
	t.Cleanup(binding.Close)
	render.GetObject(renderInput.Named())
	render.RequireData(renderInput.Named())
	render.AddWarning(renderWarning)
	render.Accept(AssessmentReconciled)
	binding.GetObject(bindingInput.Named())
	binding.AddWarning(bindingWarning)
	binding.Accept(AssessmentEntryBinding)
	st.Retire(&binding)
	for range st.Sweep() {
	}
	assert.Equal(t, binding.retired.Load(), false)
	assert.Diff(t, st.Warnings(), warningSubjects([]manifest.Warning{bindingWarning, renderWarning}))
	var required []manifest.NamedResource
	for id := range st.RequiredInputs(origin.Named()) {
		required = append(required, id)
	}
	assert.Diff(t, required, []manifest.NamedResource{renderInput.Named()})

	var replacement ReadSet
	replacement.Init(st, origin.Named(), nil)
	t.Cleanup(replacement.Close)
	replacement.GetObject(renderInput.Named())
	replacement.Accept(AssessmentReconciled)
	assert.Diff(t, st.Warnings(), warningSubjects([]manifest.Warning{bindingWarning}))
	st.AddObject(
		&manifest.ConfigMap{
			Name:      bindingInput.Name,
			Namespace: bindingInput.Namespace,
			Data:      map[string]any{"value": "next"},
		},
	)
	assert.Equal(t, binding.Valid(), false)
	assert.Diff(t, st.Warnings(), []manifest.Warning(nil))
	notified := slices.Collect(st.Sweep())
	assert.Equal(t, slices.Contains(notified, origin.Named()), true)

	var nextRender ReadSet
	nextRender.Init(st, origin.Named(), nil)
	t.Cleanup(nextRender.Close)
	nextRender.GetObject(renderInput.Named())
	nextRender.AddWarning(renderWarning)
	nextRender.Accept(AssessmentReconciled)
	var nextBinding ReadSet
	nextBinding.Init(st, origin.Named(), nil)
	t.Cleanup(nextBinding.Close)
	nextBinding.GetObject(bindingInput.Named())
	nextBinding.Accept(AssessmentEntryBinding)
	assert.Diff(t, st.Warnings(), warningSubjects([]manifest.Warning{renderWarning}))

	t.Run("binding_payload_lifetime", func(t *testing.T) {
		t.Parallel()
		st := New()
		origin := &manifest.Kustomization{Name: "origin", Namespace: "apps"}
		st.AddObject(origin)
		var binding ReadSet
		var storage [2]Input
		binding.Init(st, origin.Named(), storage[:])
		t.Cleanup(binding.Close)
		binding.GetObject(origin.Named())
		declaration := func() weak.Pointer[manifest.Kustomization] {
			child := &manifest.Kustomization{Name: "consumed", Namespace: "apps"}
			st.AddObject(child)
			binding.GetDeclaration(child.Named())
			return weak.Make(child)
		}()
		for i := range 10 {
			input := &manifest.ConfigMap{Name: fmt.Sprintf("input-%d", i), Namespace: "apps"}
			st.AddObject(input)
			binding.GetObject(input.Named())
			binding.RequireQualification(input.Named())
		}
		binding.Require(manifest.DependencyRef{NamedResource: origin.Named(), ReadyExpr: new("true"), Artifact: true})
		binding.Require(
			manifest.DependencyRef{NamedResource: origin.Named(), ReadyExpr: new("false"), ArtifactOnly: true},
		)
		before := maps.Collect(binding.Required())
		gates := slices.Collect(binding.Gates())
		start, observed := make(chan struct{}), make(chan bool)
		go func() {
			<-start
			observed <- binding.Valid()
		}()
		close(start)
		binding.Accept(AssessmentEntryBinding)
		assert.Equal(t, <-observed, true)
		after := maps.Collect(binding.Required())
		assert.Diff(t, after, before)
		assert.Diff(t, slices.Collect(binding.Gates()), gates)
		st.DeleteObject((&manifest.Kustomization{Name: "consumed", Namespace: "apps"}).Named())
		for range st.Sweep() {
		}
		runtime.GC()
		assert.Equal(t, declaration.Value(), nil)
		assert.Equal(t, binding.Valid(), false)
		runtime.KeepAlive(&binding)
	})

	t.Run("proposal_payload_lifetime", func(t *testing.T) {
		t.Parallel()
		st := New()
		origin := &manifest.Kustomization{Name: "origin", Namespace: "apps"}
		retained := &manifest.ConfigMap{Name: "retained", Namespace: "apps"}
		st.AddObjects([]manifest.BaseManifest{origin, retained})
		st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Content: "source"})
		var proposal, binding, output ReadSet
		var storage [2]Input
		proposal.Init(st, manifest.NamedResource{}, storage[:])
		t.Cleanup(proposal.Retire)
		binding.Init(st, origin.Named(), nil)
		t.Cleanup(binding.Close)
		output.Init(st, retained.Named(), nil)
		t.Cleanup(output.Close)
		output.GetObject(retained.Named())
		output.Accept(AssessmentReconciled)
		declaration := func() weak.Pointer[manifest.Kustomization] {
			child := &manifest.Kustomization{Name: "consumed", Namespace: "apps"}
			st.AddObject(child)
			proposal.GetDeclaration(child.Named())
			return weak.Make(child)
		}()
		for i := range 10 {
			id := manifest.NamedResource{
				Kind:      manifest.KindConfigMap,
				Namespace: "apps",
				Name:      fmt.Sprintf("absent-%d", i),
			}
			proposal.GetObject(id)
			proposal.GetStatus(id)
			proposal.RequireQualification(id)
		}
		child := Child{Obj: &manifest.ConfigMap{Name: "admitted", Namespace: "apps"}}
		proposal.Admit(Child{Obj: child.Obj}, manifest.BootstrapSourceID, manifest.NamedResource{})
		proposal.ListObjects("", manifest.KindConfigMap)
		proposal.Require(manifest.DependencyRef{NamedResource: origin.Named(), ReadyExpr: new("true")})
		binding.GetDeclaration(child.Obj.Named())
		binding.Accept(AssessmentEntryBinding)
		before := maps.Collect(proposal.Required())
		gates := slices.Collect(proposal.Gates())
		source := proposal.input(manifest.BootstrapSourceID)
		start, observed := make(chan struct{}), make(chan bool)
		go func() {
			<-start
			observed <- source.sourceCurrent() && binding.Valid() && output.Valid()
		}()
		close(start)
		proposal.ReleaseCapture()
		assert.Equal(t, <-observed, true)
		assert.Equal(t, proposal.Valid(), true)
		assert.Diff(t, maps.Collect(proposal.Required()), before)
		assert.Diff(t, slices.Collect(proposal.Gates()), gates)
		proposal.ReleaseCapture()
		proposal.Retire()
		st.DeleteObject((&manifest.Kustomization{Name: "consumed", Namespace: "apps"}).Named())
		for range st.Sweep() {
		}
		runtime.GC()
		assert.Equal(t, declaration.Value(), nil)
		assert.Equal(t, st.GetDeclaration(child.Obj.Named()), child.Obj)
		var replay ReadSet
		replay.Init(st, retained.Named(), nil)
		t.Cleanup(replay.Retire)
		assert.Equal(t, replay.Matches(&output), true)
		st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Content: "replacement"})
		assert.Equal(t, source.sourceCurrent(), false)
		assert.Equal(t, binding.Valid(), false)
		runtime.KeepAlive(&proposal)
		runtime.KeepAlive(&binding)
	})

	t.Run("captured_source_guard", func(t *testing.T) {
		t.Parallel()
		st := New()
		origin := &manifest.Kustomization{Name: "origin", Namespace: "apps"}
		credential := &manifest.Secret{Name: "credential", Namespace: "apps"}
		st.AddObjects([]manifest.BaseManifest{origin, credential})
		var acquisition, binding ReadSet
		acquisition.Init(st, manifest.BootstrapSourceID, nil)
		t.Cleanup(acquisition.Close)
		binding.Init(st, origin.Named(), nil)
		t.Cleanup(binding.Close)
		acquisition.GetObject(credential.Named())
		st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Reads: &acquisition, Content: "original"})
		binding.GetObject(origin.Named())
		binding.GetArtifact(manifest.BootstrapSourceID, ArtifactContent|ArtifactRevision|ArtifactOrigin)
		source := binding.input(manifest.BootstrapSourceID)
		binding.Accept(AssessmentEntryBinding)
		assert.Equal(t, source.sourceCurrent(), true)
		assert.Equal(t, source.details.Load().artifact, nil)
		start, replaced, observed := make(chan struct{}), make(chan struct{}), make(chan bool)
		go func() {
			<-start
			observed <- source.sourceCurrent()
			<-replaced
			observed <- source.sourceCurrent()
		}()
		close(start)
		assert.Equal(t, <-observed, true)
		st.AddObject(
			&manifest.Secret{
				Name:      credential.Name,
				Namespace: credential.Namespace,
				Data:      map[string]any{"token": "next"},
			},
		)
		close(replaced)
		assert.Equal(t, <-observed, false)
		st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Content: "replacement"})
		assert.Equal(t, binding.Valid(), false)
	})
}

func TestReadSet_ReleaseCaptureStorage(t *testing.T) {
	t.Parallel()
	for _, storage := range []string{"inline", "spare", "overflow"} {
		for _, outcome := range []string{"accepted_binding", "failed_attempt", "retained_output"} {
			t.Run(storage+"/"+outcome, func(t *testing.T) {
				t.Parallel()
				testReadSetReleaseCaptureStorage(t, storage, outcome)
			})
		}
	}
}

func testReadSetReleaseCaptureStorage(t *testing.T, storage, outcome string) {
	t.Helper()
	st := New()
	origin := &manifest.Kustomization{Name: "origin", Namespace: "apps"}
	st.AddObject(origin)
	var reads ReadSet
	var inputs [2]Input
	var supplied []Input
	if storage == "spare" {
		supplied = inputs[:]
	}
	reads.Init(st, origin.Named(), supplied)
	t.Cleanup(reads.Close)
	captured := func() weak.Pointer[manifest.ConfigMap] {
		object := &manifest.ConfigMap{
			Name: "consumed", Namespace: "apps",
			Data: map[string]any{"value": "payload"},
		}
		if storage != "inline" {
			reads.GetObject(origin.Named())
		}
		if storage == "overflow" {
			for i := range 10 {
				reads.GetStatus(manifest.NamedResource{
					Kind: manifest.KindConfigMap, Name: fmt.Sprintf("missing-%d", i),
				})
			}
		}
		st.AddObject(object)
		reads.GetObject(object.Named())
		reads.Snapshot(object.Named(), true)
		reads.ListObjects("", manifest.KindConfigMap)
		return weak.Make(object)
	}()
	var output Emission
	switch outcome {
	case "accepted_binding":
		reads.Accept(AssessmentEntryBinding)
		st.Retire(&reads)
		assert.Equal(t, reads.retired.Load(), false)
	case "failed_attempt":
		reads.Retire()
		reads.ReleaseCapture()
	case "retained_output":
		_, _, output = reads.GetEmission(origin.Named())
		reads.Bind(&output)
		output.Complete = true
		st.RecordEmitted(origin.Named(), &output, nil)
		output.Finish()
		reads.Accept(AssessmentReconciled)
		st.Retire(&reads)
		assert.Equal(t, reads.retired.Load(), false)
	}
	st.DeleteObject((&manifest.ConfigMap{Name: "consumed", Namespace: "apps"}).Named())
	for range st.Sweep() {
	}
	runtime.GC()
	assert.Equal(t, captured.Value() != nil, outcome == "retained_output")
	if outcome == "retained_output" {
		var replay ReadSet
		replay.Init(st, origin.Named(), nil)
		t.Cleanup(replay.Retire)
		assert.Equal(t, replay.Matches(&reads), false)
		reads.Close()
	}
	reads.ReleaseCapture()
	reads.ReleaseCapture()
	runtime.GC()
	assert.Equal(t, captured.Value(), nil)
	runtime.KeepAlive(&reads)
	runtime.KeepAlive(&output)
}

func TestReadSet_ReleasedSourceStamps(t *testing.T) {
	t.Parallel()
	for _, facet := range []string{"content", "revision", "origin"} {
		t.Run(facet, func(t *testing.T) {
			t.Parallel()
			st := New()
			source := manifest.BootstrapSourceID
			original := &SourceArtifact{
				Content: "content", Revision: "revision",
				Metadata: map[string]string{"org.opencontainers.image.revision": "origin"},
			}
			st.SetArtifact(source, original)
			var reads ReadSet
			reads.Init(st, ownerID("admission"), nil)
			t.Cleanup(reads.Close)
			child := &manifest.ConfigMap{Name: "admitted", Namespace: "apps"}
			reads.GetArtifact(source, ArtifactContent|ArtifactRevision|ArtifactOrigin)
			reads.Admit(Child{Obj: child}, source, manifest.NamedResource{})
			captured := reads.input(source)
			reads.ReleaseCapture()
			reads.Retire()
			for range st.Sweep() {
			}
			assert.Equal(t, captured.sourceCurrent(), true)
			assert.Equal(t, st.GetObject(child.Named()), manifest.BaseManifest(child))
			var observer ReadSet
			observer.Init(st, ownerID("observer"), nil)
			t.Cleanup(observer.Retire)
			observer.GetObject(child.Named())
			assert.Equal(t, observer.Valid(), true)
			replacement := *original
			switch facet {
			case "content":
				replacement.Content = "replacement"
			case "revision":
				replacement.Revision = "replacement"
			case "origin":
				replacement.Metadata = map[string]string{"org.opencontainers.image.revision": "replacement"}
			}
			start, observed := make(chan struct{}), make(chan bool)
			go func() {
				<-start
				observed <- captured.sourceCurrent() && observer.Valid()
			}()
			st.SetArtifact(source, &replacement)
			close(start)
			assert.Equal(t, <-observed, false)
			assert.Equal(t, captured.sourceCurrent(), false)
			assert.Equal(t, observer.Valid(), false)
			assert.Equal(t, st.GetObject(child.Named()), nil)
		})
	}
}

func TestCapture_AcquiringOwnSource(t *testing.T) {
	t.Parallel()
	s := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "file:///input"}
	root := &manifest.Kustomization{Name: "root", Namespace: "apps", Path: "."}
	s.AddObject(source)
	s.AddObject(root)
	s.SetArtifact(source.Named(), &SourceArtifact{Key: "old"})
	var admission, acquisition ReadSet
	admission.Init(s, root.Named(), nil)
	acquisition.Init(s, source.Named(), nil)
	defer admission.Close()
	defer acquisition.Close()
	admission.Admit(Child{Obj: root}, source.Named(), manifest.NamedResource{})
	assert.Equal(t, acquisition.GetDeclaration(root.Named()), manifest.BaseManifest(root))
	assert.Equal(t, acquisition.Valid(), true)
	s.SetArtifact(source.Named(), &SourceArtifact{Key: "new"})
	assert.Equal(t, acquisition.Valid(), true)
	changed := root.Clone()
	changed.Path = "changed"
	s.AddObject(changed)
	assert.Equal(t, acquisition.Valid(), false)
}

//nolint:gocognit // The capture scope MUST end before collection across all authority transitions.
func TestReadSet_SourceBindingRetainsOnlySourceCapture(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"source", "owned source", "producer", "late producer"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			st := New()
			source := manifest.BootstrapSourceID
			original := &SourceArtifact{
				Content: "content", Metadata: map[string]string{"org.opencontainers.image.revision": "first"},
			}
			st.SetArtifact(source, original)
			child := &manifest.ConfigMap{Name: "admitted", Namespace: "apps"}
			var owner manifest.NamedResource
			if change != "source" {
				owner = ownerID("producer")
				recordForTest(t, st, owner, nil)
				st.SetDiscoveryIndex(
					&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						child.Named(): {Productions: []manifest.Production{{Owner: owner, Source: source}}},
					}},
				)
			}
			captured := func() weak.Pointer[ReadSet] {
				reads := new(ReadSet)
				reads.Init(st, manifest.NamedResource{}, nil)
				for i := range 10 {
					object := &manifest.ConfigMap{Name: fmt.Sprintf("input-%d", i), Namespace: "apps"}
					st.AddObject(object)
					reads.GetObject(object.Named())
				}
				if owner != (manifest.NamedResource{}) && change != "late producer" {
					reads.Productions(child.Named(), nil)
					assert.Equal(t, reads.input(owner).cell(producerFacet).head != nil, true)
				}
				reads.Admit(Child{Obj: child}, source, owner)
				if change == "late producer" {
					reads.Productions(child.Named(), nil)
				}
				reads.GetArtifact(source, ArtifactOrigin)
				reads.Retire()
				return weak.Make(reads)
			}()
			for range st.Sweep() {
			}
			runtime.GC()
			assert.Equal(t, captured.Value() == nil, true)
			assert.Equal(t, st.GetObject(child.Named()), manifest.BaseManifest(child))
			if change == "producer" || change == "late producer" {
				recordForTest(t, st, owner, nil)
			} else {
				replacement := *original
				replacement.Metadata = map[string]string{"org.opencontainers.image.revision": "second"}
				st.SetArtifact(source, &replacement)
			}
			assert.Equal(t, st.GetObject(child.Named()), nil)
		})
	}
}

func TestReadSet_ObjectCaptureDefersOtherFacets(t *testing.T) {
	t.Parallel()
	st := New()
	var reads ReadSet
	reads.Init(st, ownerID("reader"), nil)
	defer reads.Close()
	reads.GetObject(ownerID("inline"))
	for i := range 16 {
		object := &manifest.OCIRepository{Name: fmt.Sprintf("source-%d", i), Namespace: "apps"}
		id := object.Named()
		st.AddObject(object)
		assert.Equal(t, reads.GetObject(id), manifest.BaseManifest(object))
		assert.Equal(t, reads.input(id).details.Load() == nil, true)
	}
	source := (&manifest.OCIRepository{Name: "source-8", Namespace: "apps"}).Named()
	input := &SourceArtifact{Content: "before"}
	st.SetArtifact(source, input)
	assert.Equal(t, reads.GetArtifact(source, ArtifactContent), Artifact(input))
	assert.Equal(t, reads.input(source).details.Load() != nil, true)
	assert.Equal(t, reads.Valid(), true)
	st.SetArtifact(source, &SourceArtifact{Content: "after"})
	assert.Equal(t, reads.Valid(), false)
}

func TestReadSet_ProducerCaptureDefersOtherFacets(t *testing.T) {
	t.Parallel()
	st := New()
	owner := ownerID("producer")
	child := (&manifest.ConfigMap{Name: "child", Namespace: "apps"}).Named()
	recordForTest(t, st, owner, nil)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child: {Productions: []manifest.Production{{Owner: owner}}},
	}})
	var reads ReadSet
	reads.Init(st, ownerID("reader"), nil)
	defer reads.Close()
	reads.GetObject(ownerID("inline"))
	reads.Productions(child, nil)
	assert.Equal(t, reads.input(owner).details.Load() == nil, true)
	assert.Equal(t, reads.input(child).details.Load() == nil, true)
	var repeat ReadSet
	repeat.Init(st, ownerID("repeat"), nil)
	defer repeat.Close()
	repeat.Productions(child, nil)
	assert.Equal(t, repeat.MatchesContent(&reads, child), true)
	assert.Equal(t, reads.Valid(), true)
	frozen := reads.freezeEvidence()
	assert.Equal(t, frozen.Valid(), true)
	recordForTest(t, st, owner, nil)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, frozen.Valid(), false)
}

func TestReadSet_FrozenEvidenceReleasesDiscoveryIndex(t *testing.T) {
	t.Parallel()
	st := New()
	var reads ReadSet
	reads.Init(st, ownerID("reader"), nil)
	defer reads.Close()
	child := (&manifest.ConfigMap{Name: "child", Namespace: "apps"}).Named()
	index := func() weak.Pointer[manifest.DiscoveryIndex] {
		captured := &manifest.DiscoveryIndex{}
		reads.Productions(child, captured)
		return weak.Make(captured)
	}()
	frozen := reads.freezeEvidence()
	reads.ReleaseCapture()
	runtime.GC()
	assert.Equal(t, index.Value() == nil, true)
	var replay ReadSet
	replay.Init(st, ownerID("replay"), nil)
	defer replay.Close()
	assert.Equal(t, replay.Matches(frozen), true)
	assert.Equal(t, frozen.Valid(), true)
}

func TestReadSet_RegistersOnlyConsumedFacets(t *testing.T) {
	t.Parallel()
	st := New()
	id := ownerID("source")
	st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	first := &SourceArtifact{Content: "same", Revision: "first"}
	st.SetArtifact(id, first)
	reads := new(ReadSet)
	reads.Init(st, ownerID("reader"), nil)
	defer reads.Retire()
	reads.GetArtifact(id, ArtifactContent)
	input := reads.input(id)
	for kind := conditionFacet; kind <= reconciliationFacet; kind++ {
		assert.Equal(t, input.cell(kind) != nil, kind == contentFacet)
	}
	cell := input.cell(contentFacet)
	reads.GetArtifact(id, ArtifactContent)
	assert.Equal(t, input.cell(contentFacet) == cell, true)
	st.SetArtifact(id, &SourceArtifact{Content: "same", Revision: "second"})
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, reads.GetArtifact(id, ArtifactRevision) == first, true)
	assert.Equal(t, reads.Valid(), false)
}

func TestReadSet_CapturedPublicationLifetime(t *testing.T) {
	t.Parallel()
	st := New()
	owner := ownerID("publisher")
	st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	child := emissionChild("captured", "value")
	var reads ReadSet
	reads.Init(st, ownerID("reader"), nil)
	defer reads.Close()
	warning := func() weak.Pointer[manifest.Warning] {
		warnings := []manifest.Warning{
			{Resource: child.Obj.Named(), Category: manifest.WarnSOPSUnreadable, Message: "publication"},
		}
		emitted := child
		emitted.Warnings = warnings
		next := &Emission{Children: []Child{emitted}, Complete: true}
		st.RecordEmitted(owner, next, nil)
		next.Finish()
		assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
		_, _, published := st.GetEmission(owner)
		assert.Equal(t, len(published.Children[0].Warnings), 1)
		published.Finish()
		return weak.Make(&warnings[0])
	}()
	frozen := reads.freezeEvidence()
	st.RecordEmitted(owner, &Emission{Complete: true}, nil)
	for range st.Sweep() {
	}
	runtime.GC()
	assert.Equal(t, warning.Value() != nil, true)
	assert.Equal(t, len(frozen.findInput(child.Obj.Named()).view.child.Warnings), 1)
	assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
	assert.Equal(t, frozen.findInput(child.Obj.Named()).view.child.Obj, child.Obj)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, len(st.Warnings()), 0)
	reads.Close()
	reads.ReleaseCapture()
	frozen.Retire()
	frozen.ReleaseCapture()
	runtime.GC()
	assert.Equal(t, warning.Value() == nil, true)
}
