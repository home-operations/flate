package store

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_SeedDocumentArrival(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("publisher")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	doc := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindHelmRepository,
		"metadata": map[string]any{"name": "shared", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://charts.example"},
	}
	parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj := parsed.Object
	assert.NoError(t, err)
	s.AddObject(obj)
	foreignDoc := manifest.DeepCopyMap(doc)
	foreignDoc["apiVersion"] = "example.test/v1"
	parsed2, err := manifest.ParseDoc(foreignDoc, manifest.ParseDocOptions{})
	foreign := parsed2.Object
	assert.NoError(t, err)
	s.AddObject(foreign)
	var arrivals int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == obj.Named() {
			arrivals++
		}
	}, false)
	defer unsub()
	s.RecordEmitted(
		p,
		&Emission{
			Children:  []Child{{Obj: obj, Doc: doc}, {Obj: foreign, Doc: foreignDoc}},
			Manifests: testutil.DocumentOccurrences([]map[string]any{foreignDoc, doc}),
			Complete:  true,
		},
		nil,
	)
	assert.FatalIf(t, arrivals != 0, "equal seed acquired document with %d arrivals", arrivals)
	_, _, accepted := s.GetEmission(p)
	assert.FatalIf(
		t,
		len(accepted.Manifests) != 2 || !manifest.ValueEqual(accepted.Manifests[0].Document, foreignDoc) ||
			!manifest.ValueEqual(
				accepted.Manifests[1].Document,
				doc,
			),
		"%s",
		"suppressed arrival lost authoritative document",
	)
	{
		got := s.Productions(obj.Named(), nil)
		assert.FatalIf(t, len(got) != 1 || got[0].Owner != p, "accepted support = %v", got)
	}
	next := manifest.DeepCopyMap(doc)
	next["apiVersion"] = manifest.SourceDomain + "/v1beta2"
	s.RecordEmitted(
		p,
		&Emission{
			Children:  []Child{{Obj: obj, Doc: next}, emissionChild("other", "value")},
			Manifests: testutil.DocumentOccurrences([]map[string]any{next}),
		},
		nil,
	)
	assert.FatalIf(t, arrivals != 1, "changed authoritative document arrivals = %d, want 1", arrivals)
	_, _, accepted = s.GetEmission(p)
	assert.FatalIf(t, len(accepted.Manifests) != 2 || !manifest.ValueEqual(accepted.Manifests[0].Document, next) ||
		!manifest.ValueEqual(
			accepted.Manifests[1].Document,
			foreignDoc,
		), "partial version update lost cross-group documents: %v", accepted.Manifests)
	{
		got := s.Productions(foreign.Named(), nil)
		assert.FatalIf(t, len(got) != 1 || got[0].Owner != p, "cross-group support = %v", got)
	}

	next = manifest.DeepCopyMap(next)
	next["metadata"].(map[string]any)["annotations"] = map[string]any{"semantic": "changed"}
	next["metadata"].(map[string]any)["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": p.Name}
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: obj, Doc: next}}, Complete: true}, nil)
	assert.FatalIf(t, arrivals != 1, "unchanged source document metadata caused %d arrivals, want 1", arrivals)
	_, retained, attempt := s.GetEmission(obj.Named())
	defer attempt.Finish()
	assert.FatalIf(
		t,
		!manifest.ValueEqual(retained, next),
		"%s",
		"suppressed arrival lost updated document metadata",
	)
}

func TestRecordEmitted_CrossGroupSeedDocumentArrival(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("publisher")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	doc := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindHelmRepository,
		"metadata": map[string]any{"name": "shared", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://charts.example"},
	}
	parsed3, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj := parsed3.Object
	assert.NoError(t, err)
	s.AddObject(obj)
	foreignDoc := manifest.DeepCopyMap(doc)
	foreignDoc["apiVersion"] = "example.test/v1"
	parsed4, err := manifest.ParseDoc(foreignDoc, manifest.ParseDocOptions{})
	foreign := parsed4.Object
	assert.NoError(t, err)
	s.AddObject(foreign)
	var arrivals int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == obj.Named() {
			arrivals++
		}
	}, false)
	defer unsub()
	s.RecordEmitted(
		p,
		captureEmission(
			s,
			p,
			&Emission{
				Children:  []Child{{Obj: obj, Doc: doc}, {Obj: foreign, Doc: foreignDoc}},
				Manifests: testutil.DocumentOccurrences([]map[string]any{foreignDoc, doc}),
				Complete:  true,
			},
		),
		nil,
	)
	assert.FatalIf(t, arrivals != 0, "equal seed acquired document with %d arrivals", arrivals)
	_, _, accepted := s.GetEmission(p)
	assert.FatalIf(
		t,
		len(accepted.Manifests) != 2 || !manifest.ValueEqual(accepted.Manifests[0].Document, foreignDoc) ||
			!manifest.ValueEqual(
				accepted.Manifests[1].Document,
				doc,
			),
		"%s",
		"suppressed arrival lost authoritative document",
	)
	{
		got := s.Productions(obj.Named(), nil)
		assert.FatalIf(t, len(got) != 1 || got[0].Owner != p, "accepted support = %v", got)
	}
	next := manifest.DeepCopyMap(doc)
	next["apiVersion"] = manifest.SourceDomain + "/v1beta2"
	s.RecordEmitted(
		p,
		captureEmission(
			s,
			p,
			&Emission{
				Children:  []Child{{Obj: obj, Doc: next}, emissionChild("other", "value")},
				Manifests: testutil.DocumentOccurrences([]map[string]any{next}),
			},
		),
		nil,
	)
	assert.FatalIf(t, arrivals != 1, "changed authoritative document arrivals = %d, want 1", arrivals)
	_, _, accepted = s.GetEmission(p)
	assert.FatalIf(t, len(accepted.Manifests) != 2 || !manifest.ValueEqual(accepted.Manifests[0].Document, next) ||
		!manifest.ValueEqual(
			accepted.Manifests[1].Document,
			foreignDoc,
		), "partial version update lost cross-group documents: %v", accepted.Manifests)
	{
		got := s.Productions(foreign.Named(), nil)
		assert.FatalIf(t, len(got) != 1 || got[0].Owner != p, "cross-group support = %v", got)
	}
}
