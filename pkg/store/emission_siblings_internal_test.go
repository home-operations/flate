package store

import (
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_DirectWritesPreserveSiblings(t *testing.T) {
	t.Parallel()

	for _, batch := range []bool{false, true} {
		t.Run(strconv.FormatBool(batch), func(t *testing.T) {
			t.Parallel()
			testRecordEmittedDirectWritesPreserveSiblingsCase(
				t,
				&batch,
			)
		},
		)
	}
	for _, state := range []string{"member", "retired", "hidden"} {
		t.Run("equal_static/"+state, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedDirectWritesPreserveSiblingsCase2(
				t,
				&state,
			)
		},
		)
	}
}

func testRecordEmittedDirectWritesPreserveSiblingsCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	s := New()
	p, k := ownerID("p"), ownerID("k")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				k: {Productions: []manifest.Production{{Owner: p}}},
			},
		},
	)
	child := Child{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}
	accept(t, s, p, &Emission{Children: []Child{child}, Complete: true})
	grandchild := emissionChild("grandchild", "old")
	accept(t, s, k, &Emission{Children: []Child{grandchild}, Complete: true})
	accept(t, s, p, &Emission{Complete: true})
	replacement := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace, Path: "new"}
	sibling := emissionChild("sibling", "value")
	observed := false
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id == k {
			observed = true
			{
				assert.ErrorIf(
					t,
					payload == nil || s.GetObject(k) != replacement || s.GetObject(grandchild.Obj.Named()) == nil,
					"%s",
					"direct reanchor did not activate retained outgoing support",
				)
			}
			{
				assert.ErrorIf(
					t,
					(*testCaseInput) && s.GetObject(sibling.Obj.Named()) == nil,
					"%s",
					"batch notified before its sibling was live",
				)
			}
		}
	}, false)
	if *testCaseInput {
		s.AddObjects([]manifest.BaseManifest{replacement, sibling.Obj})
	} else {
		s.AddObject(replacement)
	}
	unsub()
	{
		assert.FatalIf(t, !observed, "%s", "direct update did not notify")
	}
	{
		assert.FatalIf(t, s.GetObject(k) != replacement, "%s", "direct input update lost to predecessor contribution")
	}
	conflict := replacement.Clone()
	conflict.Path = "conflicting"
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: conflict}}, Complete: true}, nil)
	{
		assert.FatalIf(t, s.GetObject(k) != replacement, "%s", "direct input lost its independent authority")
	}
	s.RecordEmitted(p, &Emission{Complete: true}, nil)

	q := ownerID("q")
	s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
	accept(t, s, q, &Emission{Children: []Child{grandchild}, Complete: true})
	{
		assert.FatalIf(
			t,
			!s.DeleteObject(k) || s.GetObject(k) != nil || s.GetObject(grandchild.Obj.Named()) == nil,
			"%s",
			"direct support removal lost independent support or retained its root",
		)
	}
	accept(t, s, q, &Emission{Complete: true})
	{
		assert.FatalIf(
			t,
			s.GetObject(grandchild.Obj.Named()) != nil || s.GetArtifact(k, ArtifactContent) != nil,
			"%s",
			"deleted root retained outgoing live support",
		)
	}
}

func testRecordEmittedDirectWritesPreserveSiblingsCase2(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	p, k := ownerID("p"), ownerID("k")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	seed := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}
	s.AddObject(seed)
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				k: {Productions: []manifest.Production{{Owner: p}}},
			},
		},
	)
	contribution := seed.Clone()
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: contribution}}, Complete: true}, nil)
	if (*testCaseInput) == "hidden" {
		child := emissionChild("grandchild", "retained")
		accept(t, s, k, &Emission{Children: []Child{child}, Complete: true})
	}
	if (*testCaseInput) != "member" {
		accept(t, s, p, &Emission{Complete: true})
		assert.Equal(t, s.GetObject(k), nil)
	}
	s.AddObject(seed)
	if (*testCaseInput) == "hidden" {
		assert.Equal(t, s.GetObject(emissionChild("grandchild", "retained").Obj.Named()) != nil, true)
	}
	s.RecordEmitted(p, &Emission{Complete: true}, nil)
	{
		assert.FatalIf(
			t,
			s.GetObject(k) != seed,
			"%s",
			"equal direct write did not retain independent static authority",
		)
	}
	var events int
	defer s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == k {
			events++
		}
	}, false)()
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: contribution}}, Complete: true}, nil)
	s.AddObject(seed)
	{
		assert.FatalIf(
			t,
			s.GetObject(k) != seed || events != 0,
			"%s",
			"equal static write changed the rooted representative",
		)
	}
}
