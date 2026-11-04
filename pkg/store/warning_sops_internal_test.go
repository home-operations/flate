package store

import (
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestWarnings_SOPSUnreadableUnionAndScope(t *testing.T) {
	t.Parallel()
	s := New()
	resource := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
	second := &manifest.Kustomization{Name: "second", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{first, second})
	var a, b, rejected ReadSet
	a.Init(s, first.Named(), nil)
	b.Init(s, second.Named(), nil)
	rejected.Init(s, first.Named(), nil)
	defer a.Close()
	defer b.Close()
	defer rejected.Close()
	for i, reads := range []*ReadSet{&a, &b, &rejected} {
		reads.GetObject(reads.ID())
		keys := [][]string{{"z", "a"}, {"b", "a"}, {"rejected"}}
		reads.AddWarning(
			manifest.Warning{
				Resource: resource,
				Category: manifest.WarnSOPSUnreadable,
				Message:  "unreadable",
				Detail:   keys[i],
			},
		)
		if i < 2 {
			reads.Accept()
		}
	}
	var writers sync.WaitGroup
	for _, key := range []string{"c", "d"} {
		writers.Go(func() {
			s.AddWarning(
				manifest.Warning{
					Resource: resource,
					Category: manifest.WarnSOPSUnreadable,
					Message:  "unreadable",
					Detail:   []string{key},
				},
			)
		})
	}
	writers.Wait()
	for range 2 {
		warnings := s.Warnings()
		assert.Equal(t, len(warnings), 1)
		assert.Equal(t, warnings[0].Count, 1)
		assert.Diff(t, warnings[0].Detail, []string{"a", "b", "c", "d", "z"})
	}
}
