package store

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_InclusionArrival(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"absent", "hidden", "present", "excluded"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.Kustomization{Name: "child", Namespace: "apps"}
			id := obj.Named()
			if state != "absent" {
				s.AddObject(obj)
			}
			if state == "hidden" {
				sh := s.shardFor(id)
				cell := sh.cells[id]
				cell.hidden = true
				sh.cells[id] = cell
			}
			if state == "excluded" {
				s.SetIncluded(id, false)
			}
			var calls atomic.Int32
			s.AddListener(EventObjectAdded, func(got manifest.NamedResource, payload any) {
				assert.Equal(t, got, id)
				assert.Equal(t, payload.(manifest.BaseManifest), manifest.BaseManifest(obj))
				assert.Equal(t, s.GetObject(id), manifest.BaseManifest(obj))
				calls.Add(1)
			}, false)
			start := make(chan struct{})
			var workers sync.WaitGroup
			for range 2 {
				workers.Go(func() {
					<-start
					s.SetIncluded(id, true)
				})
			}
			close(start)
			workers.Wait()
			s.SetIncluded(id, false)
			var reads ReadSet
			reads.Init(s, id, nil)
			defer reads.Retire()
			assert.Equal(t, reads.Admitted(), true)
			want := int32(1)
			if state == "absent" || state == "hidden" {
				want = 0
			}
			assert.Equal(t, calls.Load(), want)
			if state == "absent" {
				s.AddObject(obj)
				assert.Equal(t, calls.Load(), int32(1))
			}
		})
	}
}

func TestStore_InclusionCellSize(t *testing.T) {
	t.Parallel()
	type originalCell struct {
		Static                                         manifest.BaseManifest
		Document                                       map[string]any
		Publication                                    uint64
		Binding, Support                               *Input
		Excluded, Rooted, Retired, Hidden, Independent bool
		Preflight                                      string
		Authority                                      authority
	}
	assert.Equal(t, reflect.TypeFor[objectCell]().Size(), reflect.TypeFor[originalCell]().Size())
}
