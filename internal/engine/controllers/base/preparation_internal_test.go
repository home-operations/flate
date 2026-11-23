package base

import (
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestExecution_PreparationTransfer(t *testing.T) {
	t.Parallel()
	t.Run("precedence", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name, destination, previous, discovery, want string
			consumed                                     bool
		}{
			{name: "destination", destination: "destination", previous: "previous", discovery: "discovery", want: "destination"},
			{name: "predecessor", destination: "mismatch", previous: "previous", discovery: "discovery", want: "previous"},
			{name: "discovery", destination: "mismatch", previous: "mismatch", discovery: "discovery", want: "discovery", consumed: true},
			{name: "mismatches", destination: "mismatch", previous: "mismatch", discovery: "mismatch"},
			{name: "empty"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				st := store.New()
				var run Execution
				run.InitProbe(New(st, nil), &manifest.Kustomization{Name: "owner", Namespace: "apps"})
				defer run.Close()
				destination := Preparation[string]{value: tc.destination}
				previous := Preparation[string]{value: tc.previous}
				discovery := tc.discovery
				run.preparation = &previous
				got := run.Prepare(&destination, &discovery, func(value *string) bool {
					return *value != "" && *value != "mismatch"
				})
				assert.Equal(t, got, &destination.value)
				assert.Equal(t, *got, tc.want)
				assert.Equal(t, previous.value, "")
				assert.Equal(t, run.preparation == &destination, true)
				wantDiscovery := tc.discovery
				if tc.consumed {
					wantDiscovery = ""
				}
				assert.Equal(t, discovery, wantDiscovery)
				run.Retire()
				assert.Equal(t, destination.value, "")
				assert.Equal(t, run.preparation == nil, true)
			})
		}
	})
	t.Run("aliases", func(t *testing.T) {
		t.Parallel()
		for _, match := range []bool{false, true} {
			t.Run(map[bool]string{false: "mismatch", true: "match"}[match], func(t *testing.T) {
				t.Parallel()
				destination := Preparation[string]{value: "retained"}
				run := Execution{preparation: &destination}
				got := run.Prepare(&destination, &destination.value, func(*string) bool { return match })
				want := ""
				if match {
					want = "retained"
				}
				assert.Equal(t, *got, want)
				assert.Equal(t, run.preparation == &destination, true)
			})
		}
	})
	t.Run("wrong holder type", func(t *testing.T) {
		t.Parallel()
		previous := Preparation[int]{value: 1}
		run := Execution{preparation: &previous}
		var destination Preparation[string]
		discovery := "discovery"
		got := run.Prepare(&destination, &discovery, func(value *string) bool { return *value != "" })
		assert.Equal(t, *got, "discovery")
		assert.Equal(t, discovery, "")
		assert.Equal(t, previous.value, 0)
	})
}

func TestExecution_PreparationTransferDistinctOwners(t *testing.T) {
	t.Parallel()
	var ready, finished sync.WaitGroup
	ready.Add(2)
	finished.Add(2)
	start := make(chan struct{})
	for _, name := range []string{"first", "second"} {
		go func() {
			defer finished.Done()
			st := store.New()
			controller := New(st, nil)
			owner := &manifest.Kustomization{Name: name, Namespace: "apps"}
			var previous, next Execution
			previous.InitProbe(controller, owner)
			defer previous.Close()
			next.InitProbe(controller, owner)
			defer next.Close()
			holder := Preparation[string]{value: name}
			previous.preparation = &holder
			ready.Done()
			<-start
			next.Take(schedule.Result{Assessment: &previous})
			var destination Preparation[string]
			got := next.Prepare(&destination, nil, func(value *string) bool { return *value == name })
			assert.Equal(t, *got, name)
			assert.Equal(t, holder.value, "")
		}()
	}
	ready.Wait()
	close(start)
	finished.Wait()
}

func TestExecution_PreparationTransferGuards(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"same", "resource", "declaration", "pending declaration"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			controller := New(st, nil)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "before"}
			var previous, next Execution
			previous.InitProbe(controller, owner)
			defer previous.Close()
			holder := Preparation[string]{value: "retained"}
			previous.preparation = &holder
			current := owner.Clone()
			if change == "resource" {
				current.Name = "other"
			}
			if change == "declaration" {
				current.Path = "after"
			}
			if change == "pending declaration" {
				previous.RetainPreparation = true
				var pending Execution
				pending.Init(controller, current.Named())
				defer pending.Close()
				pending.Take(schedule.Result{Assessment: &previous})
				assert.Equal(t, pending.declaration == nil, true)
				assert.Equal(t, previous.preparation == nil, true)
				next.InitProbe(controller, current)
				next.Take(schedule.Result{Assessment: &pending})
				assert.Equal(t, pending.preparation == nil, true)
			} else {
				next.InitProbe(controller, current)
				next.Take(schedule.Result{Assessment: &previous})
			}
			defer next.Close()
			var destination Preparation[string]
			got := next.Prepare(&destination, nil, func(value *string) bool { return *value != "" })
			retained := change == "same" || change == "pending declaration"
			assert.Equal(t, *got == "retained", retained)
			assert.Equal(t, holder.value == "", retained)
		})
	}
}
