package depwait

import (
	"fmt"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"strings"
	"testing"
)

type crdPromotion struct {
	store       *store.Store
	materialize bool
	calls       []manifest.NamedResource
}

func (e *crdPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }
func (e *crdPromotion) Promote(id manifest.NamedResource) bool {
	e.calls = append(e.calls, id)
	if !e.materialize {
		return false
	}
	e.store.AddObject(&manifest.RawObject{Kind: id.Kind, Namespace: id.Namespace, Name: id.Name})
	return true
}

func TestClassify_MissingCRDs(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, drain := range []int{drainNone, drainCascade, drainForce} {
			for _, state := range []string{"absent", "explicit namespace", "object", "ready status", "pending status", "failed status", "promoted", "promotion miss", "ConfigMap", "Secret", "Widget", "Kustomization", "HelmRelease", "CustomResourceDefinitionList", "invalid expression", "present invalid expression"} {
				t.Run(fmt.Sprintf("enabled_%t/drain_%d/%s", enabled, drain, state), func(t *testing.T) {
					s := store.New()
					ref := manifest.DependencyRef{Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"}
					want := ClassBlocked
					msg := ""
					if drain >= drainCascade {
						want = ClassFailed
						msg = "dependency not found"
						if enabled {
							want = ClassReady
							msg = ""
						}
					}
					var e *crdPromotion
					switch state {
					case "explicit namespace":
						ref.Namespace = "explicit"
					case "object", "present invalid expression":
						s.AddObject(&manifest.RawObject{Kind: ref.Kind, Name: ref.Name})
						want = ClassReady
						msg = ""
						if state == "present invalid expression" {
							ref.ReadyExpr = "dep.(("
						}
					case "ready status", "pending status", "failed status":
						status := store.StatusReady
						if state == "pending status" {
							status = store.StatusPending
						}
						if state == "failed status" {
							status = store.StatusFailed
						}
						s.UpdateStatus(ref.NamedResource, status, "state")
						want = ClassReady
						msg = ""
					case "promoted", "promotion miss":
						e = &crdPromotion{store: s, materialize: state == "promoted"}
						if e.materialize {
							want = ClassReady
							msg = ""
						}
					case "ConfigMap", "Secret", "Widget", "Kustomization", "HelmRelease", "CustomResourceDefinitionList":
						ref.Kind = state
						ref.Namespace = "apps"
						want = ClassBlocked
						msg = ""
						if drain >= drainCascade {
							want = ClassFailed
							msg = "dependency not found"
						}
					case "invalid expression":
						ref.ReadyExpr = "dep.(("
						want = ClassFailed
					}
					w := &Waiter{Store: s, AllowMissingCRDs: enabled}
					if e != nil {
						w.Existence = e
					}
					got := w.Classify(ref, drain)
					assert.Equal(t, got.Kind, want)
					if state == "invalid expression" {
						if !strings.HasPrefix(got.Message, "readyExpr:") {
							t.Fatalf("invalid expression must fail: %+v", got)
						}
					} else {
						assert.Equal(t, got.Message, msg)
					}
					if e != nil {
						assert.Diff(t, e.calls, []manifest.NamedResource{ref.NamedResource})
						assert.Equal(t, s.GetObject(ref.NamedResource) != nil, e.materialize)
					}
					if state == "absent" || state == "explicit namespace" {
						assert.Equal(t, s.GetObject(ref.NamedResource) == nil, true)
						_, ok := s.GetStatus(ref.NamedResource)
						assert.Equal(t, ok, false)
						assert.Equal(t, s.GetArtifact(ref.NamedResource) == nil, true)
					}
				})
			}
		}
	}
}

func TestClassify_MissingCRDs_NoAllocations(t *testing.T) {
	s := store.New()
	ref := manifest.DependencyRef{Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"}
	for _, enabled := range []bool{false, true} {
		w := &Waiter{Store: s, AllowMissingCRDs: enabled}
		if got := testing.AllocsPerRun(100, func() { w.Classify(ref, drainCascade) }); got != 0 {
			t.Fatalf("enabled=%t allocations=%v", enabled, got)
		}
	}
}
