package e2e_test

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_ImplicitEntryComponentReach(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reached bool
	}{
		{name: "implicit-imported", reached: true},
		{name: "implicit-unread"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := harness.Open(t, "external-artifact-reach", tc.name)
			work := fixture.RunWithWork(t)[0].ReconcileWork(t)
			maps.DeleteFunc(
				work,
				func(key [3]string, _ [2]int) bool { return key[2] != "kustomize" && key[2] != "helm" },
			)
			want := make(map[[3]string][2]int)
			if tc.reached {
				want[[3]string{"0", "/", "kustomize"}] = [2]int{1, 0}
				want[[3]string{"1", "/", "kustomize"}] = [2]int{1, 0}
			}
			if !maps.Equal(work, want) {
				t.Errorf("work got %v, want %v", work, want)
			}
			for range 2 {
				if got, want := fixture.Run(t, "--namespace", "default"), fixture.Want(t); !slices.Equal(got, want) {
					t.Fatalf("namespace default: want %#v, got %#v", want, got)
				}
			}
		})
	}
}

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_BoundEntryComponentReach(t *testing.T) {
	for _, name := range []string{"bound-entry-imported", "bound-entry-unread"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "external-artifact-reach", name)
			want := fixture.Want(t)
			result := fixture.RunWithWork(t)[0]
			if name == "bound-entry-unread" {
				work := result.ReconcileWork(t)
				maps.DeleteFunc(
					work,
					func(key [3]string, _ [2]int) bool { return key[2] != "kustomize" && key[2] != "helm" },
				)
				if len(work) != 0 {
					t.Errorf("work got %v, want no Kustomize or Helm attempts", work)
				}
				attempts := strings.Count(result.Stderr, `msg="kustomize: build"`) +
					strings.Count(result.Stderr, `msg="helm: render"`)
				if attempts != 0 {
					t.Errorf("unread sibling edit triggered %d builds or renders", attempts)
				}
			}
			for range 2 {
				if got := fixture.Run(t, "--namespace", "flux-system"); !slices.Equal(got, want) {
					t.Fatalf("namespace flux-system: want %#v, got %#v", want, got)
				}
			}
		})
	}
}
