package e2e_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_FluxInstanceRootDiff(t *testing.T) {
	fixture := harness.Open(t, "flux-instance-root", "patch-chain")
	want := fixture.Want(t)
	first, second := fixture.Run(t), fixture.Run(t)
	if !slices.Equal(first, want) {
		t.Errorf("first sequence:\nwant %#v\ngot %#v", want, first)
	}
	if !slices.Equal(second, want) {
		t.Errorf("second sequence:\nwant %#v\ngot %#v", want, second)
	}
	if !slices.Equal(first, second) {
		t.Errorf("sequences differ:\nfirst %#v\nsecond %#v", first, second)
	}

	t.Run("descendant", func(t *testing.T) {
		fixture := harness.Open(t, "flux-instance-root", "descendant")
		if got, want := fixture.Run(t), fixture.Want(t); !slices.Equal(got, want) {
			t.Errorf("descendant:\nwant %#v\ngot %#v", want, got)
		}
	})
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_FluxInstanceAuthoredParent(t *testing.T) {
	fixture := harness.Open(t, "flux-instance-authored-parent", "authored-parent")
	if got, want := fixture.Run(t), fixture.Want(t); !slices.Equal(got, want) {
		t.Errorf("authored parent:\nwant %#v\ngot %#v", want, got)
	}

	t.Run("sync-overlap", func(t *testing.T) {
		fixture := harness.Open(t, "flux-instance-authored-parent", "sync-overlap")
		want := fixture.Want(t)
		for _, name := range []string{"first", "second"} {
			t.Run(name, func(t *testing.T) {
				if got := fixture.Run(t); !slices.Equal(got, want) {
					t.Errorf("overlapping sync output:\nwant %#v\ngot %#v", want, got)
				}
			})
		}
	})
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_FluxInstanceAggregateValues(t *testing.T) {
	fixture := harness.Open(t, "flux-instance-aggregate-values", "generator-values")
	if got, want := fixture.Run(t), fixture.Want(t); !slices.Equal(got, want) {
		t.Errorf("aggregate generator values: want %#v, got %#v", want, got)
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_FluxInstanceSyncRootWorkloads(t *testing.T) {
	for _, tc := range []struct{ scenario, name string }{
		{"flux-instance-ancestor", "ancestor"},
		{"flux-instance-ancestor", "separate"},
		{"flux-instance-scoped-workloads", "workloads"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := harness.Open(t, tc.scenario, tc.name)
			if got, want := fixture.Run(t), fixture.Want(t); !slices.Equal(got, want) {
				t.Errorf("sync-root output:\nwant %#v\ngot %#v", want, got)
			}
		})
	}
}
