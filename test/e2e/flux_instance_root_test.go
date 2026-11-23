package e2e_test

import (
	"bytes"
	"encoding/json"
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
func TestE2E_FluxInstanceChangedOnlyPatchAndInterval(t *testing.T) {
	for _, name := range []string{"changed-only-authored-crds", "changed-only-interval"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "flux-instance-root", name)
			got, want := fixture.Run(t), fixture.Want(t)
			if !slices.Equal(got, want) {
				t.Errorf("changed-only diff:\nwant %#v\ngot %#v", want, got)
			}
		})
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_FluxInstanceNamedBuild(t *testing.T) {
	for _, name := range []string{"named", "named-restore", "named-oci"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "flux-instance-root", name)
			want := fixture.Want(t)
			for _, cache := range []string{"cold", "reused"} {
				t.Run(cache, func(t *testing.T) {
					got := fixture.Run(t)
					if len(got) != len(want) {
						t.Fatalf("got %d results, want %d", len(got), len(want))
					}
					for i := range want {
						if got[i] != want[i] {
							t.Errorf("%s:\nwant %+v\ngot %+v", want[i].Step, want[i], got[i])
						}
					}
				})
			}
		})
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_FluxInstanceNamedBuildValuesFrom(t *testing.T) {
	fixture := harness.Open(t, "flux-instance-root", "named-oci-valuesfrom")
	got, want := fixture.Run(t), fixture.Want(t)
	if len(got) != 2 || len(want) != 2 {
		t.Fatalf("expected two CLI runs, got %d results and %d expectations", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s:\nwant %+v\ngot %+v", want[i].Step, want[i], got[i])
		}
	}
	full := patchedPlexDocument(t, got[0])
	named := patchedPlexDocument(t, got[1])
	if !bytes.Equal(full, named) {
		t.Errorf("Plex HelmRelease bytes differ:\nfull %s\nnamed %s", full, named)
	}
}

func patchedPlexDocument(t *testing.T, result harness.Result) json.RawMessage {
	t.Helper()
	var documents []json.RawMessage
	if err := json.Unmarshal([]byte(result.Stdout), &documents); err != nil {
		t.Fatalf("%s: %v", result.Step, err)
	}
	var plex json.RawMessage
	for _, document := range documents {
		var object struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Spec struct {
				Upgrade struct {
					Strategy struct {
						Name string `json:"name"`
					} `json:"strategy"`
				} `json:"upgrade"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(document, &object); err != nil {
			t.Fatalf("%s: %v", result.Step, err)
		}
		if object.Kind != "HelmRelease" || object.Metadata.Name != "plex" ||
			object.Metadata.Namespace != "default" {
			continue
		}
		if plex != nil || object.Spec.Upgrade.Strategy.Name != "RemediateOnFailure" {
			t.Errorf("%s: expected one patched default/plex HelmRelease, got %s", result.Step, document)
		}
		plex = document
	}
	if plex == nil {
		t.Errorf("%s: missing default/plex HelmRelease", result.Step)
	}
	return plex
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
func TestE2E_FluxInstanceSyncOverlap(t *testing.T) {
	for _, name := range []string{"first", "second"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "multi-cluster", "chart-sync-overlap")
			if got, want := fixture.Run(t), fixture.Want(t); !slices.Equal(got, want) {
				t.Errorf("overlapping sync output:\nwant %#v\ngot %#v", want, got)
			}
		})
	}
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
