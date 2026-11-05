package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_SourceCoordinates(t *testing.T) {
	t.Parallel()
	for _, origin := range []string{"/workspace/selected", "/workspace", "/mounted"} {
		t.Run(origin, func(t *testing.T) {
			t.Parallel()
			root := "/workspace/selected"
			if origin == "/mounted" {
				root = "/mounted/selected"
			}
			file := "input.txt"
			if origin != root {
				file = "selected/input.txt"
			}
			var sources [2]*store.SourceArtifact
			for side := range sources {
				sources[side] = &store.SourceArtifact{
					LocalPath: "/mounted", LocalRoot: origin, InputAlias: true,
					FS: fstest.MapFS{file: {Data: fmt.Appendf(nil, "%d", side)}},
				}
			}
			changes, err := change.Detect(t.Context(), sources[0], sources[1], manifest.WorkScope{})
			assert.NoError(t, err)
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, sources[1])
			l := New(st)
			l.SourceRoot, l.Options.Changes = root, changes
			c := admissionClosure{
				loader:      l,
				objects:     st,
				comparisons: &admissionSources{bindings: map[manifest.NamedResource]*admissionSource{}},
			}
			coordinates2 := c.coordinates(
				new(manifest.Production{
					Source: manifest.BootstrapSourceID,
					File:   file,
					Path:   filepath.Join(sources[1].LocalPath, file),
				}),
			)
			relative, indexed := coordinates2.relative, coordinates2.indexed
			assert.Equal(t, relative, file)
			assert.Equal(t, indexed, "input.txt")
			assert.Equal(t, c.fileChanged(manifest.BootstrapSourceID, "input.txt"), true)
			assert.Diff(t, slices.Collect(c.sourceFiles(manifest.BootstrapSourceID)), []string{"input.txt"})
		})
	}
}

func TestAdmission_InputVisibility(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"krmignore", "sourceignore", "spec_ignore"} {
		for _, scenario := range []string{"comment_only", "hidden"} {
			t.Run(policy+"/"+scenario, func(t *testing.T) {
				t.Parallel()
				testAdmissionInputVisibilityCase(
					t,
					&testAdmissionInputVisibilityCaseInputs{policy: &policy, scenario: &scenario},
				)
			},
			)
		}
	}
}

func TestAdmission_DuplicateInputContent(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"retained", "changed_content", "last_hidden"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testAdmissionDuplicateInputContentCase(t, &scenario)
		},
		)
	}
}

type testAdmissionInputVisibilityCaseInputs struct {
	policy   *string
	scenario *string
}

func testAdmissionInputVisibilityCase(t *testing.T, testCaseInput *testAdmissionInputVisibilityCaseInputs) {
	t.Helper()

	hide := (*testCaseInput.scenario) == "hidden"
	loaders, stores := admissionVisibilityPair(t, *testCaseInput.policy, hide)
	testAdmissionInputVisibilityCaseCompletion(t, &hide, &loaders, &stores)
}

func testAdmissionDuplicateInputContentCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	loaders, stores := duplicateAdmissionInputPair(t, *testCaseInput)
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	consumer := testutil.KustomizationID("apps", "consumer")
	assert.Diff(t, loaders[0].Index.Visibility[0].Inputs[input], []string{"a/settings.yaml", "b/settings.yaml"})
	for _, reverse := range []bool{false, true} {
		before, after := 0, 1
		if reverse {
			before, after = after, before
		}
		changes, err := change.Qualify(
			t.Context(),
			nil,
			[2]*store.Store{stores[before], stores[after]},
			manifest.WorkScope{},
		)
		assert.NoError(t, err)
		changes, err = changes.WithReaders(
			t.Context(), [2]*manifest.DiscoveryIndex{
				loaders[before].Index,
				loaders[after].Index,
			},
			[2]map[manifest.NamedResource]inputprojection.Projection{
				loaders[before].InputContent,
				loaders[after].InputContent,
			}, manifest.WorkScope{},
		)
		assert.NoError(t, err)
		if (*testCaseInput) == "changed_content" {
			assert.Equal(t, changes.Contains("b/settings.yaml"), true)
		}
		for _, l := range loaders {
			l.Options.Changes = changes
			proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Selected, consumer), (*testCaseInput) != "retained")
		}
	}
}

func testAdmissionInputVisibilityCaseCompletion(
	t *testing.T,
	hide *bool,
	loaders *[2]*Loader,
	stores *[2]*store.Store,
) {
	t.Helper()
	for _, reverse := range []bool{false, true} {
		before, after := 0, 1
		if reverse {
			before, after = after, before
		}
		changes, err := change.Qualify(
			t.Context(),
			nil,
			[2]*store.Store{stores[before], stores[after]},
			manifest.WorkScope{},
		)
		assert.NoError(t, err)
		changes, err = changes.WithReaders(
			t.Context(), [2]*manifest.DiscoveryIndex{
				loaders[before].Index,
				loaders[after].Index,
			},
			[2]map[manifest.NamedResource]inputprojection.Projection{
				loaders[before].InputContent,
				loaders[after].InputContent,
			}, manifest.WorkScope{},
		)
		assert.NoError(t, err)
		for _, l := range *loaders {
			l.Options.Changes = changes
			proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			assert.Equal(
				t,
				slices.Contains(proposal.Selected, testutil.KustomizationID("apps", "consumer")),
				(*hide),
			)
			assert.Equal(
				t,
				slices.Contains(proposal.Needed, testutil.HelmReleaseID("apps", "opaque")),
				false,
			)
			for _, name := range []string{"stable", "ordering", "ready"} {
				if slices.Contains(proposal.Needed, testutil.KustomizationID("apps", name)) {
					t.Errorf(
						"unrelated %s admitted: needed=%v selected=%v",
						name,
						proposal.Needed,
						proposal.Selected,
					)
				}
			}
			assert.Equal(
				t,
				slices.Contains(
					proposal.Needed,
					manifest.NamedResource{
						Group:     manifest.SourceDomain,
						Kind:      manifest.KindGitRepository,
						Namespace: "apps",
						Name:      "unused",
					},
				),
				false,
			)
		}
	}
}
