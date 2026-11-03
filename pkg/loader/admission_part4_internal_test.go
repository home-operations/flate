package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
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
			changes, err := change.Detect(t.Context(), sources[0], sources[1])
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
			relative, indexed := c.coordinates(
				manifest.Production{
					Source: manifest.BootstrapSourceID,
					File:   file,
					Path:   filepath.Join(sources[1].LocalPath, file),
				},
			)
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
	var loaders [2]*Loader
	var stores [2]*store.Store
	for side := range loaders {
		root := t.TempDir()
		st := store.New()
		stores[side] = st
		ignore := "# unchanged visibility\n"
		if side == 1 {
			ignore = "# new comment\n"
			if hide {
				ignore += "settings.yaml\n"
				if (*testCaseInput.policy) == "krmignore" {
					ignore += "unused.yaml\nready.yaml\n"
				}
			}
		}
		files := fstest.MapFS{
			".krmignore": {Data: []byte(ignore)},
			"opaque.yaml": {
				Data: []byte(
					"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: opaque, namespace: apps}\nspec: {chartRef: {kind: OCIRepository, name: absent}}\n",
				),
			},
			"consumer.yaml": {
				Data: []byte(
					testutil.KustomizationYAML(
						"consumer",
						"apps",
						"  path: ./app\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
					),
				),
			},
			"stable.yaml": {
				Data: []byte(testutil.KustomizationYAML("stable", "apps", "  path: ./stable\n")),
			},
			"ordering.yaml": {
				Data: []byte(
					testutil.KustomizationYAML(
						"ordering",
						"apps",
						"  path: ./ordering\n  dependsOn: [{name: ready}]\n",
					),
				),
			},
			"ready.yaml": {
				Data: []byte(testutil.KustomizationYAML("ready", "apps", "  path: ./ready\n")),
			},
			"settings.yaml": {
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: unchanged}\n",
				),
			},
			"unused.yaml": {
				Data: []byte(
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: unused, namespace: apps}\nspec: {url: https://fixture.invalid/unused}\n",
				),
			},
		}
		for _, directory := range []string{"app", "stable", "ordering", "ready"} {
			files[directory+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: []\n")}
		}
		input, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: files},
		)
		assert.NoError(t, err)
		if (*testCaseInput.policy) != "krmignore" {
			delete(files, ".krmignore")
			view := gittree.ViewOptions{Mode: gittree.Source, Current: files}
			if (*testCaseInput.policy) == "sourceignore" {
				files[".sourceignore"] = &fstest.MapFile{Data: []byte(ignore)}
			} else {
				view.Ignore = ignore
			}
			input, _, err = gittree.New(t.Context(), nil, plumbing.ZeroHash, view)
			assert.NoError(t, err)
		}
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
		l := New(st)
		loaders[side] = l
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.Filter = change.NewFilter()
		_, err = l.Load(t.Context(), root)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	}
	testAdmissionInputVisibilityCaseCompletion(t, &hide, &loaders, &stores)
}

func testAdmissionDuplicateInputContentCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	var loaders [2]*Loader
	var stores [2]*store.Store
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	consumer := testutil.KustomizationID("apps", "consumer")
	for side := range loaders {
		root := t.TempDir()
		data := []byte(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: old}\n",
		)
		files := fstest.MapFS{
			"flux/consumer.yaml": {
				Data: []byte(
					testutil.KustomizationYAML(
						"consumer",
						"apps",
						"  path: ./app\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
					),
				),
			},
			"app/kustomization.yaml": {Data: []byte("resources: []\n")},
			"a/settings.yaml":        {Data: data},
			"b/settings.yaml":        {Data: data},
			".sourceignore":          {},
		}
		if side == 1 {
			ignore := "a/settings.yaml\n"
			if (*testCaseInput) == "last_hidden" {
				ignore += "b/settings.yaml\n"
			}
			files[".sourceignore"].Data = []byte(ignore)
			if (*testCaseInput) == "changed_content" {
				files["b/settings.yaml"].Data = []byte(strings.ReplaceAll(string(data), "old", "new"))
			}
		}
		view, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Source, Current: files},
		)
		assert.NoError(t, err)
		st := store.New()
		stores[side] = st
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
		l := New(st)
		loaders[side] = l
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.Filter = change.NewFilter()
		_, err = l.Load(t.Context(), root)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	}
	assert.Diff(t, loaders[0].Index.Visibility[0].Inputs[input], []string{"a/settings.yaml", "b/settings.yaml"})
	for _, reverse := range []bool{false, true} {
		before, after := 0, 1
		if reverse {
			before, after = after, before
		}
		changes, err := change.Qualify(t.Context(), nil, stores[before], stores[after])
		assert.NoError(t, err)
		changes, err = changes.WithReaders(
			t.Context(),
			loaders[before].Index,
			loaders[after].Index,
			[2]map[manifest.NamedResource]inputprojection.Projection{
				loaders[before].InputContent,
				loaders[after].InputContent,
			},
		)
		assert.NoError(t, err)
		if (*testCaseInput) == "changed_content" {
			assert.Equal(t, changes.Contains("b/settings.yaml"), true)
		}
		for _, l := range loaders {
			l.Options.Changes = changes
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Selected, consumer), (*testCaseInput) != "retained")
		}
	}
}

func testAdmissionInputVisibilityCaseCompletion(t *testing.T, hide *bool, loaders *[2]*Loader, stores *[2]*store.Store) {
	t.Helper()
	for _, reverse := range []bool{false, true} {
		before, after := 0, 1
		if reverse {
			before, after = after, before
		}
		changes, err := change.Qualify(t.Context(), nil, (*stores)[before], (*stores)[after])
		assert.NoError(t, err)
		changes, err = changes.WithReaders(
			t.Context(),
			(*loaders)[before].Index,
			(*loaders)[after].Index,
			[2]map[manifest.NamedResource]inputprojection.Projection{
				(*loaders)[before].InputContent,
				(*loaders)[after].InputContent,
			},
		)
		assert.NoError(t, err)
		for _, l := range *loaders {
			l.Options.Changes = changes
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
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
