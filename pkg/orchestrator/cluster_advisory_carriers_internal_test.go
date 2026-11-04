package orchestrator

import (
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_ClusterAdvisories(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"head", "paired", "head_only", "base", "two_clusters", "head_failed", "base_failed"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testRenderTreesClusterAdvisoriesCase(t, &scenario)
		},
		)
	}
}

func testRenderTreesClusterAdvisoriesCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	cfg := Config{
		Concurrency: 2,
		CacheDir:    t.TempDir(),
		HeadOnly:    (*testCaseInput) == "head_only",
		Supply:      []store.SuppliedObject{suppliedSubstitution("settings")},
	}
	clusters := []manifest.ClusterID{"alpha"}
	if (*testCaseInput) == "two_clusters" {
		clusters = append(clusters, "omega")
	}
	var trees []Tree
	for _, cluster := range clusters {
		for _, side := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
			if (*testCaseInput) == "head" && side == manifest.TreeBase || (*testCaseInput) ==
				"base" && side == manifest.TreeHead {
				continue
			}
			root := t.TempDir()
			writeSubstitutionCluster(t, root, "settings", fmt.Sprintf("${VALUE}-%d", side))
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, settings.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"settings.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n",
			)
			failed := (*testCaseInput) == "head_failed" && side == manifest.TreeHead ||
				(*testCaseInput) ==
					"base_failed" && side == manifest.TreeBase
			if failed {
				testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: [\n")
			}
			trees = append(trees, Tree{ID: manifest.TreeID{Cluster: cluster, Side: side}, RepoRoot: root})
		}
	}
	runs, err := RenderTrees(t.Context(), trees, cfg)
	failed := (*testCaseInput) == "head_failed" || (*testCaseInput) == "base_failed"
	{
		assert.FatalIf(
			t,
			(err != nil) != failed || failed && !errors.Is(err, manifest.ErrFlux),
			"render failure=%v, want failed=%t",
			err,
			failed,
		)
	}
	assertRenderClusterAdvisoryCarriers(t, runs, testCaseInput, len(clusters))
}

func assertRenderClusterAdvisoryCarriers(t *testing.T, runs []Rendered, testCaseInput *string, wantCarriers int) {
	t.Helper()
	carriers := 0
	for _, run := range runs {
		if run.Result == nil {
			continue
		}
		{
			warningValues := run.Result.Warnings
			for warningIndex := range warningValues {
				warning := warningValues[warningIndex]

				{
					assert.FatalIf(
						t,
						warning.Category == manifest.WarnSuppliedPrecedence,
						"%v",
						"cluster advisory escaped through side warnings",
					)
				}
			}
		}
		if len(run.Result.ClusterWarnings) == 0 {
			continue
		}
		carriers++
		wantSide := manifest.TreeHead
		if (*testCaseInput) == "base" {
			wantSide = manifest.TreeBase
		}
		assert.Equal(t, run.ID.Side, wantSide)
		assert.Diff(t, run.Result.ClusterWarnings, []manifest.Warning{
			{
				Category: manifest.WarnSuppliedPrecedence,
				Message:  "repository data takes precedence over supplied data",
				Detail:   []string{"ConfigMap/apps/settings: VALUE (ci-values.yaml)"},
				Count:    1,
			},
		})
	}
	assert.Equal(t, carriers, wantCarriers)
}
