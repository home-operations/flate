package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The orchestrator fixture changes process-wide source policy.
func TestRender_FluxInstancePatchChain(t *testing.T) {
	for _, placement := range []string{"authored", "emitted"} {
		t.Run(placement, func(t *testing.T) {
			o, result := renderPlexSyncFixture(t, placement == "authored", true)
			categories := make([]string, 0, len(result.Warnings))
			for _, warning := range result.Warnings {
				categories = append(categories, warning.Category)
			}
			if placement == "authored" {
				assert.Diff(t, categories, []string{manifest.WarnBootstrapAliases})
			} else {
				assert.Equal(t, len(categories), 0)
			}
			var patchAttempts uint64
			for _, record := range result.Work {
				if record.Key.Phase == manifest.WorkPreparation && record.Key.Input == "patches" {
					patchAttempts += record.Counts.Attempts
				}
			}
			assert.Equal(t, patchAttempts, uint64(1))
			id := testutil.HelmReleaseID("media", "plex")
			assert.Equal(t, workCounts(result.Work, id, manifest.WorkHelm).Attempts, uint64(1))
			plex, ok := o.store.GetDeclaration(id).(*manifest.HelmRelease)
			assert.FatalIf(t, !ok || plex.Install == nil || plex.Rollback == nil || plex.Upgrade == nil ||
				plex.Upgrade.Strategy == nil || plex.Upgrade.Remediation == nil,
				"the effective Plex declaration lacks the patched fields")
			assert.Equal(t, string(plex.Install.CRDs), "CreateReplace")
			assert.Equal(t, plex.Rollback.CleanupOnFail, true)
			assert.Equal(t, plex.Upgrade.CleanupOnFail, true)
			assert.Equal(t, string(plex.Upgrade.CRDs), "CreateReplace")
			assert.Equal(t, plex.Upgrade.Strategy.Name, "RemediateOnFailure")
			assert.Equal(t, plex.Upgrade.Remediation.Retries, 2)
			assert.Equal(t, plex.Upgrade.Remediation.RemediateLastFailure != nil &&
				*plex.Upgrade.Remediation.RemediateLastFailure, true)
		})
	}
}

//nolint:paralleltest // The orchestrator fixture changes process-wide source policy.
func TestRender_GeneratedSyncDocumentsVisible(t *testing.T) {
	for _, mode := range []string{"patched", "unpatched"} {
		t.Run(mode, func(t *testing.T) {
			_, result := renderPlexSyncFixture(t, false, mode == "patched")
			visible := map[string]int{}
			for _, batch := range result.Manifests {
				for _, doc := range batch.Documents {
					kind := manifest.DocKind(doc)
					if manifest.DocMetadata(doc).Name == "sync" || kind == manifest.KindHelmChart {
						visible[kind]++
					}
				}
			}
			assert.Diff(t, visible, map[string]int{manifest.KindGitRepository: 1, manifest.KindKustomization: 1})
		})
	}
}

func renderPlexSyncFixture(t *testing.T, authored, patched bool) (*Orchestrator, *Result) {
	t.Helper()
	root := t.TempDir()
	assert.NoError(t, os.CopyFS(root, os.DirFS("../../../testdata/flux-instance-root/repo")))
	const template = "charts/bootstrap/templates/instance.yaml"
	if authored || !patched {
		document, err := os.ReadFile(filepath.Join(root, template))
		assert.NoError(t, err)
		spec := string(document)
		if !patched {
			spec, _, _ = strings.Cut(spec, "  kustomize:\n")
		}
		if authored {
			testutil.WriteFile(t, root, "kubernetes/apps/instance.yaml", spec)
			testutil.WriteFile(
				t,
				root,
				"kubernetes/apps/kustomization.yaml",
				"resources: [source.yaml, instance.yaml, plex.yaml]\n",
			)
		} else {
			testutil.WriteFile(t, root, template, spec)
		}
	}
	repo := testutil.GitInit(t, root)
	hash := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{
			Author: &object.Signature{Name: "fixture", Email: "fixture@example.invalid", When: time.Unix(0, 0)},
		},
	)
	ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), hash)
	assert.NoError(t, repo.Storer.SetReference(ref))
	o, err := newTestOrchestrator(t, Tree{
		Path: filepath.Join(
			root,
			"kubernetes",
			"apps",
		),
		RepoRoot: root,
		SelfURLs: []string{"https://example.invalid/fixture"},
	}, Config{CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true})
	assert.NoError(t, err)
	result, err := o.render(t.Context(), t)
	assert.NoError(t, err)
	return o, result
}
