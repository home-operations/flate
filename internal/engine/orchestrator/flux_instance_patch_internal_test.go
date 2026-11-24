package orchestrator

import (
	"cmp"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/engine/selection"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/testfixture"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_FluxInstancePatchChain(t *testing.T) {
	t.Parallel()
	for _, placement := range []string{"authored", "emitted"} {
		t.Run(placement, func(t *testing.T) {
			t.Parallel()
			o, result := renderPlexSyncFixture(t, plexSyncFixture{authored: placement == "authored", patched: true})
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

func TestRender_GeneratedSyncDocumentsVisible(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"patched", "unpatched"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			_, result := renderPlexSyncFixture(t, plexSyncFixture{patched: mode == "patched"})
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

func TestRender_FluxInstanceNamedPatchChain(t *testing.T) {
	t.Parallel()
	plex := testutil.HelmReleaseID("media", "plex")
	for _, id := range []manifest.NamedResource{{}, testutil.KustomizationID("media", "plex"), plex} {
		t.Run(cmp.Or(id.Kind, "full"), func(t *testing.T) {
			t.Parallel()
			request := selection.Request{}
			if id.Name != "" {
				request.Roots = []manifest.NamedResource{id}
			}
			o, result := renderPlexSyncFixture(t, plexSyncFixture{patched: true, request: request, overlay: "named"})
			assertPlexSyncWinner(t, o)
			if id.Name != "" {
				assert.Diff(t, plexApplicationAttempts(result.Work), map[manifest.NamedResource]uint64{
					testutil.KustomizationID("flux-system", "bootstrap"):   1,
					testutil.KustomizationID("flux-system", "sync"):        1,
					testutil.HelmReleaseID("flux-system", "flux-instance"): 1,
					testutil.KustomizationID("media", "plex"):              1,
					plex: 1,
				})
			}
		})
	}
}

func TestRender_FluxInstanceNamedAcrossNamespaces(t *testing.T) {
	t.Parallel()
	for _, overlay := range []string{"namespaces", "disjoint", "overlap", "shared"} {
		t.Run(overlay, func(t *testing.T) {
			t.Parallel()
			assertPlexNamespaceSelection(t, overlay)
		})
	}
}

func assertPlexNamespaceSelection(t *testing.T, overlay string) {
	t.Helper()
	id := testutil.KustomizationID("", "plex")
	full, fullResult := renderPlexSyncFixture(t, plexSyncFixture{patched: true, overlay: overlay})
	named, namedResult := renderPlexSyncFixture(t, plexSyncFixture{
		patched: true, overlay: overlay, request: selection.Request{Roots: []manifest.NamedResource{id}},
	})
	sync := testutil.KustomizationID("flux-system", "sync")
	otherSync := testutil.KustomizationID("flux-system", "other-sync")
	assert.Equal(t, named.entryBinding.Entry != nil, overlay == "shared")
	if overlay == "disjoint" {
		for i := range namedResult.Warnings {
			warning := &namedResult.Warnings[i]
			assert.FatalIf(
				t, warning.Category == manifest.WarnConflictingProducers, "unexpected producer conflict: %v", warning,
			)
		}
	}
	for _, namespace := range []string{"media", "other"} {
		t.Run(namespace, func(t *testing.T) {
			id := testutil.KustomizationID(namespace, "plex")
			fullParent, fullFound := full.store.Parent(id, full.index)
			namedParent, namedFound := named.store.Parent(id, named.index)
			if namespace == "media" {
				assert.Equal(t, fullFound, true)
				assert.Equal(t, fullParent, sync)
			} else {
				switch overlay {
				case "disjoint", "overlap":
					assert.Equal(t, fullFound, true)
					assert.Equal(t, fullParent, otherSync)
				case "shared":
					assert.Equal(t, fullFound, true)
					assert.Equal(t, fullParent, sync)
				}
			}
			assert.Equal(t, namedFound, fullFound)
			assert.Equal(t, namedParent, fullParent)
			want := outputByParent(fullResult.Manifests)[id]
			assert.FatalIf(t, len(want) == 0, "the full render did not include the selected object")
			assert.Diff(t, outputByParent(namedResult.Manifests)[id], want)
		})
	}
	if overlay == "disjoint" || overlay == "overlap" {
		assertPlexNamespaceRender(t, &Rendered{Orchestrator: full, Result: fullResult},
			&Rendered{Orchestrator: named, Result: namedResult}, overlay)
	}
}

func assertPlexNamespaceRender(t *testing.T, full, named *Rendered, overlay string) {
	t.Helper()
	interval := 7 * time.Minute
	if overlay == "overlap" {
		interval = time.Hour
	}
	sync := testutil.KustomizationID("flux-system", "sync")
	otherSync := testutil.KustomizationID("flux-system", "other-sync")
	other := testutil.KustomizationID("other", "plex")
	plex := testutil.HelmReleaseID("media", "plex")
	for _, run := range []*Orchestrator{full.Orchestrator, named.Orchestrator} {
		ks, ok := run.store.GetDeclaration(other).(*manifest.Kustomization)
		assert.FatalIf(t, !ok, "the render did not include the other Plex declaration")
		assert.Equal(t, ks.Interval.Duration, interval)
		release, ok := run.store.GetDeclaration(plex).(*manifest.HelmRelease)
		assert.FatalIf(t, !ok || release.Upgrade == nil || release.Upgrade.Strategy == nil,
			"the effective Plex declaration lacks its upgrade strategy")
		assert.Equal(t, release.Upgrade.Strategy.Name, "RemediateOnFailure")
	}
	assert.Diff(t, outputByParent(named.Result.Manifests)[plex], outputByParent(full.Result.Manifests)[plex])
	assert.Diff(t, plexApplicationAttempts(named.Result.Work), map[manifest.NamedResource]uint64{
		testutil.KustomizationID("flux-system", "bootstrap"):   1,
		testutil.HelmReleaseID("flux-system", "flux-instance"): 1,
		sync: 1, otherSync: 1, other: 1, testutil.KustomizationID("media", "plex"): 1, plex: 1,
	})
}

func TestRender_FluxInstanceNamedQualificationStopsAtWinner(t *testing.T) {
	t.Parallel()
	plex := testutil.HelmReleaseID("media", "plex")
	o, result := renderPlexSyncFixture(t, plexSyncFixture{
		patched: true, request: selection.Request{Roots: []manifest.NamedResource{plex}}, overlay: "conditional",
	})
	assert.Diff(t, plexApplicationAttempts(result.Work), map[manifest.NamedResource]uint64{
		testutil.KustomizationID("flux-system", "bootstrap"):   1,
		testutil.KustomizationID("flux-system", "sync"):        1,
		testutil.HelmReleaseID("flux-system", "b-conditional"): 1,
		testutil.HelmReleaseID("flux-system", "flux-instance"): 1,
		testutil.KustomizationID("media", "plex"):              1,
		plex: 1,
	})
	assert.Equal(t, o.entryBinding.SupportOwner, testutil.HelmReleaseID("flux-system", "flux-instance"))
	assertPlexSyncWinner(t, o)
}

func TestRender_FluxInstanceNamedDiffMatchesFull(t *testing.T) {
	t.Parallel()
	plex := testutil.HelmReleaseID("media", "plex")
	var pair [2]Tree
	var full [2][]map[string]any
	for side := range pair {
		fixture := testfixture.Materialize(t, "flux-instance-root", "named")
		if side == int(manifest.TreeHead) {
			changed := slices.IndexFunc(fixture.Plan.Steps, func(step testfixture.Step) bool {
				return step.ID == "changed"
			})
			fixture.Edit(t, fixture.Plan.Steps[changed])
		}
		tree := Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			RepoRoot: fixture.Repo,
			Path:     fixture.Repo,
			Input:    testDirectoryInput(t, fixture.Repo),
			SelfURLs: []string{"https://example.invalid/fixture"},
		}
		runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{Concurrency: 2})
		assert.NoError(t, err)
		full[side] = outputByParent(runs[0].Result.Manifests)[plex]
		pair[side] = tree
	}
	pair[0].BaseInput, pair[1].BaseInput = pair[1].Input, pair[0].Input
	runs, err := renderTreesForTest(t.Context(), t, pair[:], Config{
		Concurrency: 2, RecordWork: true,
		Selection: selection.Request{Roots: []manifest.NamedResource{plex}, Changed: true},
	})
	assert.NoError(t, err)
	for side := range runs {
		run := &runs[side]
		assertPlexSyncWinner(t, run.Orchestrator)
		assert.Diff(t, full[side], outputByParent(run.Result.Manifests)[plex])
		attempts := plexApplicationAttempts(run.Result.Work)
		assert.Diff(t, attempts, map[manifest.NamedResource]uint64{
			testutil.KustomizationID("flux-system", "bootstrap"):   1,
			testutil.KustomizationID("flux-system", "sync"):        1,
			testutil.KustomizationID("media", "plex"):              1,
			testutil.HelmReleaseID("flux-system", "flux-instance"): 1,
			plex: 1,
		})
	}
}

func TestRender_FluxInstanceNamedDisjointRootWithdrawal(t *testing.T) {
	t.Parallel()
	var pair [2]Tree
	var full [2]Rendered
	for side := range pair {
		fixture := testfixture.Materialize(t, "flux-instance-root", "named")
		root := fixture.Repo
		encloseDisjointPlexPackages(t, root)
		addSecondPlexRoot(t, root, "other-flux", "7m")
		if side == int(manifest.TreeHead) {
			assert.NoError(t, os.Remove(filepath.Join(root, "charts", "bootstrap", "templates", "other.yaml")))
			const template = "charts/bootstrap/templates/instance.yaml"
			body, err := os.ReadFile(filepath.Join(root, template))
			assert.NoError(t, err)
			patched := strings.Replace(string(body), "{mode: patched}", "{mode: revised}", 1)
			testutil.WriteFile(t, root, template, patched)
		}
		pair[side] = Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			RepoRoot: root, Path: root, Input: testDirectoryInput(t, root),
			SelfURLs: []string{"https://example.invalid/fixture"},
		}
		runs, err := renderTreesForTest(t.Context(), t, pair[side:side+1], Config{Concurrency: 2})
		assert.NoError(t, err)
		full[side] = runs[0]
	}

	pair[0].BaseInput, pair[1].BaseInput = pair[1].Input, pair[0].Input
	runs, err := renderTreesForTest(t.Context(), t, pair[:], Config{
		Concurrency: 2,
		Selection: selection.Request{
			Roots:   []manifest.NamedResource{testutil.KustomizationID("", "plex")},
			Changed: true,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, len(runs), len(pair))
	for side := range runs {
		assertDisjointPlexComparisonSide(t, &runs[side], &full[side], manifest.TreeSide(side))
	}
}

func assertDisjointPlexComparisonSide(t *testing.T, run, full *Rendered, side manifest.TreeSide) {
	t.Helper()
	assert.Equal(t, len(run.Orchestrator.index.ProducerKeys), 0)
	media := testutil.KustomizationID("media", "plex")
	other := testutil.KustomizationID("other", "plex")
	sync := testutil.KustomizationID("flux-system", "sync")
	otherSync := testutil.KustomizationID("flux-system", "other-sync")
	mode, interval := "patched", 7*time.Minute
	if side == manifest.TreeHead {
		otherSync = manifest.NamedResource{}
		mode, interval = "revised", time.Hour
	}
	otherPlex, ok := run.Orchestrator.store.GetDeclaration(other).(*manifest.Kustomization)
	assert.FatalIf(t, !ok, "comparison omitted other/plex")
	assert.Equal(t, otherPlex.Interval.Duration, interval)
	release, ok := run.Orchestrator.store.GetDeclaration(testutil.HelmReleaseID("media", "plex")).(*manifest.HelmRelease)
	assert.FatalIf(t, !ok, "comparison omitted the media/plex HelmRelease")
	assert.Equal(t, release.Values["mode"], any(mode))
	for _, id := range []manifest.NamedResource{media, other} {
		want := sync
		if id == other {
			want = otherSync
		}
		parent, found := run.Orchestrator.store.Parent(id, run.Orchestrator.index)
		fullParent, fullFound := full.Orchestrator.store.Parent(id, full.Orchestrator.index)
		assert.Equal(t, parent, want)
		assert.Equal(t, found, want != (manifest.NamedResource{}))
		assert.Equal(t, parent, fullParent)
		assert.Equal(t, found, fullFound)
		documents := outputByParent(full.Result.Manifests)[id]
		assert.FatalIf(t, len(documents) == 0, "full render omitted output for %v", id)
		assert.Diff(t, outputByParent(run.Result.Manifests)[id], documents)
	}
}

func TestRender_FluxInstanceNamedWithdrawalMatchesFull(t *testing.T) {
	t.Parallel()
	plex := testutil.HelmReleaseID("media", "plex")
	trees, full := plexWithdrawalTrees(t)
	for _, order := range []struct {
		name  string
		sides [2]int
	}{{"withdraw", [2]int{0, 1}}, {"restore", [2]int{1, 0}}} {
		t.Run(order.name, func(t *testing.T) {
			t.Parallel()
			for _, cache := range []string{"cold", "receipt"} {
				t.Run(cache, func(t *testing.T) {
					t.Parallel()
					cfg := Config{
						CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true,
						Selection: selection.Request{Roots: []manifest.NamedResource{plex}},
					}
					if cache == "receipt" {
						primePlexEntryReceipt(t, &trees[0], &cfg)
					}
					cfg.Selection.Changed = true
					pair := [2]Tree{trees[order.sides[0]], trees[order.sides[1]]}
					for i := range pair {
						pair[i].ID.Side = manifest.TreeSide(i)
						pair[i].BaseInput = pair[1-i].Input
					}
					runs, err := renderTreesForTest(t.Context(), t, pair[:], cfg)
					assert.NoError(t, err)
					for i := range runs {
						side := order.sides[i]
						assertPlexWithdrawalSide(t, &runs[i], &full[side], plexWithdrawal{
							primed: cache == "receipt", withdrawn: side == 1,
						})
					}
				})
			}
		})
	}
}

func TestRender_FluxInstanceOCINamedDiffMatchesFull(t *testing.T) {
	t.Parallel()
	registryURL := fluxInstanceChartRegistry(t)
	plex := testutil.HelmReleaseID("default", "plex")
	var trees [4]Tree
	var full [4][]map[string]any
	for i, mode := range []string{"covered", "removed", "disjoint", "resolved-disjoint"} {
		tree := ociPlexTree(t, registryURL, mode)
		if i != 0 {
			origin := trees[0].Input
			tree.Input.LocalPath, tree.Input.LocalRoot, tree.Input.URL = origin.LocalPath, origin.LocalRoot, origin.URL
			tree.Path, tree.RepoRoot = origin.LocalPath, origin.LocalRoot
		}
		trees[i] = tree
		runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{Concurrency: 2})
		assert.NoError(t, err)
		full[i] = outputByParent(runs[0].Result.Manifests)[plex]
		assert.FatalIf(t, len(full[i]) == 0, "%s full build omitted Plex", mode)
	}
	assert.ErrorIf(t, manifest.ValueEqual(full[0], full[1]), "removal did not change the full projection")
	assert.Diff(t, full[2], full[1])
	assert.Diff(t, full[3], full[1])

	for _, tc := range []struct {
		name  string
		sides [2]int
	}{
		{"withdraw", [2]int{0, 1}},
		{"restore", [2]int{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pair := [2]Tree{trees[tc.sides[0]], trees[tc.sides[1]]}
			for i := range pair {
				pair[i].ID.Side = manifest.TreeSide(i)
				pair[i].BaseInput = pair[1-i].Input
			}
			runs, err := renderTreesForTest(t.Context(), t, pair[:], Config{
				Concurrency: 2, RecordWork: true,
				Selection: selection.Request{Roots: []manifest.NamedResource{plex}, Changed: true},
			})
			assert.NoError(t, err)
			assert.Equal(t, len(runs), len(pair))
			for i := range runs {
				run := &runs[i]
				side := tc.sides[i]
				assert.Diff(t, outputByParent(run.Result.Manifests)[plex], full[side])
				assertOCIPlexAttempts(t, run.Result, side == 0, false)
			}
		})
	}
	for i, name := range []string{"disjoint", "resolved-disjoint"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			side := i + 2
			runs, err := renderTreesForTest(t.Context(), t, trees[side:side+1], Config{
				Concurrency: 2, RecordWork: true,
				Selection: selection.Request{Roots: []manifest.NamedResource{plex}},
			})
			assert.NoError(t, err)
			assert.Diff(t, outputByParent(runs[0].Result.Manifests)[plex], full[side])
			assertOCIPlexAttempts(t, runs[0].Result, false, true)
		})
	}
	t.Run("changed interval under unpatched root", func(t *testing.T) {
		t.Parallel()
		assertOCIUnpatchedInterval(t, registryURL)
	})
}

func assertOCIUnpatchedInterval(t *testing.T, registryURL string) {
	t.Helper()
	base := ociPlexTree(t, registryURL, "unpatched")
	head := ociPlexTree(t, registryURL, "unpatched-interval")
	head.Input.LocalPath, head.Input.LocalRoot, head.Input.URL = base.Input.LocalPath, base.Input.LocalRoot, base.Input.URL
	head.Path, head.RepoRoot = base.Path, base.RepoRoot
	base.ID.Side, head.ID.Side = manifest.TreeBase, manifest.TreeHead
	base.BaseInput, head.BaseInput = head.Input, base.Input
	root, app := testutil.KustomizationID("flux-system", "flux-system"), testutil.KustomizationID("default", "plex")
	for _, mode := range []string{"all", "named"} {
		t.Run(mode, func(t *testing.T) {
			request := selection.Request{Changed: true}
			if mode == "named" {
				request.Roots = []manifest.NamedResource{app}
			}
			runs, err := renderTreesForTest(t.Context(), t, []Tree{base, head}, Config{
				Concurrency: 2, RecordWork: true, Selection: request,
			})
			assert.NoError(t, err)
			for i, interval := range []string{"1h", "2h"} {
				var got []any
				for _, doc := range outputByParent(runs[i].Result.Manifests)[root] {
					if manifest.DocIdentity(doc) == app {
						spec, _ := doc["spec"].(map[string]any)
						got = append(got, spec["interval"])
					}
				}
				assert.Diff(t, got, []any{interval})
				if mode == "named" {
					attempts := plexApplicationAttempts(runs[i].Result.Work)
					assert.Equal(t, attempts[testutil.KustomizationID("default", "sibling")], uint64(0))
				}
			}
		})
	}
}

func ociPlexTree(t *testing.T, registryURL, mode string) Tree {
	t.Helper()
	fixture := testfixture.Materialize(t, "flux-instance-root", "named-oci")
	root := fixture.Repo
	const app = "kubernetes/apps/flux-system/flux-instance/app/"
	source, err := os.ReadFile(filepath.Join(root, app+"source.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, root, app+"source.yaml", strings.NewReplacer(
		"oci://registry.invalid/flux-instance", registryURL+"\n  insecure: true",
		"tag: 1.0.0", "tag: 0.1.0",
	).Replace(string(source)))
	switch mode {
	case "removed":
		assert.NoError(t, os.Remove(filepath.Join(root, app+"release.yaml")))
		testutil.WriteFile(t, root, app+"kustomization.yaml", "resources: [source.yaml]\n")
	case "unpatched", "unpatched-interval":
		release, err := os.ReadFile(filepath.Join(root, app+"release.yaml"))
		assert.NoError(t, err)
		spec, _, found := strings.Cut(string(release), "      kustomize:\n")
		assert.Equal(t, found, true)
		testutil.WriteFile(t, root, app+"release.yaml", spec)
		if mode == "unpatched-interval" {
			for _, app := range []string{"plex", "sibling"} {
				file := "kubernetes/apps/default/" + app + "/ks.yaml"
				body, err := os.ReadFile(filepath.Join(root, file))
				assert.NoError(t, err)
				testutil.WriteFile(t, root, file, strings.Replace(string(body), "interval: 1h", "interval: 2h", 1))
			}
		}
	case "disjoint", "resolved-disjoint":
		const disjoint = "./kubernetes/apps/default/sibling"
		release, err := os.ReadFile(filepath.Join(root, app+"release.yaml"))
		assert.NoError(t, err)
		if mode == "disjoint" {
			release = []byte(strings.Replace(string(release), "path: ./kubernetes/apps}", "path: "+disjoint+"}", 1))
		} else {
			testutil.WriteFile(
				t,
				root,
				app+"kustomization.yaml",
				"resources: [source.yaml, release.yaml, values.yaml]\n",
			)
			testutil.WriteFile(t, root, app+"values.yaml", "apiVersion: v1\nkind: ConfigMap\n"+
				"metadata: {name: sync-path, namespace: flux-system}\ndata: {path: "+disjoint+"}\n")
			release = []byte(strings.Replace(
				string(release),
				"  values:\n",
				"  valuesFrom: [{kind: ConfigMap, name: sync-path, valuesKey: path, targetPath: instance.sync.path}]\n  values:\n",
				1,
			))
		}
		testutil.WriteFile(t, root, app+"release.yaml", string(release))
		testutil.WriteFile(t, root, "kubernetes/apps/default/sibling/kustomization.yaml", "resources: [ks.yaml]\n")
	}
	return Tree{
		ID: manifest.TreeID{Cluster: "."}, RepoRoot: root, Path: root, Input: testDirectoryInput(t, root),
		SelfURLs: []string{"https://example.invalid/fixture"},
	}
}

func plexWithdrawalTrees(t *testing.T) ([2]Tree, [2]Rendered) {
	t.Helper()
	var trees [2]Tree
	var full [2]Rendered
	for side, edit := range []string{"", "control"} {
		fixture := testfixture.Materialize(t, "flux-instance-root", "named")
		if edit != "" {
			step := slices.IndexFunc(fixture.Plan.Steps, func(step testfixture.Step) bool { return step.ID == edit })
			fixture.Edit(t, fixture.Plan.Steps[step])
		}
		input := testDirectoryInput(t, fixture.Repo)
		if side != 0 {
			// Both snapshots use one checkout identity so the withdrawal can open the stored receipt.
			before := trees[0].Input
			input.LocalPath, input.LocalRoot, input.URL = before.LocalPath, before.LocalRoot, before.URL
		}
		trees[side] = Tree{
			ID: manifest.TreeID{Cluster: "."}, RepoRoot: input.LocalRoot, Path: input.LocalPath,
			Input: input, SelfURLs: []string{"https://example.invalid/fixture"},
		}
		runs, err := renderTreesForTest(t.Context(), t, trees[side:side+1], Config{Concurrency: 2})
		assert.NoError(t, err)
		full[side] = runs[0]
	}
	return trees, full
}

func primePlexEntryReceipt(t *testing.T, tree *Tree, cfg *Config) {
	t.Helper()
	runs, err := renderTreesForTest(t.Context(), t, []Tree{*tree}, *cfg)
	assert.NoError(t, err)
	assertPlexSyncWinner(t, runs[0].Orchestrator)
	assert.FatalIf(t, runs[0].Orchestrator.entryBinding.Entry == nil, "the winning entry supplied no receipt")
}

type plexWithdrawal struct{ primed, withdrawn bool }

func assertPlexWithdrawalSide(t *testing.T, run, full *Rendered, scenario plexWithdrawal) {
	t.Helper()
	app, plex := testutil.KustomizationID("media", "plex"), testutil.HelmReleaseID("media", "plex")
	assert.Diff(t, outputByParent(run.Result.Manifests)[plex], outputByParent(full.Result.Manifests)[plex])
	for _, id := range []manifest.NamedResource{app, plex} {
		owner, found := run.Orchestrator.store.Parent(id, run.Orchestrator.index)
		wantOwner, wantFound := full.Orchestrator.store.Parent(id, full.Orchestrator.index)
		assert.Equal(t, found, wantFound)
		assert.Equal(t, owner, wantOwner)
	}
	attempts := make(map[manifest.NamedResource]uint64)
	if !scenario.primed || scenario.withdrawn {
		attempts[app], attempts[plex] = 1, 1
	}
	if !scenario.primed && !scenario.withdrawn {
		attempts[testutil.KustomizationID("flux-system", "bootstrap")] = 1
		attempts[testutil.KustomizationID("flux-system", "sync")] = 1
		attempts[testutil.HelmReleaseID("flux-system", "flux-instance")] = 1
	}
	assert.Diff(t, plexApplicationAttempts(run.Result.Work), attempts)

	var receipt, expected manifest.WorkCounts
	for i := range run.Result.Work {
		record := &run.Result.Work[i]
		if record.Key.Phase == manifest.WorkValidation && record.Key.Input == "entry-receipt" {
			receipt.Add(&record.Counts)
		}
	}
	if scenario.primed && sourcecache.RenderPersistenceDisabledReason() == "" {
		expected.Attempts = 1
		if scenario.withdrawn {
			expected.Failures = 1
		} else {
			expected.Completed = 1
		}
	}
	assert.Diff(t, receipt, expected)
}

func assertPlexSyncWinner(t *testing.T, o *Orchestrator) {
	t.Helper()
	assert.Equal(t, o.entryBinding.Producer, testutil.KustomizationID("flux-system", "sync"))
	release, ok := o.store.GetDeclaration(testutil.HelmReleaseID("media", "plex")).(*manifest.HelmRelease)
	assert.FatalIf(t, !ok || release.Upgrade == nil || release.Upgrade.Strategy == nil,
		"the effective Plex declaration lacks its upgrade strategy")
	assert.Equal(t, release.Upgrade.Strategy.Name, "RemediateOnFailure")
}

func plexApplicationAttempts(work manifest.WorkSummary) map[manifest.NamedResource]uint64 {
	attempts := make(map[manifest.NamedResource]uint64)
	for i := range work {
		record := &work[i]
		if record.Counts.Attempts != 0 &&
			(record.Key.Phase == manifest.WorkHelm || record.Key.Phase == manifest.WorkKustomize) {
			attempts[record.Key.Owner] += record.Counts.Attempts
		}
	}
	return attempts
}

type plexSyncFixture struct {
	authored, patched bool
	request           selection.Request
	overlay           string
}

func renderPlexSyncFixture(t *testing.T, fixture plexSyncFixture) (*Orchestrator, *Result) {
	t.Helper()
	root := t.TempDir()
	assert.NoError(t, os.CopyFS(root, os.DirFS("../../../testdata/flux-instance-root/repo")))
	if fixture.overlay != "" {
		files := os.DirFS("../../../testdata/flux-instance-root/overlays/named/repo")
		assert.NoError(t, fs.WalkDir(files, ".", func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			file, readErr := fs.ReadFile(files, path)
			if readErr != nil {
				return readErr
			}
			testutil.WriteFile(t, root, path, string(file))
			return nil
		}))
	}
	const template = "charts/bootstrap/templates/instance.yaml"
	if fixture.overlay == "conditional" {
		addPlexQualificationCandidates(t, root, template)
	}
	switch fixture.overlay {
	case "namespaces":
		encloseDisjointPlexPackages(t, root)
		testutil.WriteFile(t, root, "kubernetes/apps/independent/kustomization.yaml", "resources: [plex.yaml]\n")
		testutil.WriteFile(t, root, "kubernetes/apps/independent/plex.yaml", testutil.KustomizationYAML(
			"plex", "other",
			"  path: ./sibling\n  sourceRef: {kind: GitRepository, name: bootstrap, namespace: flux-system}\n",
		))
	case "disjoint":
		encloseDisjointPlexPackages(t, root)
		addSecondPlexRoot(t, root, "other-flux", "7m")
	case "overlap":
		addSecondPlexRoot(t, root, "a-other-flux", "1h")
		testutil.WriteFile(t, root, "kubernetes/apps/kustomization.yaml",
			"resources: [source.yaml, bootstrap/bootstrap.yaml, plex.yaml, sibling.yaml, independent]\n")
	case "shared":
		const packagePath = "kubernetes/apps/"
		writeOtherPlex(t, root, packagePath+"other-plex.yaml")
		resources := "resources: [source.yaml, bootstrap/bootstrap.yaml, plex.yaml, sibling.yaml, other-plex.yaml]\n"
		testutil.WriteFile(t, root, packagePath+"kustomization.yaml", resources)
	}
	if fixture.authored || !fixture.patched {
		document, err := os.ReadFile(filepath.Join(root, template))
		assert.NoError(t, err)
		spec := string(document)
		if !fixture.patched {
			spec, _, _ = strings.Cut(spec, "  kustomize:\n")
		}
		if fixture.authored {
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
	path := filepath.Join(root, "kubernetes", "apps")
	if fixture.overlay != "" {
		path = root
	}
	o, err := newTestOrchestrator(t, Tree{
		Path:     path,
		RepoRoot: root,
		SelfURLs: []string{"https://example.invalid/fixture"},
	}, Config{CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true, Selection: fixture.request})
	assert.NoError(t, err)
	result, err := o.render(t.Context(), t)
	assert.NoError(t, err)
	return o, result
}

func encloseDisjointPlexPackages(t *testing.T, root string) {
	t.Helper()
	const apps = "kubernetes/apps/"
	testutil.WriteFile(t, root, apps+"primary/kustomization.yaml", "resources: [plex.yaml]\n")
	assert.NoError(t, os.Rename(filepath.Join(root, apps+"plex.yaml"), filepath.Join(root, apps+"primary/plex.yaml")))
	testutil.WriteFile(t, root, apps+"kustomization.yaml",
		"resources: [source.yaml, bootstrap/bootstrap.yaml, primary, sibling.yaml, independent]\n")
	const template = "charts/bootstrap/templates/instance.yaml"
	body, err := os.ReadFile(filepath.Join(root, template))
	assert.NoError(t, err)
	testutil.WriteFile(t, root, template, strings.Replace(string(body),
		"path: ./kubernetes/apps}", "path: ./kubernetes/apps/primary}", 1))
}

func addSecondPlexRoot(t *testing.T, root, instance, interval string) {
	t.Helper()
	const packagePath = "kubernetes/apps/independent"
	testutil.WriteFile(t, root, packagePath+"/kustomization.yaml", "resources: [plex.yaml]\n")
	writeOtherPlex(t, root, packagePath+"/plex.yaml")
	testutil.WriteFile(t, root, "charts/bootstrap/templates/other.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: `+instance+`, namespace: flux-system}
spec:
  distribution: {version: 2.6.x}
  sync: {name: other-sync, kind: GitRepository, url: https://example.invalid/fixture, ref: refs/heads/main, path: ./`+packagePath+`}
  kustomize:
    patches:
      - target: {kind: Kustomization, name: other-sync, namespace: flux-system}
        patch: |
          - op: add
            path: /spec/patches
            value:
              - target: {kind: Kustomization, name: plex, namespace: other}
                patch: |
                  - op: replace
                    path: /spec/interval
                    value: `+interval+`
`)
}

func writeOtherPlex(t *testing.T, root, path string) {
	t.Helper()
	testutil.WriteFile(t, root, path, testutil.KustomizationYAML(
		"plex", "other",
		"  interval: 1h\n  path: ./second\n"+
			"  sourceRef: {kind: GitRepository, name: bootstrap, namespace: flux-system}\n",
	))
	testutil.WriteFile(t, root, "second/kustomization.yaml", "resources: [config.yaml]\n")
	testutil.WriteFile(t, root, "second/config.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: second}\n")
}

func addPlexQualificationCandidates(t *testing.T, root, template string) {
	t.Helper()
	document, err := os.ReadFile(filepath.Join(root, template))
	assert.NoError(t, err)
	factory := "kubernetes/apps/bootstrap/factory"
	testutil.WriteFile(t, root, factory+"/kustomization.yaml",
		"resources: [release.yaml, a-ineligible.yaml, a-path.yaml, b-conditional.yaml, z-later.yaml]\n")
	recipe, _, _ := strings.Cut(string(document), "  kustomize:\n")
	const annotation = `  kustomize:
    patches:
      - target: {kind: Kustomization}
        patch: |
          - op: add
            path: /metadata/annotations
            value: {review: annotation}
`
	for _, candidate := range []struct{ name, body string }{
		{"a-ineligible", strings.ReplaceAll(recipe, "example.invalid/fixture", "unrelated.invalid/fixture") + annotation},
		{"a-path", strings.ReplaceAll(recipe, "./kubernetes/apps", "./sibling") + annotation},
		{"b-conditional", "{{ if .Values.emit }}\n---\n" + recipe + "---\n{{ end }}\n"},
		{"z-later", recipe},
	} {
		testutil.WriteFile(
			t,
			root,
			"charts/"+candidate.name+"/Chart.yaml",
			"apiVersion: v2\nname: "+candidate.name+"\nversion: 1.0.0\n",
		)
		body := strings.NewReplacer("name: flux,", "name: "+candidate.name+",", "name: sync,", "name: "+candidate.name+",").
			Replace(candidate.body)
		testutil.WriteFile(t, root, "charts/"+candidate.name+"/templates/instance.yaml", body)
		testutil.WriteFile(t, root, factory+"/"+candidate.name+".yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: `+candidate.name+`, namespace: flux-system}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/`+candidate.name+`
      sourceRef: {kind: GitRepository, name: bootstrap}
`)
	}
}
