package orchestrator

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitlib "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type fluxInstanceFixture struct {
	root string
	urls []string
}

func writeFluxInstanceEntry(t *testing.T, mode string) fluxInstanceFixture {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	assert.NoError(t,
		repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD,
			plumbing.NewBranchReferenceName("main"))))
	const url = "https://example.test/self"
	declaration := `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: control}
spec:
  distribution: {version: 2.x}
  sync: {kind: GitRepository, url: https://example.test/self, ref: refs/heads/main, path: ./entry}
  kustomize:
    patches:
    - target: {kind: Kustomization}
      patch: |
        - op: add
          path: /spec/patches
          value:
          - target: {kind: ConfigMap}
            patch: '[{"op":"add","path":"/data","value":{"patched":"yes"}}]'
`
	if mode == "patched rename" {
		declaration += `    - target: {kind: Kustomization}
      patch: '[{"op":"replace","path":"/metadata/name","value":"final"}]'
`
	}
	if mode == "patched coordinates" {
		declaration += `    - target: {kind: Kustomization}
      patch: '[{"op":"replace","path":"/metadata/name","value":"final"},{"op":"replace","path":"/spec/path","value":"./leaf"}]'
`
	}
	if mode == "unavailable named branch" {
		declaration = strings.ReplaceAll(declaration, "refs/heads/main", "refs/heads/unavailable")
	}
	testutil.WriteFile(t, root, "entry/instance.yaml", declaration)
	testutil.WriteFile(
		t,
		root,
		"entry/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n",
	)
	testutil.WriteFile(t, root, "entry/unrelated.yaml", "apiVersion: fluxcd.controlplane.io/v1\n"+
		"kind: FluxInstance\n"+
		"metadata: {name: installation, namespace: other}\n"+
		"spec: {distribution: {version: 2.x}}\n")
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"leaf/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n",
	)
	testutil.GitCommitAll(t,
		repo,
		"fixture",
		&gitlib.CommitOptions{Author: &object.Signature{
			Name:  "test",
			Email: "test@example.test",
			When: time.Unix(0,
				0),
		}})
	urls := []string{url}
	if mode == "unavailable named branch" {
		urls = nil
	}
	return fluxInstanceFixture{root: root, urls: urls}
}

func TestRender_FluxInstanceEntry(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"self declaration", "unavailable named branch", "patched rename", "patched coordinates"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fixture := writeFluxInstanceEntry(t, mode)
			o, err := newTestOrchestrator(t, Tree{
				Path: filepath.Join(fixture.root, "entry"), RepoRoot: fixture.root, SelfURLs: fixture.urls,
			}, Config{Concurrency: 2})
			assert.NoError(t, err)
			t.Cleanup(o.stop)
			fetcher := &advisorySourceFetcher{root: fixture.root}
			o.src.Fetchers[manifest.KindGitRepository] = fetcher
			assert.NoError(t, o.bootstrap(t.Context()))
			result, err := o.collectResult(o.run(t.Context()))
			assert.NoError(t, err)
			assert.Equal(t, fetcher.calls.Load(), int64(0))
			// Pass 8 Mayor ruling: literal entry and application receipts replace generated-sync ownership, identity, and publication assertions.
			assert.Equal(t, o.entryBinding.Producer.ReportHidden(), true)
			status, _ := o.store.GetStatus(o.entryBinding.Producer)
			assert.Equal(t, status.Status, store.StatusReady)
			count := 0
			for _, batch := range result.Manifests {
				for _, document := range batch.Documents {
					if manifest.DocKind(document) != manifest.KindConfigMap ||
						manifest.DocIdentity(document).Name != "workload" {
						continue
					}
					count++
					data, _ := document["data"].(map[string]any)
					assert.Equal(t, data["patched"], any("yes"))
				}
			}
			assert.Equal(t, count, 1)
			_, generated := o.store.GetArtifact(manifest.NamedResource{
				Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "control", Name: "flux",
			}, store.ArtifactContent).(*store.Emission)
			assert.Equal(t, generated, false)
		})
	}
}
