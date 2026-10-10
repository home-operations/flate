package discovery

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkRunParentGates(b *testing.B) {
	root := b.TempDir()
	testutil.WriteGeneratedValuesCluster(b, root)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Run(b.Context(), Config{Path: root, RepoRoot: root, Store: store.New(), WipeSecrets: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRun_FollowedChain(b *testing.B) {
	for _, repo := range []struct {
		name, url string
	}{
		{"unpinned", ""},
		{"non_matching_ref", "https://example.invalid/other.git"},
		{"pinned", "https://example.invalid/self.git"}, // The bootstrap ID makes this a working-tree alias.
		{"late_pin", "https://example.invalid/self.git"},
	} {
		b.Run(repo.name, func(b *testing.B) {
			root := b.TempDir()
			const depth, width = 8, 40
			sourceName, ref := "flux-system", "{branch: main}"
			if repo.name == "late_pin" {
				sourceName, ref = "cluster", "{tag: fixture}"
			}
			testutil.WriteFile(b, root, "flux/ks.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: top, namespace: flux-system}
spec: {path: ./level0, sourceRef: {kind: GitRepository, name: %s}}
`, sourceName))
			for level := range depth {
				var resources strings.Builder
				for child := range width {
					name := fmt.Sprintf("ks%d-%d", level, child)
					fmt.Fprintf(&resources, "  - %s.yaml\n", name)
					testutil.WriteFile(b, root, fmt.Sprintf("level%d/%s.yaml", level, name), fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s}
spec: {path: ./level%d, sourceRef: {kind: GitRepository, name: %s}}
`, name, level+1, sourceName))
				}
				testutil.WriteFile(b, root, fmt.Sprintf("level%d/kustomization.yaml", level),
					"namespace: flux-system\nresources:\n"+resources.String())
			}
			testutil.WriteFile(b, root, fmt.Sprintf("level%d/kustomization.yaml", depth), "resources: []\n")
			cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root, WipeSecrets: true}
			if repo.name == "late_pin" {
				cfg.SourceCache = source.NewCache(cacheroot.New(b.TempDir()))
			}
			if repo.url != "" {
				testutil.WriteFile(b, root, "flux/repo.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: %s, namespace: flux-system}
spec:
  url: %s
  ref: %s
`, sourceName, repo.url, ref))
				cfg.SelfURLs = []string{"https://example.invalid/self.git"}
			}
			if repo.name == "pinned" || repo.name == "late_pin" {
				r, err := git.PlainInit(root, false)
				if err != nil {
					b.Fatal(err)
				}
				if err := r.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))); err != nil {
					b.Fatal(err)
				}
				w, err := r.Worktree()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := w.Add("."); err != nil {
					b.Fatal(err)
				}
				commit, err := w.Commit("fixture", &git.CommitOptions{Author: &object.Signature{Name: "b", Email: "b@e", When: time.Unix(0, 0)}})
				if err != nil {
					b.Fatal(err)
				}
				if repo.name == "late_pin" {
					if _, err := r.CreateTag("fixture", commit, nil); err != nil {
						b.Fatal(err)
					}
					if _, err := w.Commit("head", &git.CommitOptions{
						Author:            &object.Signature{Name: "b", Email: "b@e", When: time.Unix(1, 0)},
						AllowEmptyCommits: true,
					}); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				cfg.Store = store.New()
				if _, err := Run(b.Context(), cfg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRun_StandaloneSecretProducer(b *testing.B) {
	root := b.TempDir()
	testutil.WriteFile(b, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}
`)
	testutil.WriteFile(b, root, "apps/kustomization.yaml", "namespace: secure\nresources: [producer.yaml]\n")
	testutil.WriteFile(b, root, "apps/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: producer, namespace: secure}
spec:
 target: {name: app-secret}
 data: [{secretKey: HOST, remoteRef: {key: fixture}}]
`)
	testutil.WriteFile(b, root, "secret.yaml", `apiVersion: v1
kind: Secret
metadata: {name: app-secret, namespace: secure}
stringData: {HOST: fixture-value}
`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Run(b.Context(), Config{Path: root, RepoRoot: root, Store: store.New(), WipeSecrets: true}); err != nil {
			b.Fatal(err)
		}
	}
}
