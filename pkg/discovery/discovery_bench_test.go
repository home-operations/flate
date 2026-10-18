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
			testutil.WriteFile(b, root, "flux/ks.yaml", testutil.KustomizationYAML("top", "flux-system", "  {path: ./level0, sourceRef: {kind: GitRepository, name: "+sourceName+"}}\n"))
			for level := range depth {
				var resources strings.Builder
				for child := range width {
					name := fmt.Sprintf("ks%d-%d", level, child)
					fmt.Fprintf(&resources, "  - %s.yaml\n", name)
					testutil.WriteFile(b, root, fmt.Sprintf("level%d/%s.yaml", level, name), testutil.KustomizationYAML(name, "", "  {path: ./level"+fmt.Sprintf("%d", level+1)+", sourceRef: {kind: GitRepository, name: "+sourceName+"}}\n"))
				}
				testutil.WriteFile(b, root, fmt.Sprintf("level%d/kustomization.yaml", level),
					"namespace: flux-system\nresources:\n"+resources.String())
			}
			testutil.WriteFile(b, root, fmt.Sprintf("level%d/kustomization.yaml", depth), "resources: []\n")
			cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root, WipeSecrets: true}
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
				r := testutil.GitInit(b, root)
				if err := r.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))); err != nil {
					b.Fatal(err)
				}
				commit := testutil.GitCommitAll(b, r, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "b", Email: "b@e", When: time.Unix(0, 0)}})
				if repo.name == "late_pin" {
					if _, err := r.CreateTag("fixture", commit, nil); err != nil {
						b.Fatal(err)
					}
					testutil.GitCommitAll(b, r, "head", &git.CommitOptions{
						Author:            &object.Signature{Name: "b", Email: "b@e", When: time.Unix(1, 0)},
						AllowEmptyCommits: true,
					})
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
	testutil.WriteFile(b, root, "flux/ks.yaml", testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}\n"))
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

func BenchmarkRun_WideDirectory(b *testing.B) {
	root := b.TempDir()
	testutil.WriteFile(b, root, "flux/ks.yaml", testutil.KustomizationYAML("wide", "flux-system", "  {path: ./wide, sourceRef: {kind: GitRepository, name: flux-system}}\n"))
	var resources strings.Builder
	for i := range 4000 {
		file := fmt.Sprintf("cm-%d.yaml", i)
		fmt.Fprintf(&resources, "  - %s\n", file)
		testutil.WriteFile(b, root, "wide/"+file, fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm-%d, namespace: default}\ndata: {value: fixture}\n", i))
	}
	testutil.WriteFile(b, root, "wide/kustomization.yaml", "resources:\n"+resources.String())
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Run(b.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Store: store.New(), WipeSecrets: true}); err != nil {
			b.Fatal(err)
		}
	}
}
