package e2e_test

import (
	"maps"
	"strings"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_ArtifactGeneratorChangedArtifactReach(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reached []string
	}{
		{name: "generator-app", reached: []string{"alpha"}},
		{name: "generator-shared", reached: []string{"alpha", "beta", "gamma"}},
		{name: "generator-excluded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := harness.Open(t, "external-artifact-reach", tc.name)
			result := fixture.RunWithWork(t)[0]
			work := result.ReconcileWork(t)
			maps.DeleteFunc(work, func(key [3]string, _ [2]int) bool {
				owner, phase := key[1], key[2]
				switch phase {
				case "kustomize":
					_, target, _ := strings.Cut(owner, "/")
					return !strings.HasPrefix(target, "default/")
				case "helm", "evaluation":
					return false
				case "acquisition", "cache":
					return !strings.HasPrefix(owner, "ArtifactGenerator.")
				default:
					return true
				}
			})
			want := make(map[[3]string][2]int)
			for _, side := range []string{"0", "1"} {
				const generator = "ArtifactGenerator.source.extensions.fluxcd.io/default/generator"
				for _, phase := range []string{"evaluation", "acquisition", "cache"} {
					want[[3]string{side, generator, phase}] = [2]int{3, 0}
				}
				for _, name := range tc.reached {
					kustomization := "Kustomization.kustomize.toolkit.fluxcd.io/default/" + name
					release := "HelmRelease.helm.toolkit.fluxcd.io/default/" + name
					want[[3]string{side, kustomization, "kustomize"}] = [2]int{1, 0}
					want[[3]string{side, release, "helm"}] = [2]int{1, 0}
				}
			}
			if !maps.Equal(work, want) {
				t.Errorf("work got %v, want %v", work, want)
			}
		})
	}
}

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_GeneratedArtifactSiblingComponent(t *testing.T) {
	fixture := harness.Open(t, "external-artifact-reach", "generator-component")
	work := fixture.RunWithWork(t)[0].ReconcileWork(t)
	const consumer = "Kustomization.kustomize.toolkit.fluxcd.io/default/consumer"
	maps.DeleteFunc(work, func(key [3]string, _ [2]int) bool {
		return key[1] != consumer || key[2] != "kustomize"
	})
	want := map[[3]string][2]int{
		{"0", consumer, "kustomize"}: {1, 0},
		{"1", consumer, "kustomize"}: {1, 0},
	}
	if !maps.Equal(work, want) {
		t.Errorf("work got %v, want %v", work, want)
	}
}

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_ArtifactGeneratorChangedArtifactReach_ImportedShared(t *testing.T) {
	const prefix = "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/"
	for _, name := range []string{"imported-shared", "imported-shared-outside", "imported-shared-unread"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "external-artifact-reach", name)
			work := fixture.RunWithWork(t)[0].ReconcileWork(t)
			maps.DeleteFunc(work, func(key [3]string, _ [2]int) bool {
				return key[2] != "kustomize" || !strings.HasPrefix(key[1], prefix) || key[1] == prefix+"flux-system"
			})
			want := map[[3]string][2]int{}
			if name == "imported-shared" {
				for _, owner := range []string{"app", "second"} {
					for _, side := range []string{"0", "1"} {
						want[[3]string{side, prefix + owner, "kustomize"}] = [2]int{1, 0}
					}
				}
			}
			if !maps.Equal(work, want) {
				t.Errorf("work got %v, want %v", work, want)
			}
		})
	}
}

//nolint:paralleltest // The CLI fixture changes process-wide state.
func TestE2E_ArtifactGeneratorChangedArtifactReach_RootDiff(t *testing.T) {
	fixture := harness.Open(t, "external-artifact-reach", "root-diff")
	result := fixture.RunWithWork(t)[0]
	work := result.ReconcileWork(t)
	const syncRelease = "HelmRelease.helm.toolkit.fluxcd.io/flux-system/flux-instance"
	const syncBootstrap = "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/flux-instance"
	const syncRoot = "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/flux-system"
	const alpha = "Kustomization.kustomize.toolkit.fluxcd.io/default/alpha"
	syncRenders, syncBuilds := 0, 0
	maps.DeleteFunc(work, func(key [3]string, count [2]int) bool {
		switch key[2] {
		case "kustomize":
			if key[1] != syncBootstrap {
				return false
			}
			syncBuilds += count[0]
		case "helm":
			if key[1] != syncRelease {
				return false
			}
			syncRenders += count[0]
		default:
			return true
		}
		return true
	})
	if syncRenders > 1 {
		t.Errorf("sync Helm attempts got %d, want at most one", syncRenders)
	}
	if syncBuilds != 1 {
		t.Errorf("bootstrap Kustomize attempts got %d, want one across both snapshots", syncBuilds)
	}
	if builds := strings.Count(result.Stderr, `msg="kustomize: build"`); builds != 5 {
		t.Errorf("Kustomize builds got %d, want five across both snapshots", builds)
	}
	const release = "HelmRelease.helm.toolkit.fluxcd.io/default/alpha"
	want := map[[3]string][2]int{
		{"0", release, "helm"}:       {1, 0},
		{"1", release, "helm"}:       {1, 0},
		{"0", alpha, "kustomize"}:    {1, 0},
		{"1", alpha, "kustomize"}:    {1, 0},
		{"0", syncRoot, "kustomize"}: {1, 0},
		{"1", syncRoot, "kustomize"}: {1, 0},
	}
	if !maps.Equal(work, want) {
		t.Errorf("work got %v, want %v", work, want)
	}
}
