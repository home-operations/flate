package oci

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_SyntheticCredentialContext(t *testing.T) {
	t.Parallel()
	for _, generated := range []bool{false, true} {
		for _, material := range []string{"auth absent", "auth empty", "auth malformed", "cert absent", "cert empty", "cert malformed", "proxy absent", "proxy empty", "proxy malformed"} {
			t.Run(fmt.Sprintf("generated=%t/%s", generated, material), func(t *testing.T) {
				t.Parallel()
				testFetcherSyntheticCredentialContextCase1(t, &generated, &material)
			})
		}
	}
}

func testFetcherSyntheticCredentialContextCase1(t *testing.T, generated *bool, material *string) {
	t.Helper()

	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "sources"}
	chart.Chart = "app"
	expected := chart.Named()
	if *generated {
		release := &manifest.HelmRelease{
			Name:      "release",
			Namespace: "apps",
			HelmReleaseSpec: helmv2.HelmReleaseSpec{
				Chart: &helmv2.HelmChartTemplate{
					Spec: helmv2.HelmChartTemplateSpec{
						Chart: "app",
						SourceRef: &helmv2.CrossNamespaceObjectReference{
							Kind:      manifest.KindHelmRepository,
							Name:      "charts",
							Namespace: "sources",
						},
					},
				},
			},
		}
		children, err := release.GeneratedObjects()
		assert.FatalIf(t, err != nil, "%s", err)

		chart = children[0].(*manifest.HelmChartSource)
		expected = release.Named()
	}
	repo := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	sec := syntheticMaterial(repo, *material)
	ref := &manifest.LocalObjectReference{Name: "material"}
	missing := !strings.HasSuffix(*material, "malformed")
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, "{}")
	op := &operation{
		Fetcher: &Fetcher{Network: newTestNetwork(t), RegistryConfig: config},
		secrets: func(namespace, name string) store.ReferenceFacts {
			assert.FatalIf(t, namespace != "sources" || name != ref.Name,
				"Secret lookup %s/%s used display owner", namespace, name)

			secretInput := sec
			if secretInput == nil {
				return store.ReferenceFacts{}
			}
			return store.ReferenceFacts{Data: secretInput, Original: secretInput}
		},
	}
	_, err := fetchtest.Retry(t.Context(), func(context.Context) (source.PreparedFetch, error) {
		return op.fetchSource(t.Context(), repo, &sourcecache.Policy{})
	})
	label := expected.Kind + " " + expected.NamespacedName()
	if (*material) == "provider" {
		label = expected.String()
	}
	assert.FatalIf(t, err == nil || strings.Contains(err.Error(), label) ||
		strings.Contains(err.Error(), "OCIRepository"),
		"early diagnostic=%v, owner=%s", err, expected)
	assert.FatalIf(t, errors.Is(err, manifest.ErrMissingSecret) != missing,
		"material classification changed: %v", err)

	if failure, ok := errors.AsType[*source.MissingSecretError](err); ok {
		assert.FatalIf(t, failure.Owner != repo.Named() || failure.Subject.Resource != expected,
			"raw/display owner=%+v", failure)
		assert.FatalIf(t, (*material) != "provider" && failure.Secret.Namespace != "sources",
			"Secret identity=%s", failure.Secret)
	}
}

func syntheticMaterial(repo *manifest.OCIRepository, material string) *manifest.Secret {
	var sec *manifest.Secret
	ref := &manifest.LocalObjectReference{Name: "material"}
	switch {
	case strings.HasPrefix(material, "auth"):
		repo.SecretRef = ref
		if material == "auth empty" {
			sec = &manifest.Secret{StringData: map[string]any{".dockerconfigjson": ""}}
		}
		if material == "auth malformed" {
			sec = &manifest.Secret{StringData: map[string]any{".dockerconfigjson": "{"}}
		}
	case strings.HasPrefix(material, "cert"):
		repo.CertSecretRef = ref
		if material == "cert empty" {
			sec = &manifest.Secret{StringData: map[string]any{"ca.crt": ""}}
		}
		if material == "cert malformed" {
			sec = &manifest.Secret{StringData: map[string]any{"ca.crt": "invalid"}}
		}
	case strings.HasPrefix(material, "proxy"):
		repo.ProxySecretRef = ref
		if material == "proxy empty" {
			sec = &manifest.Secret{StringData: map[string]any{"address": ""}}
		}
		if material == "proxy malformed" {
			sec = &manifest.Secret{StringData: map[string]any{"address": "http://invalid/%"}}
		}
	default:
		repo.Provider = "aws"
	}
	return sec
}
