package oci

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

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
		for _, material := range []string{"auth absent", "auth wiped", "auth malformed", "cert absent", "cert wiped", "cert malformed", "proxy absent", "proxy wiped", "proxy malformed"} {
			t.Run(fmt.Sprintf("generated=%t/%s", generated, material), func(t *testing.T) {
				t.Parallel()
				testFetcherSyntheticCredentialContextCase(
					t,
					&generated, &material,
				)
			},
			)
		}
	}
}

func testFetcherSyntheticCredentialContextCase(
	t *testing.T, generated *bool,
	material *string,
) {
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
		assert.NoError(t, err)

		chart = children[0].(*manifest.HelmChartSource)
		expected = release.Named()
	}
	repo := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	var sec *manifest.Secret
	ref := &manifest.LocalObjectReference{Name: "material"}
	missing := !strings.HasSuffix((*material), "malformed")
	switch {
	case strings.HasPrefix((*material), "auth"):
		repo.SecretRef = ref
		if (*material) == "auth wiped" {
			sec = &manifest.Secret{
				StringData: map[string]any{".dockerconfigjson": manifest.ValuePlaceholder("config")},
			}
		}
		if (*material) == "auth malformed" {
			sec = &manifest.Secret{StringData: map[string]any{".dockerconfigjson": "{"}}
		}
	case strings.HasPrefix((*material), "cert"):
		repo.CertSecretRef = ref
		if (*material) == "cert wiped" {
			sec = &manifest.Secret{StringData: map[string]any{"ca.crt": manifest.ValuePlaceholder("ca")}}
		}
		if (*material) == "cert malformed" {
			sec = &manifest.Secret{StringData: map[string]any{"ca.crt": "invalid"}}
		}
	case strings.HasPrefix((*material), "proxy"):
		repo.ProxySecretRef = ref
		if (*material) == "proxy wiped" {
			sec = &manifest.Secret{
				StringData: map[string]any{"address": manifest.ValuePlaceholder("proxy")},
			}
		}
		if (*material) == "proxy malformed" {
			sec = &manifest.Secret{StringData: map[string]any{"address": "http://invalid/%"}}
		}
	default:
		repo.Provider = "aws"
	}
	testFetcherSyntheticCredentialContextCaseCompletion(
		t,
		&testFetcherSyntheticCredentialContextCaseCompletionInputs{
			ref:      &ref,
			sec:      &sec,
			repo:     &repo,
			expected: &expected,
			material: &material,
			missing:  &missing,
		},
	)
}

func testFetcherSyntheticCredentialContextCaseCompletion(
	t *testing.T,
	captures *testFetcherSyntheticCredentialContextCaseCompletionInputs,
) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, "{}")
	op := &operation{
		Fetcher: &Fetcher{Network: newTestNetwork(t), RegistryConfig: config},
		secrets: func(namespace, name string) *manifest.Secret {
			assert.FatalIf(
				t,
				namespace != "sources" || name != (*captures.ref).Name,
				"Secret lookup %s/%s used display owner",
				namespace,
				name,
			)
			return (*captures.sec)
		},
	}
	_, err := op.fetchSource(t.Context(), (*captures.repo), sourcecache.Policy{})
	label := captures.expected.Kind + " " + captures.expected.NamespacedName()
	if (*(*captures.material)) == "provider" {
		label = captures.expected.String()
	}
	assert.FatalIf(t, err == nil || strings.Contains(err.Error(), label) ||
		strings.Contains(err.Error(), "OCIRepository"), "early diagnostic=%v, owner=%s", err, (*captures.expected))
	assert.FatalIf(
		t,
		errors.Is(err, manifest.ErrMissingSecret) != (*captures.missing),
		"material classification changed: %v",
		err,
	)
	if failure, ok := errors.AsType[*source.MissingSecretError](err); ok {
		assert.FatalIf(
			t,
			failure.Owner != (*captures.repo).Named() || failure.Subject.Resource != (*captures.expected),
			"raw/display owner=%+v",
			failure,
		)
		if (*(*captures.material)) != "provider" && failure.Secret.Namespace != "sources" {
			t.Fatalf("Secret identity=%s", failure.Secret)
		}
	}
}

type testFetcherSyntheticCredentialContextCaseCompletionInputs struct {
	ref      **manifest.LocalObjectReference
	sec      **manifest.Secret
	repo     **manifest.OCIRepository
	expected *manifest.NamedResource
	material **string
	missing  *bool
}
