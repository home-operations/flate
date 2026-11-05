package oci

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_ProviderTransportValidation(t *testing.T) {
	t.Parallel()
	for _, use := range []string{"cert", "proxy"} {
		t.Run(use, func(t *testing.T) {
			t.Parallel()
			repo := ociRepo("provider", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://fixture.invalid/image"
				s.Provider = sourcev1.AmazonOCIProvider
				if use == "cert" {
					s.CertSecretRef = &manifest.LocalObjectReference{Name: "transport"}
				} else {
					s.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
				}
			})
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{
					StringData: map[string]any{"ca.crt": "invalid", "address": "http://proxy.invalid/%"},
				}
			}}
			fetcher := &Fetcher{Network: newTestNetwork(t)}
			_, err := fetchtest.Fetch(
				t.Context(),
				fetcher.Fetch,
				repo,
				inputs.readSet(repo),
				&sourcecache.Policy{},
				nil,
			)
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrMissingSecret),
				"provider hid malformed transport: %v",
				err,
			)
		})
	}
}

func TestFetcher_ProviderRegistryMatching(t *testing.T) {
	t.Parallel()
	for _, matching := range []bool{false, true} {
		t.Run(strconv.FormatBool(matching), func(t *testing.T) {
			t.Parallel()
			registry := "other.invalid"
			if matching {
				registry = "fixture.invalid"
			}
			config := filepath.Join(t.TempDir(), "config.json")
			testutil.WriteFileAt(
				t,
				config,
				fmt.Sprintf(`{"auths":{%q:{"username":"ambient","password":"fixture"}}}`, registry),
			)
			repo := ociRepo("provider", func(s *sourcev1.OCIRepositorySpec) {
				s.URL = "oci://fixture.invalid/image"
				s.Provider = sourcev1.AmazonOCIProvider
				s.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			})
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{
					StringData: map[string]any{
						".dockerconfigjson": `{"auths":{"unrelated.invalid":{"username":"repository","password":"fixture"}}}`,
					},
				}
			}}
			fetcher := &Fetcher{Network: newTestNetwork(t), RegistryConfig: config}
			_, err := fetchtest.Fetch(
				t.Context(),
				fetcher.Fetch,
				repo,
				inputs.readSet(repo),
				&sourcecache.Policy{},
				nil,
			)
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(
				t,
				!ok || missing.Secret.Name != "auth" || missing.RetryAmbient == nil,
				"actual-registry continuation: %v",
				err,
			)
		})
	}
}
