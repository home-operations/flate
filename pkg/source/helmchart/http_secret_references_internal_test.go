package helmchart

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetchHTTPChartSecretReferencesCase(t *testing.T, tc *struct {
	name    string
	state   string
	cert    bool
	missing bool
	input   bool
},
) {
	t.Helper()

	r := httpRepo("https://charts.example")
	secret := &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "first"}}
	f, fInputs := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return secret })
	if tc.cert {
		r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	} else {
		r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
		preparedHTTP(t, f, fInputs, r)
	}
	switch tc.state {
	case "unwired":
		fInputs.secrets = nil
	case "absent":
		secret = nil
	case "wiped":
		secret = &manifest.Secret{
			StringData: map[string]any{"username": "alice", "password": "..PLACEHOLDER_password.."},
		}
	case "partial":
		secret = &manifest.Secret{StringData: map[string]any{"username": "alice"}}
	case "malformed":
		secret = &manifest.Secret{
			Data:       map[string]any{"username": "credential-payload!"},
			StringData: map[string]any{"password": "first"},
		}
	case "malformed password":
		secret = &manifest.Secret{
			Data:       map[string]any{"password": "credential-payload!"},
			StringData: map[string]any{"username": "alice"},
		}
	case "nonscalar":
		secret = &manifest.Secret{
			StringData: map[string]any{"username": []any{"credential-payload"}, "password": "first"},
		}
	case "wiped malformed":
		secret = &manifest.Secret{
			StringData: map[string]any{"username": "..PLACEHOLDER_username.."},
			Data:       map[string]any{"password": "credential-payload!"},
		}
	case "malformed wiped":
		secret = &manifest.Secret{
			Data:       map[string]any{"username": "credential-payload!"},
			StringData: map[string]any{"password": "..PLACEHOLDER_password.."},
		}
	}
	f.cache = nil
	var artifact *store.SourceArtifact
	var err error
	if tc.state == "unwired" {
		_, err = (&operation{Fetcher: f}).prepareHTTP(r)
	} else {
		artifact, err = f.Fetch(
			t.Context(),
			helmChart("repo", "app-template", "1.0.0"),
			fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
			sourcecache.Policy{},
		)
	}
	if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing || artifact != nil {
		t.Fatalf("missing=%t, artifact=%v, error=%v", tc.missing, artifact, err)
	}
	if tc.input {
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
			"expected hard input error, got %v",
			err,
		)
		assert.ErrorIf(t, strings.Contains(err.Error(), "HelmRepository flux-system/repo") ||
			!strings.Contains(
				err.Error(),
				"secret flux-system/creds",
			), "input error lost source or Secret identity: %v", err)
		if strings.Contains(err.Error(), "credential-payload") {
			t.Error("input error exposed credential payload")
		}
	}
	if tc.missing && tc.state != "unwired" && !tc.cert {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.Owner != r.Named() || missing.Secret.Name != "creds" ||
			missing.Secret.Namespace != r.Namespace ||
			missing.RetryAmbient == nil, "missing error lost source identity or ambient continuation: %v", err)
	}
}
