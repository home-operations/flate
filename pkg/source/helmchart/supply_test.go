package helmchart

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_SuppliedHTTPMaterial(t *testing.T) {
	t.Parallel()
	r := &manifest.HelmRepository{Name: "charts", Namespace: "apps", URL: "https://charts.invalid", SecretRef: &manifest.LocalObjectReference{Name: "auth"}, CertSecretRef: &manifest.LocalObjectReference{Name: "auth"}}
	ca := testutil.SelfSignedCA(t)
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: &manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"username": "filled", "password": "filled", "ca.crt": ca}}}})
	assert.NoError(t, err)
	s := store.New()
	s.SetSupplied(snapshot)
	s.AddObject(&manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"username": "repository"}})
	var reads store.ReadSet
	reads.Init(s, r.Named(), nil)
	defer reads.Close()
	network := new(source.Network)
	t.Cleanup(network.Close)
	prepared, err := (&operation{Fetcher: &Fetcher{network: network}, secrets: source.SecretLookup(&reads)}).prepareHTTP(r)
	assert.NoError(t, err)
	assert.Equal(t, prepared.username, "repository")
	assert.Equal(t, prepared.password, "filled")
	material, err := source.ResolveTransport(source.SecretLookup(&reads), manifest.DiagnosticFor(r.Named(), r), []source.TransportInput{{Reference: r.CertSecretRef, Use: source.TransportCert}})
	assert.NoError(t, err)
	want, err := network.HTTPTransport(source.HTTPTransportConfig{Material: material, Credentials: [2]string{"repository", "filled"}, DisableCompression: true})
	assert.NoError(t, err)
	if prepared.client.Transport != want {
		t.Fatal("supplied certificate did not configure the chart transport")
	}
}
