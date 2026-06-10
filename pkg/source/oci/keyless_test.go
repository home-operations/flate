package oci

import (
	"os"
	"path/filepath"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// OIDC identity of the captured external-secrets chart signature (GitHub
// Actions keyless).
const (
	esIssuer  = `^https://token\.actions\.githubusercontent\.com$`
	esSubject = `^https://github\.com/external-secrets/external-secrets.*$`
)

// keylessFixture loads the captured external-secrets cosign keyless `.sig`
// material — the signed payload plus the signature / certificate / Rekor bundle
// annotations of a real legacy simple-signing layer (see testdata/keyless,
// captured from ghcr.io/external-secrets/charts/external-secrets). Verification
// trusts the entry's Rekor integrated time, not wall-clock, so the long-expired
// Fulcio certificate still verifies and the fixture stays valid indefinitely.
func keylessFixture(t *testing.T) (signatureLayer, []byte) {
	t.Helper()
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("testdata", "keyless", name)) //nolint:gosec // fixed in-repo testdata path
		if err != nil {
			t.Fatalf("read fixture %s: %v", name, err)
		}
		return b
	}
	layer := signatureLayer{Annotations: map[string]string{
		cosignSignatureAnnotation: string(read("signature.b64")),
		cosignCertAnnotation:      string(read("certificate.pem")),
		cosignBundleAnnotation:    string(read("bundle.json")),
	}}
	return layer, read("payload.json")
}

func keylessRepo(matchers ...manifest.OIDCIdentityMatch) *manifest.OCIRepository {
	return &manifest.OCIRepository{
		Name: "external-secrets", Namespace: "external-secrets",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			Verify: &manifest.OCIRepositoryVerify{Provider: "cosign", MatchOIDCIdentity: matchers},
		},
	}
}

// TestVerifyPayloadKeyless_Verifies: the captured external-secrets signature
// verifies end-to-end against the embedded trusted root when the configured
// OIDC identity matches — certificate chain + Rekor inclusion promise + SCT +
// identity, fully offline.
func TestVerifyPayloadKeyless_Verifies(t *testing.T) {
	layer, payload := keylessFixture(t)
	repo := keylessRepo(manifest.OIDCIdentityMatch{Issuer: esIssuer, Subject: esSubject})
	ok, err := (&Fetcher{}).verifyPayloadKeyless(layer, payload, repo)
	if !ok || err != nil {
		t.Fatalf("verifyPayloadKeyless = (%v, %v), want (true, nil)", ok, err)
	}
}

// TestVerifyPayloadKeyless_NoMatchersAnyIdentity: an empty matchOIDCIdentity
// gates on chain + transparency log only (Flux's identity-unconstrained
// keyless), so the same signature verifies.
func TestVerifyPayloadKeyless_NoMatchersAnyIdentity(t *testing.T) {
	layer, payload := keylessFixture(t)
	ok, err := (&Fetcher{}).verifyPayloadKeyless(layer, payload, keylessRepo())
	if !ok || err != nil {
		t.Fatalf("verifyPayloadKeyless (no matchers) = (%v, %v), want (true, nil)", ok, err)
	}
}

// TestVerifyPayloadKeyless_WrongIdentityFails: a mismatched subject regex must
// fail — the gate is real, not advisory.
func TestVerifyPayloadKeyless_WrongIdentityFails(t *testing.T) {
	layer, payload := keylessFixture(t)
	repo := keylessRepo(manifest.OIDCIdentityMatch{Issuer: esIssuer, Subject: `^https://github\.com/evil/impostor.*$`})
	ok, err := (&Fetcher{}).verifyPayloadKeyless(layer, payload, repo)
	if ok || err == nil {
		t.Fatalf("verifyPayloadKeyless (wrong identity) = (%v, %v), want (false, err)", ok, err)
	}
}

// TestVerifyPayloadKeyless_TamperedPayloadFails: flipping a payload byte breaks
// the message signature (and the Rekor body↔signature binding), so verification
// fails even with a matching identity.
func TestVerifyPayloadKeyless_TamperedPayloadFails(t *testing.T) {
	layer, payload := keylessFixture(t)
	tampered := append([]byte(nil), payload...)
	tampered[0] ^= 0xff
	repo := keylessRepo(manifest.OIDCIdentityMatch{Issuer: esIssuer, Subject: esSubject})
	ok, err := (&Fetcher{}).verifyPayloadKeyless(layer, tampered, repo)
	if ok || err == nil {
		t.Fatalf("verifyPayloadKeyless (tampered payload) = (%v, %v), want (false, err)", ok, err)
	}
}

// TestVerifyPayloadKeyless_MissingMaterialFails: a layer with the signature but
// no certificate/bundle (a keyed-only or stripped layer) cannot build a keyless
// bundle and is a hard error.
func TestVerifyPayloadKeyless_MissingMaterialFails(t *testing.T) {
	_, payload := keylessFixture(t)
	layer := signatureLayer{Annotations: map[string]string{cosignSignatureAnnotation: "AAAA"}}
	repo := keylessRepo(manifest.OIDCIdentityMatch{Issuer: esIssuer, Subject: esSubject})
	ok, err := (&Fetcher{}).verifyPayloadKeyless(layer, payload, repo)
	if ok || err == nil {
		t.Fatalf("verifyPayloadKeyless (no cert/bundle) = (%v, %v), want (false, err)", ok, err)
	}
}
