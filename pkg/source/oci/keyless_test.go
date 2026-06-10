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

// OIDC identity + manifest digest of the captured cloudnative-pg chart, signed
// with cosign 2.x keyless — a native sigstore bundle attached via the OCI 1.1
// referrers API (no legacy `.sig` tag). See testdata/referrers, captured from
// ghcr.io/cloudnative-pg/charts/cloudnative-pg:0.28.0.
const (
	cnpgIssuer  = `^https://token\.actions\.githubusercontent\.com$`
	cnpgSubject = `^https://github\.com/cloudnative-pg/charts.*$`
	cnpgDigest  = "sha256:d216a2b1e3c1ea04e0baff88fff91d768cb1f4f21f83b1a8a6d620bcdf85435f"
)

// nativeBundleFixture loads the captured native sigstore bundle blob (the DSSE
// envelope the cnpg referrer manifest points at). Like the legacy fixture, it
// verifies against the entry's Rekor integrated time, so it stays valid
// indefinitely despite the short-lived Fulcio certificate.
func nativeBundleFixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "referrers", "bundle.json")) //nolint:gosec // fixed in-repo testdata path
	if err != nil {
		t.Fatalf("read native bundle fixture: %v", err)
	}
	return b
}

// TestVerifyNativeBundle_Verifies: the captured cnpg native bundle verifies
// end-to-end against the embedded trusted root when the OIDC identity matches —
// DSSE signature + Fulcio chain + Rekor inclusion + identity + the in-toto
// subject digest pinned to the pulled manifest digest, fully offline.
func TestVerifyNativeBundle_Verifies(t *testing.T) {
	repo := keylessRepo(manifest.OIDCIdentityMatch{Issuer: cnpgIssuer, Subject: cnpgSubject})
	ok, err := (&Fetcher{}).verifyNativeBundle(nativeBundleFixture(t), repo, cnpgDigest)
	if !ok || err != nil {
		t.Fatalf("verifyNativeBundle = (%v, %v), want (true, nil)", ok, err)
	}
}

// TestVerifyNativeBundle_NoMatchersAnyIdentity: an empty matchOIDCIdentity gates
// on chain + transparency log + digest only, so the bundle still verifies.
func TestVerifyNativeBundle_NoMatchersAnyIdentity(t *testing.T) {
	ok, err := (&Fetcher{}).verifyNativeBundle(nativeBundleFixture(t), keylessRepo(), cnpgDigest)
	if !ok || err != nil {
		t.Fatalf("verifyNativeBundle (no matchers) = (%v, %v), want (true, nil)", ok, err)
	}
}

// TestVerifyNativeBundle_WrongIdentityFails: a mismatched subject regex must
// fail — the identity gate is real.
func TestVerifyNativeBundle_WrongIdentityFails(t *testing.T) {
	repo := keylessRepo(manifest.OIDCIdentityMatch{Issuer: cnpgIssuer, Subject: `^https://github\.com/evil/impostor.*$`})
	ok, err := (&Fetcher{}).verifyNativeBundle(nativeBundleFixture(t), repo, cnpgDigest)
	if ok || err == nil {
		t.Fatalf("verifyNativeBundle (wrong identity) = (%v, %v), want (false, err)", ok, err)
	}
}

// TestVerifyNativeBundle_WrongDigestFails is the anti-lifting guard: the bundle
// is a valid signature, but bound to a DIFFERENT pulled digest. The in-toto
// subject-digest check in the policy must reject it — proving a valid signature
// cannot be replayed onto another artifact. Regression for the
// WithArtifactDigest-not-WithoutArtifactUnsafe correctness requirement.
func TestVerifyNativeBundle_WrongDigestFails(t *testing.T) {
	other := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	repo := keylessRepo(manifest.OIDCIdentityMatch{Issuer: cnpgIssuer, Subject: cnpgSubject})
	ok, err := (&Fetcher{}).verifyNativeBundle(nativeBundleFixture(t), repo, other)
	if ok || err == nil {
		t.Fatalf("verifyNativeBundle (wrong digest) = (%v, %v), want (false, err)", ok, err)
	}
}

// TestVerifyNativeBundle_MalformedBundleFails: garbage bytes are not a sigstore
// bundle → hard error (not a silent skip).
func TestVerifyNativeBundle_MalformedBundleFails(t *testing.T) {
	repo := keylessRepo(manifest.OIDCIdentityMatch{Issuer: cnpgIssuer, Subject: cnpgSubject})
	ok, err := (&Fetcher{}).verifyNativeBundle([]byte("not a bundle"), repo, cnpgDigest)
	if ok || err == nil {
		t.Fatalf("verifyNativeBundle (malformed) = (%v, %v), want (false, err)", ok, err)
	}
}

// TestNativeIdentityPolicy_MalformedDigest: a digest without the algo:hex shape
// is a build-time policy error, not a panic.
func TestNativeIdentityPolicy_MalformedDigest(t *testing.T) {
	if _, err := nativeIdentityPolicy("notadigest", nil); err == nil {
		t.Fatal("nativeIdentityPolicy(malformed) = nil err, want error")
	}
}
