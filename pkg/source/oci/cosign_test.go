package oci

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestCosignSigTag(t *testing.T) {
	cases := map[string]string{
		"sha256:abc":           "sha256-abc.sig",
		"sha512:deadbeef":      "sha512-deadbeef.sig",
		"sha256:1234567890abc": "sha256-1234567890abc.sig",
	}
	for in, want := range cases {
		if got := cosignSigTag(in); got != want {
			t.Errorf("cosignSigTag(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParsePEMPublicKeys_ECDSA(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	pemBytes := mustPEMPublicKey(t, &priv.PublicKey)
	keys := parsePEMPublicKeys(pemBytes)
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if _, ok := keys[0].(*ecdsa.PublicKey); !ok {
		t.Errorf("expected *ecdsa.PublicKey, got %T", keys[0])
	}
}

func TestParsePEMPublicKeys_MultipleKeys(t *testing.T) {
	priv1, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	priv2, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	combined := append(mustPEMPublicKey(t, &priv1.PublicKey), mustPEMPublicKey(t, &priv2.PublicKey)...)
	keys := parsePEMPublicKeys(combined)
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}
}

func TestParsePEMPublicKeys_IgnoresUnknownBlocks(t *testing.T) {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	combined := []byte("not pem at all\n")
	combined = append(combined, mustPEMPublicKey(t, &priv.PublicKey)...)
	// Add a noise block that isn't a public key.
	combined = append(combined, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("noise")})...)
	keys := parsePEMPublicKeys(combined)
	if len(keys) != 1 {
		t.Errorf("expected 1 key (other blocks ignored), got %d", len(keys))
	}
}

func TestVerifyCosignSignatureBytes_ECDSA(t *testing.T) {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	payload := []byte(`{"critical":{"image":{"docker-manifest-digest":"sha256:abc"}}}`)
	hash := sha256.Sum256(payload)
	sig, err := ecdsa.SignASN1(rand.Reader, priv, hash[:])
	if err != nil {
		t.Fatalf("SignASN1: %v", err)
	}
	if err := verifyCosignSignatureBytes(&priv.PublicKey, payload, sig); err != nil {
		t.Errorf("verify failed for valid signature: %v", err)
	}
	// Flip a byte; verify should fail.
	sig[0] ^= 0x01
	if err := verifyCosignSignatureBytes(&priv.PublicKey, payload, sig); err == nil {
		t.Errorf("verify should fail for tampered signature")
	}
}

func TestVerifyCosignSignatureBytes_RSA(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	payload := []byte("hello cosign")
	hash := sha256.Sum256(payload)
	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatalf("rsa.SignPKCS1v15: %v", err)
	}
	if err := verifyCosignSignatureBytes(&priv.PublicKey, payload, sig); err != nil {
		t.Errorf("verify failed for valid RSA signature: %v", err)
	}
}

// Cosign keyed-mode ed25519 signs the RAW payload (PureEdDSA), not a
// pre-computed digest. Verifying must feed the same raw payload back.
// flate previously fed SHA-256(payload), which produced false-negatives
// for every legitimate ed25519 signature.
func TestVerifyCosignSignatureBytes_Ed25519(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	payload := []byte(`{"critical":{"image":{"docker-manifest-digest":"sha256:abc"}}}`)
	sig := ed25519.Sign(priv, payload)
	if err := verifyCosignSignatureBytes(pub, payload, sig); err != nil {
		t.Errorf("verify failed for valid Ed25519 signature: %v", err)
	}
	// Sanity: tamper rejects.
	sig[0] ^= 0x01
	if err := verifyCosignSignatureBytes(pub, payload, sig); err == nil {
		t.Errorf("verify should fail for tampered ed25519 signature")
	}
}

func TestVerifyCosignSignatureBytes_UnsupportedKey(t *testing.T) {
	err := verifyCosignSignatureBytes("not a key", []byte("d"), []byte("s"))
	if err == nil || !strings.Contains(err.Error(), "unsupported public key type") {
		t.Errorf("expected unsupported-key error; got %v", err)
	}
}

func TestLoadCosignPublicKeys_NoSecretGetter(t *testing.T) {
	f := &Fetcher{}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			Verify: &manifest.OCIRepositoryVerify{
				SecretRef: &manifest.LocalObjectReference{Name: "keys"},
			},
		},
	}
	_, err := f.loadCosignPublicKeys(repo)
	if err == nil || !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Errorf("expected source.SecretGetter error; got %v", err)
	}
}

func TestLoadCosignPublicKeys_SecretNotFound(t *testing.T) {
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret { return nil },
	}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			Verify: &manifest.OCIRepositoryVerify{
				SecretRef: &manifest.LocalObjectReference{Name: "missing"},
			},
		},
	}
	_, err := f.loadCosignPublicKeys(repo)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected not-found error; got %v", err)
	}
}

func TestLoadCosignPublicKeys_ParsesPEM(t *testing.T) {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pubPEM := mustPEMPublicKey(t, &priv.PublicKey)
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{
				StringData: map[string]any{
					"cosign.pub": string(pubPEM),
					"junk":       "not a key",
				},
			}
		},
	}
	repo := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			Verify: &manifest.OCIRepositoryVerify{
				SecretRef: &manifest.LocalObjectReference{Name: "keys"},
			},
		},
	}
	keys, err := f.loadCosignPublicKeys(repo)
	if err != nil {
		t.Fatalf("loadCosignPublicKeys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
}

// TestLoadCosignPublicKeys_ParsesPEMFromData is the towonel repro at the
// loader level: the public key lives base64-encoded under data.cosign.pub
// (the common Secret shape). Since parseSecret no longer wipes non-SOPS
// values, the key survives and StringFromSecret base64-decodes it back to
// the PEM that parsePEMPublicKeys parses.
func TestLoadCosignPublicKeys_ParsesPEMFromData(t *testing.T) {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pemB64 := base64.StdEncoding.EncodeToString(mustPEMPublicKey(t, &priv.PublicKey))
	f := &Fetcher{
		Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{Data: map[string]any{"cosign.pub": pemB64}}
		},
	}
	keys, err := f.loadCosignPublicKeys(cosignVerifyRepo())
	if err != nil {
		t.Fatalf("loadCosignPublicKeys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("expected 1 key from data.cosign.pub, got %d", len(keys))
	}
}

// TestVerifyCosignSignature_KeylessSignatureUnreachableSkips: keyless verify
// (matchOIDCIdentity, no secretRef) honors the same transport boundary as the
// keyed path — when the signature isn't reachable in the registry the check
// can't complete, so it WARNs and skips (false, nil) rather than failing the
// render. A reachable keyless signature is verified end-to-end against captured
// material in keyless_test.go.
func TestVerifyCosignSignature_KeylessSignatureUnreachableSkips(t *testing.T) {
	f := &Fetcher{}
	repo := &manifest.OCIRepository{
		Name: "app-template", Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			Verify: &manifest.OCIRepositoryVerify{
				Provider: "cosign",
				MatchOIDCIdentity: []sourcev1.OIDCIdentityMatch{
					{Issuer: "^https://token.actions.githubusercontent.com$",
						Subject: "^https://github.com/bjw-s-labs/helm-charts.*$"},
				},
			},
		},
	}
	// blobRepoClient serves /blobs/ only; any /manifests/ lookup 404s, so
	// Resolve(sigTag) fails → unreachable → skip.
	rc := blobRepoClient(t, map[string][]byte{})
	verified, err := f.verifyCosignSignature(context.Background(), rc, repo, "sha256:deadbeef")
	if err != nil || verified {
		t.Errorf("unreachable keyless signature should skip: got (verified=%v, err=%v), want (false, nil)", verified, err)
	}
}

// pubKeySecretFetcher returns a Fetcher whose verify secret carries pub as a
// PEM public key under data.cosign.pub (base64) — the towonel shape.
func pubKeySecretFetcher(t *testing.T, pub crypto.PublicKey) *Fetcher {
	t.Helper()
	pemB64 := base64.StdEncoding.EncodeToString(mustPEMPublicKey(t, pub))
	return &Fetcher{Secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{Data: map[string]any{"cosign.pub": pemB64}}
	}}
}

func cosignVerifyRepo() *manifest.OCIRepository {
	return &manifest.OCIRepository{
		Name: "towonel-agent", Namespace: "network",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			Verify: &manifest.OCIRepositoryVerify{
				Provider:  "cosign",
				SecretRef: &manifest.LocalObjectReference{Name: "towonel-cosign-pub"},
			},
		},
	}
}

// TestVerifyCosignSignature_NoKeysSkips: a verify secret that yields no usable
// public key (here: junk, no PEM) can't complete the check, so it WARNs and
// skips — (false, nil) — without touching the registry (repoClient is nil).
func TestVerifyCosignSignature_NoKeysSkips(t *testing.T) {
	f := &Fetcher{Secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"cosign.pub": "not a key"}}
	}}
	verified, err := f.verifyCosignSignature(context.Background(), nil, cosignVerifyRepo(), "sha256:deadbeef")
	if err != nil || verified {
		t.Errorf("no-keys should skip: got (verified=%v, err=%v), want (false, nil)", verified, err)
	}
}

// TestVerifyCosignSignature_SignatureUnreachableSkips: the key is present but
// the cosign signature isn't in the registry (Resolve 404s) — flate can't
// complete the check, so it skips rather than hard-failing the render.
func TestVerifyCosignSignature_SignatureUnreachableSkips(t *testing.T) {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	f := pubKeySecretFetcher(t, &priv.PublicKey)
	// blobRepoClient serves /blobs/ only; any /manifests/ lookup 404s, so
	// Resolve(sigTag) fails → unreachable → skip.
	rc := blobRepoClient(t, map[string][]byte{})
	verified, err := f.verifyCosignSignature(context.Background(), rc, cosignVerifyRepo(), "sha256:deadbeef")
	if err != nil || verified {
		t.Errorf("unreachable signature should skip: got (verified=%v, err=%v), want (false, nil)", verified, err)
	}
}

// TestVerifyCosignSignature_Verifies: a reachable, matching signature →
// (true, nil). Exercises the full Resolve → Fetch manifest → per-layer verify
// path through a fake registry serving the signature manifest + payload blob.
func TestVerifyCosignSignature_Verifies(t *testing.T) {
	const pulled = "sha256:deadbeef"
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	payload, sig := mustPayload(t, pulled, priv)
	layer, blobs := payloadLayer(payload, sig)
	manBytes, err := json.Marshal(signatureManifest{Layers: []signatureLayer{layer}})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	rc := cosignSigRepoClient(t, cosignSigTag(pulled), manBytes, blobs)
	f := pubKeySecretFetcher(t, &priv.PublicKey)
	verified, err := f.verifyCosignSignature(context.Background(), rc, cosignVerifyRepo(), pulled)
	if err != nil || !verified {
		t.Errorf("matching signature should verify: got (verified=%v, err=%v), want (true, nil)", verified, err)
	}
}

// TestVerifyCosignSignature_DigestMismatchHardFails: a reachable signature
// that binds a DIFFERENT digest is a genuine integrity failure — past the
// transport boundary, so it hard-fails (false, err) rather than skipping.
func TestVerifyCosignSignature_DigestMismatchHardFails(t *testing.T) {
	const pulled = "sha256:deadbeef"
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	payload, sig := mustPayload(t, "sha256:0ther", priv) // binds a different digest
	layer, blobs := payloadLayer(payload, sig)
	manBytes, _ := json.Marshal(signatureManifest{Layers: []signatureLayer{layer}})
	rc := cosignSigRepoClient(t, cosignSigTag(pulled), manBytes, blobs)
	f := pubKeySecretFetcher(t, &priv.PublicKey)
	verified, err := f.verifyCosignSignature(context.Background(), rc, cosignVerifyRepo(), pulled)
	if verified || err == nil || !strings.Contains(err.Error(), "cosign verify failed") {
		t.Errorf("digest mismatch should hard-fail: got (verified=%v, err=%v)", verified, err)
	}
}

func TestParseOCIRepository_VerifyCosign(t *testing.T) {
	doc := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1",
		"kind":       "OCIRepository",
		"metadata":   map[string]any{"name": "o", "namespace": "ns"},
		"spec": map[string]any{
			"url":      "oci://ghcr.io/x/y",
			"interval": "5m",
			"ref":      map[string]any{"tag": "v1"},
			"verify": map[string]any{
				"provider":  "cosign",
				"secretRef": map[string]any{"name": "cosign-key"},
				"matchOIDCIdentity": []any{
					map[string]any{"issuer": "https://accounts.example.com", "subject": "user@example.com"},
				},
			},
		},
	}
	repo, err := manifest.ParseOCIRepository(doc)
	if err != nil {
		t.Fatalf("ParseOCIRepository: %v", err)
	}
	if repo.Verify == nil {
		t.Fatalf("expected Verify to be parsed")
	}
	if repo.Verify.Provider != "cosign" {
		t.Errorf("Provider = %q, want cosign", repo.Verify.Provider)
	}
	if repo.Verify.SecretRef == nil || repo.Verify.SecretRef.Name != "cosign-key" {
		t.Errorf("SecretRef = %+v", repo.Verify.SecretRef)
	}
	if len(repo.Verify.MatchOIDCIdentity) != 1 {
		t.Errorf("MatchOIDCIdentity len = %d", len(repo.Verify.MatchOIDCIdentity))
	}
}

func TestParseOCIRepository_LayerSelector(t *testing.T) {
	doc := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1",
		"kind":       "OCIRepository",
		"metadata":   map[string]any{"name": "o", "namespace": "ns"},
		"spec": map[string]any{
			"url":      "oci://ghcr.io/x/y",
			"interval": "5m",
			"ref":      map[string]any{"tag": "v1"},
			"layerSelector": map[string]any{
				"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
				"operation": "copy",
			},
		},
	}
	repo, err := manifest.ParseOCIRepository(doc)
	if err != nil {
		t.Fatalf("ParseOCIRepository: %v", err)
	}
	if repo.LayerSelector == nil {
		t.Fatalf("expected LayerSelector parsed")
	}
	if got, want := repo.LayerSelector.MediaType, "application/vnd.cncf.helm.chart.content.v1.tar+gzip"; got != want {
		t.Errorf("MediaType = %q, want %q", got, want)
	}
	if got, want := repo.LayerSelector.Operation, "copy"; got != want {
		t.Errorf("Operation = %q, want %q", got, want)
	}
}

func mustPEMPublicKey(t *testing.T, pub crypto.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

// blobRepoClient builds a *remote.Repository whose Blobs().Fetch serves
// the given payload blobs keyed by their sha256 digest. This drives the
// real oras blob-fetch path used by verifyLayerAgainstKeys without a live
// registry, so the per-layer engine (digest binding, key trials) becomes
// unit-testable. httptest.NewTLSServer's self-signed cert pairs with an
// InsecureSkipVerify client, matching how spec.insecure works in fetch.go.
func blobRepoClient(t *testing.T, blobs map[string][]byte) *remote.Repository {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/"):
			return
		case strings.Contains(r.URL.Path, "/blobs/"):
			d := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			body, ok := blobs[d]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Docker-Content-Digest", d)
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)

	repoClient, err := remote.NewRepository(mustURL(t, srv.URL).Host + "/test/repo")
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	repoClient.Client = &auth.Client{
		Client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed test cert
			},
		},
	}
	return repoClient
}

// cosignSigRepoClient builds a *remote.Repository that serves a cosign
// signature manifest (by tag and by digest) plus its payload blobs, so the
// full verifyCosignSignature path — Resolve(sigTag) → Fetch(manifest) →
// per-layer blob fetch — is exercised without a live registry. Modeled on
// startFakeRegistry in fetch_test.go.
func cosignSigRepoClient(t *testing.T, sigTag string, manifestBytes []byte, blobs map[string][]byte) *remote.Repository {
	t.Helper()
	manifestDigest := sha256Digest(manifestBytes)
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		ref := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/"):
			return
		case strings.Contains(r.URL.Path, "/manifests/"):
			if ref != sigTag && ref != manifestDigest {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = w.Write(manifestBytes)
		case strings.Contains(r.URL.Path, "/blobs/"):
			body, ok := blobs[ref]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Docker-Content-Digest", ref)
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)

	repoClient, err := remote.NewRepository(mustURL(t, srv.URL).Host + "/test/repo")
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	repoClient.Client = &auth.Client{
		Client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed test cert
			},
		},
	}
	return repoClient
}

// payloadLayer builds a signature layer whose annotation carries the
// base64 of sig and whose blob digest addresses payload. The returned
// blobs map is suitable for blobRepoClient.
func payloadLayer(payload, sig []byte) (signatureLayer, map[string][]byte) {
	dig := sha256Digest(payload)
	layer := signatureLayer{
		MediaType:   "application/vnd.dev.cosign.simplesigning.v1+json",
		Digest:      dig,
		Size:        int64(len(payload)),
		Annotations: map[string]string{cosignSignatureAnnotation: base64.StdEncoding.EncodeToString(sig)},
	}
	return layer, map[string][]byte{dig: payload}
}

// mustPayload builds the cosign "simple signing" JSON envelope binding the
// given digest, and an ECDSA signature over it under priv.
func mustPayload(t *testing.T, dig string, priv *ecdsa.PrivateKey) (payload, sig []byte) {
	t.Helper()
	var p cosignPayload
	p.Critical.Image.DockerManifestDigest = dig
	payload, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	h := sha256.Sum256(payload)
	sig, err = ecdsa.SignASN1(rand.Reader, priv, h[:])
	if err != nil {
		t.Fatalf("SignASN1: %v", err)
	}
	return payload, sig
}

// TestVerifyLayerAgainstKeys exercises the per-layer verification engine
// directly — paths that are otherwise only reachable through a live
// registry. Each case asserts the (matched, err) contract that the caller
// in verifyCosignSignature depends on for its lastErr precedence.
func TestVerifyLayerAgainstKeys(t *testing.T) {
	const pulled = "sha256:deadbeef"
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	// Happy path: payload binds the pulled digest and a trusted key
	// verifies — matched, no error.
	t.Run("match", func(t *testing.T) {
		payload, sig := mustPayload(t, pulled, priv)
		layer, blobs := payloadLayer(payload, sig)
		rc := blobRepoClient(t, blobs)
		matched, err := verifyLayerAgainstKeys(context.Background(), rc, layer,
			[]crypto.PublicKey{&priv.PublicKey}, pulled)
		if !matched || err != nil {
			t.Fatalf("got (matched=%v, err=%v), want (true, nil)", matched, err)
		}
	})

	// No cosign signature annotation → silently skipped as (false, nil)
	// so the caller's lastErr is left untouched. Touches no registry.
	t.Run("no annotation skips", func(t *testing.T) {
		layer := signatureLayer{Annotations: map[string]string{"other": "x"}}
		matched, err := verifyLayerAgainstKeys(context.Background(), nil, layer,
			[]crypto.PublicKey{&priv.PublicKey}, pulled)
		if matched || err != nil {
			t.Fatalf("got (matched=%v, err=%v), want (false, nil)", matched, err)
		}
	})

	// Empty annotation value is treated identically to a missing one.
	t.Run("empty annotation skips", func(t *testing.T) {
		layer := signatureLayer{Annotations: map[string]string{cosignSignatureAnnotation: ""}}
		matched, err := verifyLayerAgainstKeys(context.Background(), nil, layer,
			[]crypto.PublicKey{&priv.PublicKey}, pulled)
		if matched || err != nil {
			t.Fatalf("got (matched=%v, err=%v), want (false, nil)", matched, err)
		}
	})

	// Malformed base64 in the signature annotation → decode error,
	// before any registry call.
	t.Run("bad base64 signature", func(t *testing.T) {
		layer := signatureLayer{
			Annotations: map[string]string{cosignSignatureAnnotation: "!!!not base64!!!"},
		}
		matched, err := verifyLayerAgainstKeys(context.Background(), nil, layer,
			[]crypto.PublicKey{&priv.PublicKey}, pulled)
		if matched {
			t.Fatalf("matched on bad base64")
		}
		if err == nil || !strings.Contains(err.Error(), "decode signature") {
			t.Fatalf("err = %v, want decode signature", err)
		}
	})

	// Payload commits to a different digest than pulled → digest-mismatch
	// error, no key trial.
	t.Run("digest mismatch", func(t *testing.T) {
		payload, sig := mustPayload(t, "sha256:0ther", priv)
		layer, blobs := payloadLayer(payload, sig)
		rc := blobRepoClient(t, blobs)
		matched, err := verifyLayerAgainstKeys(context.Background(), rc, layer,
			[]crypto.PublicKey{&priv.PublicKey}, pulled)
		if matched {
			t.Fatalf("matched on digest mismatch")
		}
		if err == nil || !strings.Contains(err.Error(), "payload binds digest sha256:0ther, pulled "+pulled) {
			t.Fatalf("err = %v, want payload-binds-digest", err)
		}
	})

	// Payload parses and binds the right digest, but no provided key
	// verifies → key-trial fallthrough returns the last verify error.
	t.Run("no key matches", func(t *testing.T) {
		payload, sig := mustPayload(t, pulled, priv)
		layer, blobs := payloadLayer(payload, sig)
		rc := blobRepoClient(t, blobs)
		matched, err := verifyLayerAgainstKeys(context.Background(), rc, layer,
			[]crypto.PublicKey{&other.PublicKey}, pulled)
		if matched {
			t.Fatalf("matched under wrong key")
		}
		if err == nil || !strings.Contains(err.Error(), "ecdsa verify failed") {
			t.Fatalf("err = %v, want ecdsa verify failed", err)
		}
	})

	// Payload is not valid JSON → parse-payload error.
	t.Run("bad payload json", func(t *testing.T) {
		payload := []byte("not json")
		layer, blobs := payloadLayer(payload, []byte("sig"))
		rc := blobRepoClient(t, blobs)
		matched, err := verifyLayerAgainstKeys(context.Background(), rc, layer,
			[]crypto.PublicKey{&priv.PublicKey}, pulled)
		if matched {
			t.Fatalf("matched on bad payload json")
		}
		if err == nil || !strings.Contains(err.Error(), "parse payload JSON") {
			t.Fatalf("err = %v, want parse payload JSON", err)
		}
	})
}
