package oci

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"

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
	if err := verifyCosignSignatureBytes(&priv.PublicKey, hash[:], sig); err != nil {
		t.Errorf("verify failed for valid signature: %v", err)
	}
	// Flip a byte; verify should fail.
	sig[0] ^= 0x01
	if err := verifyCosignSignatureBytes(&priv.PublicKey, hash[:], sig); err == nil {
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
	if err := verifyCosignSignatureBytes(&priv.PublicKey, hash[:], sig); err != nil {
		t.Errorf("verify failed for valid RSA signature: %v", err)
	}
}

func TestVerifyCosignSignatureBytes_Ed25519(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	digest := sha256.Sum256([]byte("payload"))
	sig := ed25519.Sign(priv, digest[:])
	if err := verifyCosignSignatureBytes(pub, digest[:], sig); err != nil {
		t.Errorf("verify failed for valid Ed25519 signature: %v", err)
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
		Verify: &manifest.OCIRepositoryVerify{
			SecretRef: &manifest.LocalObjectReference{Name: "keys"},
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
		Verify: &manifest.OCIRepositoryVerify{
			SecretRef: &manifest.LocalObjectReference{Name: "missing"},
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
		Verify: &manifest.OCIRepositoryVerify{
			SecretRef: &manifest.LocalObjectReference{Name: "keys"},
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
