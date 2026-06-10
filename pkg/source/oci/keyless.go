package oci

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"sync"

	protobundle "github.com/sigstore/protobuf-specs/gen/pb-go/bundle/v1"
	protocommon "github.com/sigstore/protobuf-specs/gen/pb-go/common/v1"
	protorekor "github.com/sigstore/protobuf-specs/gen/pb-go/rekor/v1"
	"github.com/sigstore/sigstore-go/pkg/bundle"
	"github.com/sigstore/sigstore-go/pkg/root"
	"github.com/sigstore/sigstore-go/pkg/verify"

	"github.com/home-operations/flate/pkg/manifest"
)

// Keyless cosign annotations carried on a legacy `.sig` layer alongside the
// dev.cosignproject.cosign/signature the keyed path reads: the Fulcio signing
// certificate and the Rekor transparency-log bundle.
const (
	cosignCertAnnotation   = "dev.sigstore.cosign/certificate"
	cosignBundleAnnotation = "dev.sigstore.cosign/bundle"
	// A v0.1 sigstore bundle requires only an inclusion promise (the Rekor
	// SignedEntryTimestamp), which the legacy `.sig` carries — unlike v0.2+,
	// which require a Merkle inclusion proof cosign does not stash there.
	bundleMediaType01 = "application/vnd.dev.sigstore.bundle+json;version=0.1"
)

// trustedRootJSON is the public-good Sigstore trusted root (Fulcio CAs, Rekor +
// CT log keys, TSA), vendored so keyless verification runs fully offline: the
// only network is the `.sig` fetch the keyed path already does. Refresh from the
// Sigstore TUF repo (`cosign initialize` →
// ~/.sigstore/.../targets/trusted_root.json) if the public-good roots rotate.
//
//go:embed trusted_root.json
var trustedRootJSON []byte

// keylessVerifier builds the sigstore verifier once from the embedded trusted
// root. WithTransparencyLog verifies the Rekor inclusion (the legacy bundle's
// SignedEntryTimestamp promise, validated against the embedded Rekor key);
// WithIntegratedTimestamps then trusts that entry's integrated time to validate
// the short-lived Fulcio certificate; WithSignedCertificateTimestamps enforces
// the cert's embedded SCT against the CT logs. This is the same set cosign uses,
// and is required together — WithTransparencyLog is what actually verifies the
// tlog entry that the integrated-timestamp threshold then counts. The verifier
// is stateless w.r.t. the per-repo identity policy (passed to Verify), so one
// instance serves every reconcile.
var keylessVerifier = sync.OnceValues(func() (*verify.Verifier, error) {
	tr, err := root.NewTrustedRootFromJSON(trustedRootJSON)
	if err != nil {
		return nil, fmt.Errorf("cosign keyless: load embedded trusted root: %w", err)
	}
	return verify.NewVerifier(tr,
		verify.WithTransparencyLog(1),
		verify.WithIntegratedTimestamps(1),
		verify.WithSignedCertificateTimestamps(1),
	)
})

// rekorBundle is the legacy cosign Rekor bundle carried in the
// dev.sigstore.cosign/bundle annotation. encoding/json base64-decodes the
// []byte fields (SignedEntryTimestamp, body) automatically.
type rekorBundle struct {
	SignedEntryTimestamp []byte `json:"SignedEntryTimestamp"`
	Payload              struct {
		Body           []byte `json:"body"`
		IntegratedTime int64  `json:"integratedTime"`
		LogIndex       int64  `json:"logIndex"`
		LogID          string `json:"logID"`
	} `json:"Payload"`
}

// verifyPayloadKeyless verifies one signature layer's payload against the
// configured OIDC identities using sigstore-go. It reconstructs a v0.1 sigstore
// bundle from the legacy material flate already holds — the payload (the signed
// bytes), the signature, the Fulcio certificate, and the Rekor bundle — then
// validates the certificate chain, transparency-log promise, SCT, and identity
// in one call. Returns (true, nil) on success and (false, err) on a genuine
// verification failure (the caller is already past the digest-binding check).
func (f *Fetcher) verifyPayloadKeyless(layer signatureLayer, payload []byte, repo *manifest.OCIRepository) (bool, error) {
	v, err := keylessVerifier()
	if err != nil {
		return false, err
	}
	b, err := legacyBundle(
		layer.Annotations[cosignCertAnnotation],
		layer.Annotations[cosignSignatureAnnotation],
		payload,
		layer.Annotations[cosignBundleAnnotation],
	)
	if err != nil {
		return false, err
	}
	policy, err := identityPolicy(payload, repo.Verify.MatchOIDCIdentity)
	if err != nil {
		return false, err
	}
	if _, err := v.Verify(b, policy); err != nil {
		return false, err
	}
	return true, nil
}

// legacyBundle assembles a v0.1 sigstore bundle from a legacy cosign `.sig`
// layer: the leaf Fulcio certificate (PEM; the trusted root supplies the CA
// chain), the base64 signature, the signed payload, and the Rekor bundle JSON.
// The message signature is over SHA-256(payload), matching the WithArtifact
// the policy feeds the verifier.
func legacyBundle(certPEM, sigB64 string, payload []byte, rekorJSON string) (*bundle.Bundle, error) {
	if certPEM == "" {
		return nil, errors.New("keyless: signature layer carries no Fulcio certificate")
	}
	if rekorJSON == "" {
		return nil, errors.New("keyless: signature layer carries no Rekor bundle")
	}
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		return nil, errors.New("keyless: malformed certificate PEM")
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, fmt.Errorf("keyless: decode signature: %w", err)
	}
	var rb rekorBundle
	if err := json.Unmarshal([]byte(rekorJSON), &rb); err != nil {
		return nil, fmt.Errorf("keyless: parse rekor bundle: %w", err)
	}
	logID, err := hex.DecodeString(rb.Payload.LogID)
	if err != nil {
		return nil, fmt.Errorf("keyless: decode rekor log ID: %w", err)
	}
	kind, version, err := rekorKindVersion(rb.Payload.Body)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(payload)
	pb := &protobundle.Bundle{
		MediaType: bundleMediaType01,
		VerificationMaterial: &protobundle.VerificationMaterial{
			Content: &protobundle.VerificationMaterial_Certificate{
				Certificate: &protocommon.X509Certificate{RawBytes: block.Bytes},
			},
			TlogEntries: []*protorekor.TransparencyLogEntry{{
				LogIndex:          rb.Payload.LogIndex,
				LogId:             &protocommon.LogId{KeyId: logID},
				KindVersion:       &protorekor.KindVersion{Kind: kind, Version: version},
				IntegratedTime:    rb.Payload.IntegratedTime,
				InclusionPromise:  &protorekor.InclusionPromise{SignedEntryTimestamp: rb.SignedEntryTimestamp},
				CanonicalizedBody: rb.Payload.Body,
			}},
		},
		Content: &protobundle.Bundle_MessageSignature{
			MessageSignature: &protocommon.MessageSignature{
				MessageDigest: &protocommon.HashOutput{
					Algorithm: protocommon.HashAlgorithm_SHA2_256,
					Digest:    digest[:],
				},
				Signature: sig,
			},
		},
	}
	return bundle.NewBundle(pb)
}

// rekorKindVersion reads the entry kind and apiVersion from the canonicalized
// Rekor body (e.g. hashedrekord / 0.0.1). sigstore-go cross-checks these against
// the body it parses, so they must reflect the body rather than be assumed.
func rekorKindVersion(body []byte) (kind, version string, err error) {
	var head struct {
		Kind       string `json:"kind"`
		APIVersion string `json:"apiVersion"`
	}
	if err := json.Unmarshal(body, &head); err != nil {
		return "", "", fmt.Errorf("keyless: parse rekor entry body: %w", err)
	}
	if head.Kind == "" || head.APIVersion == "" {
		return "", "", errors.New("keyless: rekor entry body missing kind/apiVersion")
	}
	return head.Kind, head.APIVersion, nil
}

// identityPolicy builds the verification policy: the artifact is the cosign
// payload (the message signature is over it), gated by the OIDC identity
// matchers. Each spec.verify.matchOIDCIdentity entry contributes an
// issuer+subject regex pair; any one matching satisfies the policy. An empty
// matcher set mirrors Flux's "keyless, identity unconstrained" semantics — the
// chain and transparency log still gate, the identity does not.
func identityPolicy(payload []byte, matchers []manifest.OIDCIdentityMatch) (verify.PolicyBuilder, error) {
	artifact := verify.WithArtifact(bytes.NewReader(payload))
	if len(matchers) == 0 {
		return verify.NewPolicy(artifact, verify.WithoutIdentitiesUnsafe()), nil
	}
	opts := make([]verify.PolicyOption, 0, len(matchers))
	for _, m := range matchers {
		id, err := verify.NewShortCertificateIdentity("", m.Issuer, "", m.Subject)
		if err != nil {
			return verify.PolicyBuilder{}, fmt.Errorf("keyless: invalid matchOIDCIdentity {issuer=%q subject=%q}: %w", m.Issuer, m.Subject, err)
		}
		opts = append(opts, verify.WithCertificateIdentity(id))
	}
	return verify.NewPolicy(artifact, opts...), nil
}
