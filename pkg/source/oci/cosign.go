package oci

import (
	"cmp"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// cosignSignatureAnnotation is the OCI annotation cosign uses to
// embed the base64-encoded signature on each "signature layer".
const cosignSignatureAnnotation = "dev.cosignproject.cosign/signature"

// cosignPayload is the JSON envelope cosign signs. The "image"
// stanza ties the signature to a specific image digest so an
// attacker can't lift a signature off an unrelated artifact.
type cosignPayload struct {
	Critical struct {
		Image struct {
			DockerManifestDigest string `json:"docker-manifest-digest"`
		} `json:"image"`
	} `json:"critical"`
}

// signatureLayer is the subset of an OCI manifest layer that
// cosign verification needs.
type signatureLayer struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations"`
}

// descriptorFromLayer builds an OCI descriptor suitable for
// oras-go's Blobs().Fetch from a parsed signature manifest layer.
func descriptorFromLayer(l signatureLayer) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType: l.MediaType,
		Digest:    digest.Digest(l.Digest),
		Size:      l.Size,
	}
}

// signatureManifest is the cosign signature artifact's OCI manifest.
type signatureManifest struct {
	Layers []signatureLayer `json:"layers"`
}

// verifyCosignSignature locates the cosign signature artifact for the
// freshly-pulled image digest, fetches each signature layer, and
// verifies it against the trusted public keys carried in
// spec.verify.secretRef. Returns nil on success — at least one layer's
// signature must verify under at least one trusted key AND its payload
// must reference the same digest we just pulled.
//
// Implements the cosign "simple signing" keyed verification path:
//   - https://github.com/sigstore/cosign/blob/main/specs/SIGNATURE_SPEC.md
//
// Keyless (Fulcio / Rekor) verification is logged and skipped: real Flux
// proves the chain via sigstore-go + transparency-log roots, which costs
// ~100 MB of crypto dependencies and online Fulcio/Rekor calls. flate's
// purpose is rendering what Flux would render, not gating artifact
// pulls — so the chart is fetched and rendered, with a warn-level log
// making the unverified state visible to the user.
// Notation is also out of scope.
func (f *Fetcher) verifyCosignSignature(
	ctx context.Context,
	repoClient *remote.Repository,
	repo *manifest.OCIRepository,
	pulledDigest string,
) error {
	if repo.Verify == nil {
		return nil
	}
	provider := cmp.Or(repo.Verify.Provider, "cosign")
	if provider != "cosign" {
		return fmt.Errorf("OCIRepository %s/%s: verify provider %q is not implemented; flate currently supports only %q",
			repo.Namespace, repo.Name, provider, "cosign")
	}
	if repo.Verify.SecretRef == nil {
		// Keyless (OIDC) — flate can't reach Fulcio/Rekor offline and
		// doesn't carry the sigstore trust roots. Log and proceed so
		// the chart still renders for diff purposes. WARN (not Debug)
		// so an operator who deliberately opted into spec.verify can
		// SEE that flate skipped the verification — silent skip on a
		// security-gating spec field is the worst-of-both-worlds
		// outcome.
		slog.Warn("cosign keyless verification skipped; rendering unverified artifact",
			"ociRepository", repo.Namespace+"/"+repo.Name,
			"digest", pulledDigest)
		return nil
	}
	keys, err := f.loadCosignPublicKeys(repo)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("OCIRepository %s/%s: secret %s/%s contains no PEM-encoded public keys",
			repo.Namespace, repo.Name, repo.Namespace, repo.Verify.SecretRef.Name)
	}

	sigTag := cosignSigTag(pulledDigest)
	manDesc, err := repoClient.Resolve(ctx, sigTag)
	if err != nil {
		return fmt.Errorf("OCIRepository %s/%s: locate cosign signature %s: %w",
			repo.Namespace, repo.Name, sigTag, err)
	}
	manReader, err := repoClient.Fetch(ctx, manDesc)
	if err != nil {
		return fmt.Errorf("OCIRepository %s/%s: fetch signature manifest: %w",
			repo.Namespace, repo.Name, err)
	}
	manBytes, err := io.ReadAll(manReader)
	_ = manReader.Close()
	if err != nil {
		return fmt.Errorf("OCIRepository %s/%s: read signature manifest: %w",
			repo.Namespace, repo.Name, err)
	}
	var sigMan signatureManifest
	if err := json.Unmarshal(manBytes, &sigMan); err != nil {
		return fmt.Errorf("OCIRepository %s/%s: parse signature manifest: %w",
			repo.Namespace, repo.Name, err)
	}
	if len(sigMan.Layers) == 0 {
		return fmt.Errorf("OCIRepository %s/%s: signature manifest has no layers",
			repo.Namespace, repo.Name)
	}

	var lastErr error
	for _, layer := range sigMan.Layers {
		matched, err := verifyLayerAgainstKeys(ctx, repoClient, layer, keys, pulledDigest)
		if matched {
			return nil
		}
		// A no-signature layer returns (false, nil) and is silently
		// skipped — it must not clobber a real error from a prior layer.
		if err != nil {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no signature layer matched any trusted key")
	}
	return fmt.Errorf("OCIRepository %s/%s: cosign verify failed: %w",
		repo.Namespace, repo.Name, lastErr)
}

// verifyLayerAgainstKeys verifies a single cosign signature layer against
// the trusted public keys. It returns (true, nil) on the first key that
// verifies; (false, err) when the layer is processed but a step errors or
// no key matches; and (false, nil) for a layer carrying no cosign
// signature annotation, which the caller silently skips.
//
// Iteration short-circuits on the first matching key — first key wins —
// exactly as the inline loop did, so error precedence is unchanged: the
// returned err is the last failure encountered while processing the layer.
func verifyLayerAgainstKeys(
	ctx context.Context,
	repoClient *remote.Repository,
	layer signatureLayer,
	keys []crypto.PublicKey,
	pulledDigest string,
) (matched bool, err error) {
	sigB64, ok := layer.Annotations[cosignSignatureAnnotation]
	if !ok || sigB64 == "" {
		return false, nil
	}
	sigBytes, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return false, fmt.Errorf("decode signature: %w", err)
	}
	// Fetch the payload blob (the signed bytes).
	payloadReader, err := repoClient.Blobs().Fetch(ctx, descriptorFromLayer(layer))
	if err != nil {
		return false, fmt.Errorf("fetch payload blob: %w", err)
	}
	payload, err := io.ReadAll(payloadReader)
	_ = payloadReader.Close()
	if err != nil {
		return false, fmt.Errorf("read payload blob: %w", err)
	}
	// The payload must commit to the digest we pulled.
	var p cosignPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return false, fmt.Errorf("parse payload JSON: %w", err)
	}
	if p.Critical.Image.DockerManifestDigest != pulledDigest {
		return false, fmt.Errorf("payload binds digest %s, pulled %s",
			p.Critical.Image.DockerManifestDigest, pulledDigest)
	}
	// Try every trusted key — first success wins.
	var lastErr error
	for _, k := range keys {
		verr := verifyCosignSignatureBytes(k, payload, sigBytes)
		if verr == nil {
			return true, nil
		}
		lastErr = verr
	}
	return false, lastErr
}

// loadCosignPublicKeys resolves spec.verify.secretRef and parses every
// value containing PEM-encoded public-key blocks. Any value whose
// content does not look like a PEM key is ignored — matches Flux's
// "all *.pub keys in the secret are trusted" semantics.
func (f *Fetcher) loadCosignPublicKeys(repo *manifest.OCIRepository) ([]crypto.PublicKey, error) {
	if f.Secrets == nil {
		return nil, fmt.Errorf("OCIRepository %s/%s: spec.verify.secretRef set but no source.SecretGetter is wired",
			repo.Namespace, repo.Name)
	}
	sec := f.Secrets(repo.Namespace, repo.Verify.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr("OCIRepository",
			repo.Namespace, repo.Name, repo.Verify.SecretRef.Name, "verify secret not found")
	}
	// Iterate the union of StringData + Data keys so PEM blocks stored
	// under either field are discovered. k8s Secrets typically put
	// `cosign.pub` under `data:` (base64-encoded); StringData is the
	// at-apply-time convenience field. The seen set deduplicates keys
	// present in both fields; StringFromSecret prefers StringData.
	seen := make(map[string]struct{}, len(sec.StringData)+len(sec.Data))
	for k := range sec.StringData {
		seen[k] = struct{}{}
	}
	for k := range sec.Data {
		seen[k] = struct{}{}
	}
	keys := make([]crypto.PublicKey, 0, len(seen))
	for k := range seen {
		s := source.StringFromSecret(sec, k)
		if s == "" {
			continue
		}
		keys = append(keys, parsePEMPublicKeys([]byte(s))...)
	}
	return keys, nil
}

// parsePEMPublicKeys decodes every PUBLIC KEY block in the buffer.
// Accepts both PKIX (SubjectPublicKeyInfo) and bare RSA PUBLIC KEY blocks.
// Returns an empty slice when nothing parses; callers treat that as
// "no trusted keys" and fail loud upstream.
func parsePEMPublicKeys(b []byte) []crypto.PublicKey {
	var keys []crypto.PublicKey
	for {
		block, rest := pem.Decode(b)
		if block == nil {
			break
		}
		switch block.Type {
		case "PUBLIC KEY":
			k, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err == nil {
				keys = append(keys, k)
			}
		case "RSA PUBLIC KEY":
			k, err := x509.ParsePKCS1PublicKey(block.Bytes)
			if err == nil {
				keys = append(keys, k)
			}
		}
		b = rest
	}
	return keys
}

// verifyCosignSignatureBytes verifies sig over payload using key.
// Supports the three algorithms cosign's keyed mode emits:
//   - ECDSA-P256-SHA256 (default): verify SHA-256(payload).
//   - RSA-PKCS1v15-SHA256: verify SHA-256(payload).
//   - Ed25519: verify the RAW payload — ed25519.Verify hashes
//     internally via the algorithm's PureEdDSA mode, so feeding it a
//     pre-computed digest (as flate previously did) is wrong and
//     fails for every legitimate signature. Cosign's keyed mode signs
//     the raw payload bytes; verification must match.
func verifyCosignSignatureBytes(key crypto.PublicKey, payload, sig []byte) error {
	switch k := key.(type) {
	case *ecdsa.PublicKey:
		h := sha256.Sum256(payload)
		if !ecdsa.VerifyASN1(k, h[:], sig) {
			return errors.New("ecdsa verify failed")
		}
		return nil
	case *rsa.PublicKey:
		h := sha256.Sum256(payload)
		return rsa.VerifyPKCS1v15(k, crypto.SHA256, h[:], sig)
	case ed25519.PublicKey:
		if !ed25519.Verify(k, payload, sig) {
			return errors.New("ed25519 verify failed")
		}
		return nil
	default:
		return fmt.Errorf("unsupported public key type %T", key)
	}
}

// cosignSigTag converts an artifact digest "sha256:abc..." into the
// signature lookup tag "sha256-abc....sig" that cosign publishes.
func cosignSigTag(digest string) string {
	d := strings.ReplaceAll(digest, ":", "-")
	return d + ".sig"
}
