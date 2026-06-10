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
// freshly-pulled image digest, fetches each signature layer, and verifies it
// against spec.verify — keyed (secretRef → trusted public keys) or keyless
// (matchOIDCIdentity → sigstore Fulcio/Rekor roots, see keyless.go). Both
// require at least one layer whose payload binds the pulled digest and whose
// signature passes the configured check. Notation is out of scope (hard error).
//
// Implements the cosign "simple signing" path:
//   - https://github.com/sigstore/cosign/blob/main/specs/SIGNATURE_SPEC.md
//
// verifyCosignSignature reports whether the pulled artifact's cosign signature
// was successfully verified (verified=true), and returns a non-nil error only
// for a GENUINE verification failure.
//
// flate is an offline renderer, not an admission gate, so it distinguishes
// "couldn't complete the check" from "the check failed":
//
//   - Cannot complete (no usable public key, signature not reachable in the
//     registry) → warn + return (false, nil). The artifact still renders,
//     unverified, and the WARN makes the skip visible.
//   - Genuine failure (signature present but its manifest is broken, no layer
//     matches the keys/identity, or the payload binds the wrong digest) →
//     return (false, err) and fail loud.
//
// The boundary is the transport: Resolve + Fetch + ReadAll of the signature
// manifest are "can't reach it" → skip; everything past that point operates
// on bytes flate already holds, so a failure is a real integrity/trust
// problem → hard error.
func (f *Fetcher) verifyCosignSignature(
	ctx context.Context,
	repoClient *remote.Repository,
	repo *manifest.OCIRepository,
	pulledDigest string,
) (verified bool, err error) {
	if repo.Verify == nil {
		return false, nil
	}
	provider := cmp.Or(repo.Verify.Provider, "cosign")
	if provider != "cosign" {
		return false, fmt.Errorf("%s: verify provider %q is not implemented; flate currently supports only %q",
			ociID(repo), provider, "cosign")
	}
	// Keyed (secretRef) loads the trusted public keys up front and skips with a
	// WARN when none are usable; keyless (matchOIDCIdentity) carries no keys and
	// gates per layer against the embedded sigstore roots (see verifyLayer).
	keyless := repo.Verify.SecretRef == nil
	var keys []crypto.PublicKey
	if !keyless {
		var err error
		keys, err = f.loadCosignPublicKeys(repo)
		if err != nil {
			return false, err
		}
		if len(keys) == 0 {
			// The verify secret resolved but yielded no usable public key —
			// wiped by --wipe-secrets, or it carries no PEM public-key material.
			// flate has nothing to verify against, so skip rather than block the
			// render (a public key is not secret; see manifest.parseSecret).
			f.warnSkipVerify(repo, pulledDigest,
				"no public keys in verify secret "+repo.Namespace+"/"+repo.Verify.SecretRef.Name)
			return false, nil
		}
	}

	// Transport boundary — a failure to REACH the signature means flate
	// can't complete the check, so it warns and skips (like keyless).
	sigTag := cosignSigTag(pulledDigest)
	manDesc, err := repoClient.Resolve(ctx, sigTag)
	if err != nil {
		f.warnSkipVerify(repo, pulledDigest, "signature not found in registry ("+sigTag+"): "+err.Error())
		return false, nil
	}
	manReader, err := repoClient.Fetch(ctx, manDesc)
	if err != nil {
		f.warnSkipVerify(repo, pulledDigest, "fetch signature manifest: "+err.Error())
		return false, nil
	}
	manBytes, err := io.ReadAll(manReader)
	_ = manReader.Close()
	if err != nil {
		f.warnSkipVerify(repo, pulledDigest, "read signature manifest: "+err.Error())
		return false, nil
	}

	// Past the transport boundary: flate holds the signature bytes, so any
	// failure from here on is a genuine integrity/trust problem → hard error.
	var sigMan signatureManifest
	if err := json.Unmarshal(manBytes, &sigMan); err != nil {
		return false, fmt.Errorf("%s: parse signature manifest: %w", ociID(repo), err)
	}
	if len(sigMan.Layers) == 0 {
		return false, fmt.Errorf("%s: signature manifest has no layers", ociID(repo))
	}

	var lastErr error
	for _, layer := range sigMan.Layers {
		matched, err := f.verifyLayer(ctx, repoClient, layer, repo, keys, pulledDigest)
		if matched {
			return true, nil
		}
		// A no-signature layer returns (false, nil) and is silently
		// skipped — it must not clobber a real error from a prior layer.
		if err != nil {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no signature layer matched the verify policy")
	}
	return false, fmt.Errorf("%s: cosign verify failed: %w", ociID(repo), lastErr)
}

// warnSkipVerify logs that cosign verification was skipped because flate
// couldn't complete it, and that the artifact renders unverified. WARN (not
// Debug) so an operator who opted into spec.verify SEES the skip — a silent
// skip on a security-gating field is the worst outcome.
func (f *Fetcher) warnSkipVerify(repo *manifest.OCIRepository, digest, reason string) {
	slog.Warn("cosign verification skipped; rendering unverified artifact",
		"ociRepository", repo.Namespace+"/"+repo.Name,
		"digest", digest,
		"reason", reason)
}

// verifyLayer verifies a single signature layer under the repo's configured
// mode: keyed (spec.verify.secretRef) against the trusted public keys, or
// keyless (spec.verify.matchOIDCIdentity) against the embedded sigstore roots
// (see keyless.go). Both share loadSignedPayload, so a layer with no cosign
// signature is silently skipped (false, nil) and a payload that doesn't bind
// the pulled digest is a hard error regardless of mode.
func (f *Fetcher) verifyLayer(
	ctx context.Context,
	repoClient *remote.Repository,
	layer signatureLayer,
	repo *manifest.OCIRepository,
	keys []crypto.PublicKey,
	pulledDigest string,
) (matched bool, err error) {
	if repo.Verify.SecretRef != nil {
		return verifyLayerAgainstKeys(ctx, repoClient, layer, keys, pulledDigest)
	}
	_, payload, ok, err := loadSignedPayload(ctx, repoClient, layer, pulledDigest)
	if !ok || err != nil {
		return false, err
	}
	return f.verifyPayloadKeyless(layer, payload, repo)
}

// loadSignedPayload reads a cosign signature layer's signature and the payload
// blob it commits to: the base64 signature from the layer annotation, then the
// payload, which must bind pulledDigest. ok is false (skip, no error) for a
// layer carrying no cosign signature annotation; a payload binding a different
// digest is a hard error — a signature lifted from another artifact. Shared by
// the keyed and keyless layer paths.
func loadSignedPayload(
	ctx context.Context,
	repoClient *remote.Repository,
	layer signatureLayer,
	pulledDigest string,
) (sig, payload []byte, ok bool, err error) {
	sigB64, has := layer.Annotations[cosignSignatureAnnotation]
	if !has || sigB64 == "" {
		return nil, nil, false, nil
	}
	sig, err = base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, nil, true, fmt.Errorf("decode signature: %w", err)
	}
	payloadReader, err := repoClient.Blobs().Fetch(ctx, descriptorFromLayer(layer))
	if err != nil {
		return nil, nil, true, fmt.Errorf("fetch payload blob: %w", err)
	}
	payload, err = io.ReadAll(payloadReader)
	_ = payloadReader.Close()
	if err != nil {
		return nil, nil, true, fmt.Errorf("read payload blob: %w", err)
	}
	var p cosignPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, nil, true, fmt.Errorf("parse payload JSON: %w", err)
	}
	if p.Critical.Image.DockerManifestDigest != pulledDigest {
		return nil, nil, true, fmt.Errorf("payload binds digest %s, pulled %s",
			p.Critical.Image.DockerManifestDigest, pulledDigest)
	}
	return sig, payload, true, nil
}

// verifyLayerAgainstKeys verifies a single cosign signature layer against the
// trusted public keys. It returns (true, nil) on the first key that verifies;
// (false, err) when the layer is processed but a step errors or no key matches;
// and (false, nil) for a layer carrying no cosign signature annotation, which
// the caller silently skips. First key wins; the returned err is the last
// failure encountered.
func verifyLayerAgainstKeys(
	ctx context.Context,
	repoClient *remote.Repository,
	layer signatureLayer,
	keys []crypto.PublicKey,
	pulledDigest string,
) (matched bool, err error) {
	sig, payload, ok, err := loadSignedPayload(ctx, repoClient, layer, pulledDigest)
	if !ok || err != nil {
		return false, err
	}
	var lastErr error
	for _, k := range keys {
		verr := verifyCosignSignatureBytes(k, payload, sig)
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
// "no trusted keys".
func parsePEMPublicKeys(b []byte) []crypto.PublicKey {
	var keys []crypto.PublicKey
	for {
		block, rest := pem.Decode(b)
		if block == nil {
			break
		}
		switch block.Type {
		case "PUBLIC KEY":
			if k, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
				keys = append(keys, k)
			}
		case "RSA PUBLIC KEY":
			if k, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
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
