package oci

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// fullDigest is a sha256-shaped digest used across the cached-
// digest tests. Real OCI digests are sha256:<64-hex-chars>; the
// readCachedDigest regex requires at least 32 hex chars after the
// algorithm prefix, so test inputs must match the shape that real
// fetches produce.
const fullDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

// TestCachedDigest_Roundtrip covers writeCachedDigest + readCachedDigest:
// the digest written by one survives a re-read on cache hit.
func TestCachedDigest_Roundtrip(t *testing.T) {
	slot := t.TempDir()
	if err := writeCachedDigest(slot, fullDigest); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := readCachedDigest(slot)
	if got != fullDigest {
		t.Errorf("readCachedDigest = %q, want %q", got, fullDigest)
	}
}

// TestReadCachedDigest_MissingReturnsEmpty pins the cache-miss path:
// no .flate-digest file → empty string (signals "no cached digest").
func TestReadCachedDigest_MissingReturnsEmpty(t *testing.T) {
	if got := readCachedDigest(t.TempDir()); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

// TestReadCachedDigest_MalformedTreatedAsMissing pins the
// format-validation contract: a malformed digest in the sidecar (partial,
// garbage, or shorter than the OCI spec minimum) must read as "" so the
// fetcher's cache-hit path resets the slot instead of passing the bad digest
// to cosign (which would produce a misleading "signature not found" failure).
func TestReadCachedDigest_MalformedTreatedAsMissing(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"empty", ""},
		{"no_colon", "abc123"},
		{"too_short_hex", "sha256:abc"},
		{"non_hex", "sha256:Z" + strings.Repeat("Z", 64)},
		{"only_algorithm", "sha256:"},
		{"random_junk", "this is not a digest"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slot := t.TempDir()
			if err := source.WriteSlotMeta(slot, source.SlotMeta{Digest: tc.content}); err != nil {
				t.Fatal(err)
			}
			if got := readCachedDigest(slot); got != "" {
				t.Errorf("malformed digest %q read as %q; want empty", tc.content, got)
			}
		})
	}
}

// TestWriteCachedDigest_AtomicNoPartial pins the atomic-write
// contract: writeCachedDigest must not leave the destination file
// in a partial state at any point. We can't trigger a real crash
// mid-write, but we CAN assert that no .flate-digest-* temp files
// linger after a successful write (those would be created by the
// atomic path and renamed away).
func TestWriteCachedDigest_AtomicNoPartial(t *testing.T) {
	slot := t.TempDir()
	if err := writeCachedDigest(slot, fullDigest); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(slot)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() == source.SlotMetaFile {
			continue
		}
		t.Errorf("unexpected leftover entry in slot after writeCachedDigest: %q (atomic write should have cleaned up the temp)", e.Name())
	}
}

// TestLoadCredentials_ValidJSONLoads covers the happy path with a
// minimal docker config.
func TestLoadCredentials_ValidJSONLoads(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := loadCredentials(config)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if store == nil {
		t.Error("expected non-nil store for valid config")
	}
}

// TestLoadCredentials_EmptyPathFallsBackToDocker covers the
// docker-default lookup arm. Either it succeeds or it gracefully
// returns (nil, nil) when no default is configured — never errors.
func TestLoadCredentials_EmptyPathFallsBackToDocker(t *testing.T) {
	_, err := loadCredentials("")
	if err != nil {
		t.Errorf("empty path should never error; got %v", err)
	}
}

// TestDescriptorFromLayer copies fields verbatim — sanity check the
// mapping shape so a future field-add to signatureLayer doesn't
// silently drop something cosign needs.
func TestDescriptorFromLayer(t *testing.T) {
	l := signatureLayer{
		MediaType: "application/vnd.dev.cosign.simplesigning.v1+json",
		Digest:    "sha256:beef",
		Size:      1024,
	}
	d := descriptorFromLayer(l)
	if d.MediaType != l.MediaType || string(d.Digest) != l.Digest || d.Size != l.Size {
		t.Errorf("descriptor lost fields: %+v from %+v", d, l)
	}
}

// TestVerifyFingerprint_DeterministicAndPolicySensitive pins the
// verify-marker key: identical Verify specs must hash to the same
// fingerprint (so a cache hit re-uses the prior verify), and any
// meaningful policy change (provider, MatchOIDCIdentity, SecretRef)
// must produce a different fingerprint (forcing re-verify). A nil
// Verify hashes to empty — matches readVerifyMarker's empty-on-miss
// return so the cache-hit path's "want == got" check naturally
// requires both ends absent.
func TestVerifyFingerprint_DeterministicAndPolicySensitive(t *testing.T) {
	a := &manifest.OCIRepositoryVerify{Provider: "cosign"}
	b := &manifest.OCIRepositoryVerify{Provider: "cosign"}
	if verifyFingerprint(a) != verifyFingerprint(b) {
		t.Errorf("identical specs hashed differently: %q vs %q",
			verifyFingerprint(a), verifyFingerprint(b))
	}
	// Provider change -> different fingerprint.
	c := &manifest.OCIRepositoryVerify{Provider: "notation"}
	if verifyFingerprint(a) == verifyFingerprint(c) {
		t.Error("Provider change did not affect fingerprint")
	}
	// SecretRef change -> different fingerprint.
	d := &manifest.OCIRepositoryVerify{Provider: "cosign", SecretRef: &manifest.LocalObjectReference{Name: "trusted"}}
	if verifyFingerprint(a) == verifyFingerprint(d) {
		t.Error("SecretRef addition did not affect fingerprint")
	}
	// Nil hashes to empty (matches readVerifyMarker's miss return).
	if got := verifyFingerprint(nil); got != "" {
		t.Errorf("nil Verify fingerprint = %q, want empty", got)
	}
}

// TestVerifyMarker_AtomicRoundtrip pins the writeVerifyMarker /
// readVerifyMarker pair: a well-formed write roundtrips, and the
// final slot contains only the marker file (the atomic-write temp
// must have been renamed away). Mirrors the existing
// TestWriteCachedDigest_AtomicNoPartial discipline.
func TestVerifyMarker_AtomicRoundtrip(t *testing.T) {
	slot := t.TempDir()
	const fp = "abcd1234"
	if err := writeVerifyMarker(slot, fp); err != nil {
		t.Fatalf("writeVerifyMarker: %v", err)
	}
	if got := readVerifyMarker(slot); got != fp {
		t.Errorf("readVerifyMarker = %q, want %q", got, fp)
	}
	// Empty slot path: missing marker should read as "".
	if got := readVerifyMarker(t.TempDir()); got != "" {
		t.Errorf("missing marker read as %q, want empty", got)
	}
	// No temp leftover.
	entries, _ := os.ReadDir(slot)
	for _, e := range entries {
		if e.Name() == source.SlotMetaFile {
			continue
		}
		t.Errorf("unexpected leftover entry %q in slot after writeVerifyMarker", e.Name())
	}
}

// TestSignatureManifestRoundtrip pins the JSON shape — cosign
// signature manifests must unmarshal cleanly into signatureManifest.
func TestSignatureManifestRoundtrip(t *testing.T) {
	raw := `{"layers":[{"mediaType":"application/vnd.dev.cosign.simplesigning.v1+json","digest":"sha256:abc","size":42,"annotations":{"dev.cosignproject.cosign/signature":"MEUC..."}}]}`
	var m signatureManifest
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(m.Layers) != 1 || !strings.Contains(m.Layers[0].Digest, "sha256:") {
		t.Errorf("unexpected layers: %+v", m.Layers)
	}
	if m.Layers[0].Annotations["dev.cosignproject.cosign/signature"] == "" {
		t.Error("annotation roundtrip lost the signature key")
	}
}

// TestSlotMeta_DigestAndVerifyCoexist pins the read-modify-write contract of
// the unified sidecar: writing the digest, then the verify fingerprint (the
// real fetch order), must leave BOTH readable — a writer must never clobber the
// sibling field. A regression here surfaces as a cache hit re-verifying every
// run (digest lost) or, worse, the digest rewrite dropping the fingerprint, so
// it is the load-bearing invariant of the marker unification.
func TestSlotMeta_DigestAndVerifyCoexist(t *testing.T) {
	slot := t.TempDir()
	if err := writeCachedDigest(slot, fullDigest); err != nil {
		t.Fatalf("writeCachedDigest: %v", err)
	}
	if err := writeVerifyMarker(slot, "fp99"); err != nil {
		t.Fatalf("writeVerifyMarker: %v", err)
	}
	if got := readCachedDigest(slot); got != fullDigest {
		t.Errorf("digest lost after verify write: %q", got)
	}
	if got := readVerifyMarker(slot); got != "fp99" {
		t.Errorf("verify lost: %q", got)
	}
	// A re-pull (digest rewrite) must preserve the verify fingerprint.
	if err := writeCachedDigest(slot, fullDigest); err != nil {
		t.Fatalf("rewrite digest: %v", err)
	}
	if got := readVerifyMarker(slot); got != "fp99" {
		t.Errorf("verify clobbered by digest rewrite: %q", got)
	}
}
