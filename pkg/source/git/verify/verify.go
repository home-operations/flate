// Package verify performs PGP signature verification against a
// freshly cloned GitRepository's HEAD commit and/or referenced tag,
// matching source-controller's spec.verify behavior.
package verify

import (
	"fmt"
	"strings"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// GitVerificationMode is the Flux GitVerificationMode type re-exported
// so callers do not need to import sourcev1 just to call Signatures.
type GitVerificationMode = sourcev1.GitVerificationMode

// Signatures applies PGP verification for the given namespace/name owner,
// looking up the keyring secret secretRefName in ns, applying the given
// mode, and (for tag/tagAndHEAD modes) verifying the annotated tag tagName.
// Pass tagName="" when mode does not require it. Returns nil when mode is
// unrecognised/empty (i.e. no verification configured).
//
// Fails loud on any failure — missing secret, malformed keys,
// unsigned/badly-signed object.
//
// The Secret named by secretRefName may carry multiple ASCII-armored public
// keys (any *.asc filename); they're concatenated into a single keyring
// before verification.
func Signatures(secrets source.SecretGetter, ns, name, secretRefName string, mode GitVerificationMode, tagName string, cloned *git.Repository, resolvedRef plumbing.Hash) error {
	if !matchesHEAD(mode) && !matchesTag(mode) {
		return nil
	}
	if secretRefName == "" {
		return fmt.Errorf("GitRepository %s/%s: spec.verify.secretRef is required",
			ns, name)
	}
	if secrets == nil {
		return fmt.Errorf("GitRepository %s/%s: spec.verify set but no source.SecretGetter is wired",
			ns, name)
	}
	sec := secrets(ns, secretRefName)
	if sec == nil {
		return fmt.Errorf("GitRepository %s/%s: verify secret %s/%s not found",
			ns, name, ns, secretRefName)
	}
	keyring, err := buildPGPKeyring(sec)
	if err != nil {
		return fmt.Errorf("GitRepository %s/%s: %w", ns, name, err)
	}

	if matchesHEAD(mode) {
		if err := verifyCommit(cloned, resolvedRef, keyring); err != nil {
			return fmt.Errorf("GitRepository %s/%s: HEAD verify: %w",
				ns, name, err)
		}
	}
	if matchesTag(mode) {
		if tagName == "" {
			return fmt.Errorf("GitRepository %s/%s: verify mode %q requires spec.ref.tag",
				ns, name, mode)
		}
		if err := verifyTagObject(cloned, tagName, keyring); err != nil {
			return fmt.Errorf("GitRepository %s/%s: tag verify: %w",
				ns, name, err)
		}
	}
	return nil
}

func matchesHEAD(mode sourcev1.GitVerificationMode) bool {
	return mode == sourcev1.ModeGitHEAD || mode == sourcev1.ModeGitTagAndHEAD
}

func matchesTag(mode sourcev1.GitVerificationMode) bool {
	return mode == sourcev1.ModeGitTag || mode == sourcev1.ModeGitTagAndHEAD
}

// buildPGPKeyring concatenates every string value in the Secret into
// one armored keyring. Each value is expected to be an armored PGP
// public key block; the helper doesn't validate the shape — go-git's
// Commit/Tag .Verify rejects malformed keyrings.
//
// Treats source.StringFromSecret's PLACEHOLDER wipe as missing so a
// --wipe-secrets run doesn't try to verify against a placeholder.
func buildPGPKeyring(sec *manifest.Secret) (string, error) {
	seen := make(map[string]struct{}, len(sec.StringData)+len(sec.Data))
	var b strings.Builder
	needsNL := false // true when previous block didn't end with '\n'
	add := func(k string) {
		if _, dup := seen[k]; dup {
			return // StringData wins over Data per StringFromSecret
		}
		seen[k] = struct{}{}
		v := source.StringFromSecret(sec, k)
		if v == "" {
			return
		}
		if needsNL {
			b.WriteByte('\n')
		}
		b.WriteString(v)
		needsNL = v[len(v)-1] != '\n'
	}
	for k := range sec.StringData {
		add(k)
	}
	for k := range sec.Data {
		add(k)
	}
	if b.Len() == 0 {
		return "", fmt.Errorf("verify secret carries no PGP public keys")
	}
	return b.String(), nil
}

func verifyCommit(repo *git.Repository, hash plumbing.Hash, keyring string) error {
	c, err := repo.CommitObject(hash)
	if err != nil {
		return fmt.Errorf("read commit %s: %w", hash, err)
	}
	if c.PGPSignature == "" {
		return fmt.Errorf("commit %s is not signed", hash)
	}
	if _, err := c.Verify(keyring); err != nil {
		return fmt.Errorf("commit %s signature: %w", hash, err)
	}
	return nil
}

func verifyTagObject(repo *git.Repository, name, keyring string) error {
	ref, err := repo.Tag(name)
	if err != nil {
		return fmt.Errorf("resolve tag %q: %w", name, err)
	}
	tag, err := repo.TagObject(ref.Hash())
	if err != nil {
		// Not an annotated tag — only annotated tags carry
		// PGP signatures.
		return fmt.Errorf("tag %q is not annotated (no signature to verify)", name)
	}
	if tag.PGPSignature == "" {
		return fmt.Errorf("tag %q is not signed", name)
	}
	if _, err := tag.Verify(keyring); err != nil {
		return fmt.Errorf("tag %q signature: %w", name, err)
	}
	return nil
}
