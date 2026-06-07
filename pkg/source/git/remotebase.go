package git

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/git/mirror"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// FetchRemoteBase fetches a public kustomize remote git base: repoURL at a
// bare, undifferentiated ref (tag, branch, commit, or "" for the default
// branch), anonymously, returning a SourceArtifact whose LocalPath is a
// materialized worktree directory (no .git). It is a lean sibling of
// fetchViaMirror for the one job kustomize remote bases need.
//
// Unlike Fetch — which takes a Flux GitRepository CR with EXCLUSIVE ref
// fields (Tag XOR Branch XOR Commit XOR …) and a NARROW per-field mirror
// fetch — kustomize's ?ref= is a single opaque string with `git checkout
// <ref>` semantics: it may be a tag, branch, or commit SHA and the caller
// cannot know which. So FetchRemoteBase fetches the mirror BROADLY (empty
// FetchPlan → +refs/*:refs/*, all heads+tags+HEAD) and resolves the string
// through resolveRefHash's Name branch, which delegates to go-git
// ResolveRevision — git rev-parse order over refs/tags then refs/heads then
// a hash prefix, peeling annotated tags. One call covers every ref kind with
// no field-guessing; an empty ref resolves to the mirror's HEAD.
//
// Anonymous only (no SecretRef/proxy/TLS/verification/submodules/sparse):
// kustomize remote bases are public bases; an authenticated base belongs in
// a real GitRepository CR. Requires Mirrors and Cache. The materialized
// worktree is cached per (repoURL, ref) in a slot whose ref label is
// namespaced so it never collides with a real GitRepository CR's slot for
// the same URL, and is reused on warm runs via the revision recorded in
// the slot's .flate-meta.json sidecar.
func (f *Fetcher) FetchRemoteBase(ctx context.Context, repoURL, ref string) (*store.SourceArtifact, error) {
	if f.Mirrors == nil || f.Cache == nil {
		return nil, fmt.Errorf("%w: remote git base fetch requires mirror+cache", manifest.ErrInput)
	}
	if repoURL == "" {
		return nil, fmt.Errorf("%w: remote git base missing url", manifest.ErrInput)
	}
	refLabel := cmp.Or(ref, "HEAD")

	// Per-(url, ref) slot, anonymous (authID ""). The ref label is
	// namespaced ("kustomize-base#…") so a remote base never shares a slot
	// with a real GitRepository CR pinned at the same URL+ref — the two have
	// different materialization disciplines (this one is .git-free + marked).
	slot, err := f.Cache.Slot(ctx, repoURL, "kustomize-base#"+refLabel, "")
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", repoURL, err)
	}
	defer slot.Release()

	// Warm-run hit: a committed slot always carries the revision marker
	// (written below, before Commit). A bare ref may name a mutable branch,
	// but kustomize bases are pinned in practice and re-resolution is cheap,
	// so treat any marked slot as a hit and leave the mirror untouched.
	if slot.Exists {
		if rev := readCachedRevision(slot.Path); rev != "" {
			return gitArtifact(repoURL, slot.Path, rev), nil
		}
		// Marker-less slot (legacy / hand-edited) — re-materialize cleanly.
		if err := slot.Refresh(); err != nil {
			return nil, err
		}
	}

	// Broad fetch: empty FetchPlan → mirror falls back to +refs/*:refs/* so
	// refs/tags/*, refs/heads/*, and HEAD are all present for ResolveRevision.
	// go-git accepts file:// URLs directly (same as fetchViaMirror), so no
	// scheme stripping is needed here.
	mirrorRepo, err := f.Mirrors.OpenOrFetch(ctx, repoURL, nil, nil, mirror.FetchPlan{})
	if err != nil {
		return nil, err
	}

	var refPtr *manifest.GitRepositoryRef
	if ref != "" {
		refPtr = &manifest.GitRepositoryRef{Name: ref} // Name → ResolveRevision (rev-parse order)
	}
	hash, err := resolveRefHash(mirrorRepo, refPtr)
	if err != nil {
		return nil, fmt.Errorf("remote git base %s ref %q: %w", repoURL, refLabel, err)
	}

	// Walk the tree at hash into the slot's staging dir. Symlinks materialize
	// as real OS symlinks so a base that follows symlinked components renders
	// like a git checkout; submodules are warn-and-skipped (the bare mirror
	// has no nested object stores).
	if err := gittree.Materialize(ctx, mirrorRepo, hash, slot.Path, gittree.Options{
		OnSubmodule: func(path string) {
			slog.Warn("kustomize remote base: skipping submodule (mirror path does not recurse)",
				"url", repoURL, "path", path)
		},
	}); err != nil {
		return nil, fmt.Errorf("materialize %s at %s: %w", hash, refLabel, err)
	}

	rev := hash.String()
	if err := writeCachedRevision(slot.Path, rev); err != nil {
		return nil, fmt.Errorf("remote git base %s: write revision marker: %w", repoURL, err)
	}
	if err := slot.Commit(); err != nil {
		return nil, fmt.Errorf("remote git base %s: commit slot: %w", repoURL, err)
	}
	return gitArtifact(repoURL, slot.Path, rev), nil
}
