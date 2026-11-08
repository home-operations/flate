package kustomize

import (
	"context"
	"path"
	"path/filepath"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
)

// fetchGitBase mounts the whole repository so local parent references retain their coordinates.
func (w *inputWalk) fetchGitBase(
	ctx context.Context,
	spec kustomizeinput.GitBase,
	incoming *walkScope,
) (string, error) {
	scope := *incoming
	input := w.fs
	descriptor := remoteInput{
		Operation:  remoteGit,
		URL:        spec.RepoURL,
		Ref:        spec.Ref,
		Timeout:    spec.Timeout,
		Submodules: spec.Submodules,
	}
	if dest := input.bases[descriptor]; dest != "" {
		if input.rec != nil {
			identity := input.rec.remotes[descriptor].identity
			w.recordRemote(descriptor, &identity, &scope)
		}
		rel, err := filepath.Rel(scope.root, filepath.Join(dest, spec.SubPath))
		//nolint:wrapcheck // Git-base path resolution must preserve the original filepath diagnostic.
		return "./" + filepath.ToSlash(rel), err
	}
	acquired, err := w.acquire(ctx, descriptor, &scope)
	if err != nil {
		return "", err
	}
	name := remoteResourcePrefix + manifest.Fingerprint(descriptor)
	dest := filepath.Join(scope.root, name)
	rel, err := input.rel(dest)
	if err != nil {
		return "", err
	}
	if err := input.upper.Mount(filepath.ToSlash(rel), acquired); err != nil {
		return "", err
	}
	input.origins = append(input.origins, mountOrigin{path: filepath.ToSlash(rel)})
	if input.bases == nil {
		input.bases = make(map[remoteInput]string)
	}
	input.bases[descriptor] = dest
	return "./" + path.Join(name, spec.SubPath), nil
}
