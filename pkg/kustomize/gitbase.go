package kustomize

import (
	"context"
	"path"
	"path/filepath"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
)

// fetchGitBase mounts the whole repository so local parent references retain their coordinates.
func (w *inputWalk) fetchGitBase(ctx context.Context, spec kustomizeinput.GitBase, scope walkScope) (string, error) {
	input := w.fs
	descriptor := remoteInput{Operation: remoteGit, URL: spec.RepoURL, Ref: spec.Ref}
	identity := spec.RepoURL + "@" + spec.Ref
	if dest := input.bases[identity]; dest != "" {
		if input.rec != nil {
			w.recordRemote(descriptor, input.rec.remotes[descriptor].identity, scope)
		}
		rel, err := filepath.Rel(scope.root, filepath.Join(dest, spec.SubPath))
		return "./" + filepath.ToSlash(rel), err
	}
	acquired, err := w.acquire(ctx, descriptor, scope)
	if err != nil {
		return "", err
	}
	name := remoteResourcePrefix + manifest.Fingerprint(identity)
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
		input.bases = make(map[string]string)
	}
	input.bases[identity] = dest
	return "./" + path.Join(name, spec.SubPath), nil
}
