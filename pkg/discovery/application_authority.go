package discovery

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type applicationReader interface {
	GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
}

type applicationAcceptance struct {
	accepted, complete bool
}

func (e *applicationEvidence) ownerAccepts(reader applicationReader) applicationAcceptance {
	declaration := reader.GetDeclaration(e.object.Named())
	if e.owner == (manifest.NamedResource{}) {
		return applicationAcceptance{accepted: manifest.Equal(declaration, e.object), complete: true}
	}
	output, _ := reader.GetArtifact(e.owner, store.ArtifactContent).(*store.Emission)
	if output == nil || !output.Complete || !output.OutputReads.Valid() || len(output.Failures) != 0 {
		return applicationAcceptance{}
	}
	accepted := manifest.Equal(declaration, e.object) &&
		slices.ContainsFunc(output.Children, func(child store.Child) bool {
			return child.Obj != nil && manifest.Equal(child.Obj, e.object)
		})
	return applicationAcceptance{accepted: accepted, complete: true}
}

func (a *Admission) readApplication(
	ctx context.Context,
	reads *store.ReadSet,
	evidence *applicationEvidence,
	candidate *entryResolution,
) (bool, manifest.NamedResource, error) {
	if reads == nil && candidate.git == nil && evidence.owner == (manifest.NamedResource{}) {
		return false, manifest.NamedResource{}, nil
	}
	if reads == nil && candidate.git != nil && !candidate.synthetic {
		if _, matched := a.remotes[normalizeGitURL(candidate.git.URL)]; !matched {
			return false, manifest.NamedResource{}, nil
		}
	}
	if candidate.git != nil && evidence.application.Declaration != nil {
		original := a.sourceCandidate(nil, &ResolveRequest{
			ID: evidence.application.Source, Object: evidence.application.Declaration,
		})
		if original.git != nil && candidate.git.URL != original.git.URL &&
			(normalizeGitURL(candidate.git.URL) == "" ||
				normalizeGitURL(candidate.git.URL) != normalizeGitURL(original.git.URL)) {
			return false, manifest.NamedResource{}, nil
		}
	}
	scope := candidate.work
	key := scope.Identity()
	key.Owner = evidence.object.Named()
	work := scope.Recorder(&key).Scope().RenderWork()
	application, err := kustomize.PrepareApplication(ctx, evidence.application, work)
	if err != nil {
		return false, manifest.NamedResource{}, err
	}
	evidence.application = application
	if application.Source != candidate.object.Named() || !a.appliesEntry(&application, candidate) {
		return false, manifest.NamedResource{}, nil
	}
	var reader applicationReader = a.store
	if reads != nil {
		reader = reads
	}
	acceptance := evidence.ownerAccepts(reader)
	if !acceptance.complete {
		return true, evidence.owner, nil
	}
	return acceptance.accepted, manifest.NamedResource{}, nil
}

func (a *Admission) appliesEntry(application *manifest.EntryApplication, candidate *entryResolution) bool {
	if candidate.object == nil || candidate.input == nil {
		return false
	}
	if candidate.git == nil && !candidate.authored {
		return false
	}
	behavior, ok := candidate.object.(entrySource)
	if !ok {
		return false
	}
	alias, _, _ := behavior.BootstrapAlias(candidate.input.LocalPath)
	if alias == nil {
		return false
	}
	root := candidate.input.LocalPath
	target := filepath.Join(root, manifest.NormalizeClaimBase(application.Path))
	return safepath.Contains(root, target) && candidate.input.OriginPath(target) == candidate.input.OriginPath(a.path)
}

func (a *Admission) entryOwnsSource(
	ctx context.Context,
	reads *store.ReadSet,
	request *ResolveRequest,
	candidate *entryResolution,
) (bool, []manifest.NamedResource, error) {
	objects := request.Witnesses
	if len(objects) == 0 {
		objects = a.declarations
	}
	seen := map[manifest.NamedResource]bool{}
	var evidenceValues []applicationEvidence
	for _, object := range objects {
		application, valid := manifest.ApplicationFor(object)
		if !valid {
			continue
		}
		seen[object.Named()] = true
		evidenceValues = append(evidenceValues, applicationEvidence{
			object: object, application: application, owner: a.store.EmissionOwner(object.Named()),
		})
	}
	for evidence := a.applications.Load(); evidence != nil; evidence = evidence.next {
		if !seen[evidence.object.Named()] {
			evidenceValues = append(evidenceValues, *evidence)
			seen[evidence.object.Named()] = true
		}
	}
	var owners []manifest.NamedResource
	eligible := false
	for i := range evidenceValues {
		accepted, owner, err := a.readApplication(ctx, reads, &evidenceValues[i], candidate)
		if err != nil {
			return false, nil, err
		}
		eligible = eligible || accepted
		if owner != (manifest.NamedResource{}) {
			owners = append(owners, owner)
		}
	}
	return eligible, owners, nil
}

func (a *Admission) waitApplicationOwners(
	ctx context.Context,
	reads *store.ReadSet,
	request *ResolveRequest,
	owners []manifest.NamedResource,
) error {
	if reads == nil || len(owners) == 0 {
		return nil
	}
	slices.SortFunc(owners, manifest.NamedResource.Compare)
	owners = slices.Compact(owners)
	waiter := depwait.Waiter{Store: reads, Parent: request.ID, Materializer: true}
	var blocked, failed []manifest.NamedResource
	reasons := map[manifest.NamedResource]manifest.DiagnosticReason{}
	for _, owner := range owners {
		reads.RequireMaterializer(owner)
		classification := waiter.Classify(ctx, manifest.DependencyRef{NamedResource: owner}, request.Level)
		if classification.Err != nil {
			return classification.Err
		}
		if classification.Kind == schedule.Terminal {
			if request.Level < manifest.DrainForce {
				blocked = append(blocked, owner)
				continue
			}
			classification = depwait.Classification{
				Kind:    schedule.DependencyFailed,
				Message: "accepted application output unavailable",
			}
		}
		if classification.Kind == schedule.DependencyFailed {
			failed = append(failed, owner)
			reasons[owner] = classification.DiagnosticReason(owner)
		} else {
			blocked = append(blocked, owner)
		}
	}
	if len(failed) != 0 {
		return &manifest.DependencyFailedError{Parent: request.ID, Failed: failed, Reasons: reasons}
	}
	if len(blocked) != 0 {
		return &depwait.ErrBlocked{Deps: blocked}
	}
	return nil
}
