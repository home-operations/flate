package discovery

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type entryAuthorityFixture struct {
	store     *store.Store
	admission *Admission
	source    *manifest.GitRepository
	owner     *manifest.Kustomization
	app       *manifest.RawObject
	output    store.Emission
}

func newEntryAuthorityFixture(t *testing.T) *entryAuthorityFixture {
	t.Helper()
	const root = "/virtual/application-continuation"
	const url = "https://fixture.invalid/self"
	st := store.New()
	source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: url}
	source.Reference = &sourcev1.GitRepositoryRef{Name: "refs/heads/unavailable"}
	owner := &manifest.Kustomization{Name: "generator", Namespace: "apps", Path: "./generator"}
	app := &manifest.RawObject{
		APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance", Name: "flux", Namespace: "apps",
		Spec: map[string]any{"sync": map[string]any{
			"kind": manifest.KindGitRepository, "name": source.Name, "url": url,
			"ref": "refs/heads/unavailable", "path": ".",
		}},
	}
	st.AddObjects([]manifest.BaseManifest{source, owner})
	reads := new(store.ReadSet)
	reads.Init(st, owner.Named(), nil)
	t.Cleanup(reads.Close)
	_, _, output := reads.GetEmission(owner.Named())
	output.Children, output.Complete = []store.Child{{Obj: app}}, true
	st.RecordEmitted(owner.Named(), &output, nil)
	t.Cleanup(output.Finish)
	admission := &Admission{
		store: st, path: root,
		input:   &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}},
		remotes: map[string]struct{}{normalizeGitURL(url): {}},
	}
	admission.AddApplication(st, app)
	return &entryAuthorityFixture{
		store:     st,
		admission: admission,
		source:    source,
		owner:     owner,
		app:       app,
		output:    output,
	}
}

func TestResolve_AcceptedApplicationBeforeNotification(t *testing.T) {
	t.Parallel()
	for _, evidence := range []string{"declaration", "ancestor notification"} {
		t.Run(evidence, func(t *testing.T) {
			t.Parallel()
			fixture := newEntryAuthorityFixture(t)
			if evidence == "declaration" {
				fixture.admission.applications.Store(nil)
				fixture.admission.declarations = []manifest.BaseManifest{fixture.app}
			}
			var reads store.ReadSet
			reads.Init(fixture.store, fixture.source.Named(), nil)
			defer reads.Close()
			resolved, err := fixture.admission.Resolve(t.Context(), &reads, ResolveRequest{
				ID: fixture.source.Named(), Object: fixture.source,
			})
			assert.NoError(t, err)
			artifact, err := resolved.Resolve(t.Context(), manifest.DrainNone)
			assert.NoError(t, err)
			assert.FatalIf(t, artifact == nil || !artifact.InputAlias,
				"accepted application did not authorize the working tree before notification")
			assert.Equal(t, reads.Valid(), true)
		})
	}
}

func TestResolve_EntryOwnerRebinding(t *testing.T) {
	t.Parallel()
	fixture := newEntryAuthorityFixture(t)
	replacement := fixture.owner.Clone()
	replacement.Path = "./replacement"
	fixture.store.AddObject(replacement)
	assert.Equal(t, fixture.output.OutputReads.Valid(), false)
	var pending store.ReadSet
	pending.Init(fixture.store, fixture.source.Named(), nil)
	defer pending.Close()
	resolved, err := fixture.admission.Resolve(t.Context(), &pending, ResolveRequest{
		ID: fixture.source.Named(), Object: fixture.source,
	})
	assert.NoError(t, err)
	artifact, err := resolved.Resolve(t.Context(), manifest.DrainNone)
	blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
	assert.FatalIf(t, !ok, "source did not wait for accepted owner output: %v", err)
	assert.Diff(t, blocked.Deps, []manifest.NamedResource{fixture.owner.Named()})
	assert.Equal(t, artifact, (*store.SourceArtifact)(nil))
	fixture.store.RecordEmitted(fixture.owner.Named(), &store.Emission{
		Children: fixture.output.Children, Complete: true,
	}, nil)
	assert.Equal(t, pending.Valid(), false)
	_, err = resolved.Resolve(t.Context(), manifest.DrainNone)
	_, blockedAgain := errors.AsType[*depwait.ErrBlocked](err)
	assert.Equal(t, blockedAgain, true)
	var accepted store.ReadSet
	accepted.Init(fixture.store, fixture.source.Named(), nil)
	defer accepted.Close()
	_, _, attempt := fixture.store.GetEmission(fixture.owner.Named())
	assert.Equal(t, attempt.Complete, false)
	attempt.Finish()
	resolved, err = fixture.admission.Resolve(t.Context(), &accepted, ResolveRequest{
		ID: fixture.source.Named(), Object: fixture.source,
	})
	assert.NoError(t, err)
	artifact, err = resolved.Resolve(t.Context(), manifest.DrainNone)
	assert.NoError(t, err)
	assert.FatalIf(t, artifact == nil || !artifact.InputAlias, "accepted output did not restore local source authority")
	assert.Equal(t, accepted.Valid(), true)
}

func TestResolve_EntryOwnerTerminal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		status  store.Status
		level   manifest.DrainLevel
		reason  string
		removed bool
	}{
		{name: "failed", status: store.StatusFailed, reason: "owner failed"},
		{name: "ready before drain", status: store.StatusReady},
		{name: "ready at force", status: store.StatusReady, level: manifest.DrainForce, reason: "accepted application output unavailable"},
		{name: "pending at force", status: store.StatusPending, level: manifest.DrainForce, reason: "not ready"},
		{name: "absent at force", level: manifest.DrainForce, reason: "dependency not found"},
		{name: "application removed", removed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newEntryAuthorityFixture(t)
			replacement := fixture.owner.Clone()
			replacement.Path = "./replacement"
			fixture.store.AddObject(replacement)
			if tc.status != "" {
				fixture.store.UpdateStatus(
					replacement.Named(),
					store.StatusInfo{Status: tc.status, Message: "owner failed"},
				)
			} else if !tc.removed {
				fixture.store.DeleteObject(replacement.Named())
			}
			if tc.removed {
				fixture.store.RecordEmitted(replacement.Named(), &store.Emission{Complete: true}, nil)
			}
			var reads store.ReadSet
			reads.Init(fixture.store, fixture.source.Named(), nil)
			defer reads.Close()
			resolved, err := fixture.admission.Resolve(t.Context(), &reads, ResolveRequest{
				ID: fixture.source.Named(), Object: fixture.source, Level: tc.level,
			})
			assert.NoError(t, err)
			artifact, err := resolved.Resolve(t.Context(), tc.level)
			assert.Equal(t, artifact, (*store.SourceArtifact)(nil))
			if tc.removed {
				assert.NoError(t, err)
				return
			}
			if tc.reason == "" {
				blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
				assert.FatalIf(t, !ok, "expected owner wait, got %v", err)
				assert.Diff(t, blocked.Deps, []manifest.NamedResource{fixture.owner.Named()})
				return
			}
			failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
			assert.FatalIf(t, !ok, "expected dependency failure, got %v", err)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			assert.Diff(t, failed.Failed, []manifest.NamedResource{fixture.owner.Named()})
			assert.Equal(t, failed.Reasons[fixture.owner.Named()].Message, tc.reason)
		})
	}
}

func TestResolve_ApplicationReplacedBeforeCapture(t *testing.T) {
	t.Parallel()
	fixture := newEntryAuthorityFixture(t)
	var reads store.ReadSet
	reads.Init(fixture.store, fixture.source.Named(), nil)
	defer reads.Close()
	enumerated, replaced := make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "capture", func(ctx context.Context) {
		evidence := *fixture.admission.applications.Load()
		close(enumerated)
		tasks.YieldSlot(func() { <-replaced })
		candidate := fixture.admission.sourceCandidate(&reads, &ResolveRequest{
			ID: fixture.source.Named(), Object: fixture.source,
		})
		accepted, pending, err := fixture.admission.readApplication(ctx, &reads, &evidence, &candidate)
		assert.NoError(t, err)
		assert.Equal(t, accepted, true)
		assert.Equal(t, pending, fixture.owner.Named())
		assert.Equal(t, evidence.ownerAccepts(&reads).accepted, false)
	})
	tasks.Go(t.Context(), "replace", func(context.Context) {
		tasks.YieldSlot(func() { <-enumerated })
		replacement := fixture.owner.Clone()
		replacement.Path = "./replacement"
		fixture.store.AddObject(replacement)
		close(replaced)
	})
	tasks.BlockTillDone()
	assert.Equal(t, tasks.Failures(), int64(0))
}
