package store

import (
	"context"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func TestRecordEmitted_SourceRetainsInvocation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"equal", "changed", "foreign", "withdrawn"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedSourceRetainsInvocation(t, mode)
		})
	}
}

func testRecordEmittedSourceRetainsInvocation(t *testing.T, mode string) {
	t.Helper()
	s := New()
	source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://fixture.invalid/self"}
	owner := &manifest.Kustomization{
		Name:      "root",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: source.Name,
		},
	}
	s.AddObjects([]manifest.BaseManifest{source, owner})
	s.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Key: "input"})
	var supplied ReadSet
	supplied.Init(s, manifest.BootstrapSourceID, nil)
	defer supplied.Retire()
	supplied.Admit(source, manifest.BootstrapSourceID, manifest.NamedResource{})
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		source.Named(): {Declaration: source, Productions: []manifest.Production{
			{Source: manifest.BootstrapSourceID}, {Owner: owner.Named(), Source: source.Named()},
		}},
	}})
	var alias ReadSet
	alias.Init(s, source.Named(), nil)
	defer alias.Retire()
	assert.Equal(t, alias.GetDeclaration(source.Named()), manifest.BaseManifest(source))
	alias.GetArtifact(manifest.BootstrapSourceID, ArtifactContent)
	artifact := (&SourceArtifact{InputAlias: true, FS: fstest.MapFS{}}).WithReads(&alias)
	s.SetArtifact(source.Named(), artifact)
	var reads ReadSet
	reads.Init(s, owner.Named(), nil)
	defer reads.Retire()
	_, _, output := reads.GetEmission(owner.Named())
	defer output.Finish()
	reads.GetArtifact(source.Named(), ArtifactContent)
	output.Children, output.Complete = []Child{{Obj: source}}, true
	publish, done := make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "publish", func(context.Context) {
		<-publish
		s.RecordEmitted(owner.Named(), &output, nil)
		close(done)
	})
	tasks.Go(t.Context(), "observe", func(context.Context) {
		close(publish)
		<-done
	})
	tasks.BlockTillDone()
	assert.Equal(t, tasks.Failures(), 0)
	assert.Equal(t, output.accepted, true)
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, alias.Valid(), true)
	assert.Equal(t, s.GetDeclaration(source.Named()), manifest.BaseManifest(source))

	if mode == "equal" {
		return
	}
	if mode == "foreign" {
		foreign := &manifest.GitRepository{Name: "foreign", Namespace: source.Namespace}
		s.AddObject(foreign)
		s.SetArtifact(foreign.Named(), &SourceArtifact{Key: "foreign", FS: fstest.MapFS{}})
		owner = owner.Clone()
		owner.SourceRef.Name = foreign.Name
		s.AddObject(owner)
	}
	var replacement ReadSet
	replacement.Init(s, owner.Named(), nil)
	defer replacement.Retire()
	_, _, next := replacement.GetEmission(owner.Named())
	defer next.Finish()
	if mode == "withdrawn" {
		next.Children = nil
	} else {
		updated := new(*source)
		if mode == "changed" {
			updated.URL = "https://fixture.invalid/changed"
		}
		next.Children = []Child{{Obj: updated}}
	}
	next.Complete = true
	s.RecordEmitted(owner.Named(), &next, nil)
	assert.Equal(t, next.accepted, true)
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, alias.Valid(), false)
	if mode == "withdrawn" {
		assert.Equal(t, s.GetDeclaration(source.Named()), nil)
	}
}
