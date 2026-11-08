package store

import (
	"context"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_SourceHistorySurvivesAcquisition(t *testing.T) {
	t.Parallel()
	s := New()
	source := projectionSource(t, s, "source")
	root := projectionOwner(s, "root", manifest.BootstrapSourceID)
	s.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{LocalPath: "/input", FS: fstest.MapFS{}})
	input := &manifest.ConfigMap{Name: "authority", Data: map[string]any{"value": "initial"}}
	s.AddObject(input)
	var alias ReadSet
	alias.Init(s, source, nil)
	defer alias.Retire()
	alias.GetObject(input.Named())
	artifact := (&SourceArtifact{InputAlias: true, LocalPath: "/input", FS: fstest.MapFS{}}).WithReads(&alias)
	s.SetArtifact(source, artifact)
	workload := &manifest.Kustomization{
		Name: "workload", Namespace: "apps", Path: "workload",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name},
	}
	children := []Child{{Obj: workload}}
	expected := manifest.SourceEvidence{Source: artifact.HistoryIdentity(source), Path: "workload", Subtree: true}
	assert.Equal(t, slices.Contains(s.ChildSourcePaths(root.Named(), children, nil), expected), true)
	var reads ReadSet
	reads.Init(s, root.Named(), nil)
	defer reads.Retire()
	_, _, output := reads.GetEmission(root.Named())
	defer output.Finish()
	output.Children, output.Complete = children, true
	published, invalidated := make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	tasks.Go(t.Context(), "publish", func(context.Context) {
		s.RecordEmitted(root.Named(), &output, nil)
		close(published)
		<-invalidated
	})
	tasks.Go(t.Context(), "replace", func(context.Context) {
		<-published
		s.AddObject(&manifest.ConfigMap{Name: input.Name, Data: map[string]any{"value": "replacement"}})
		close(invalidated)
	})
	tasks.BlockTillDone()
	assert.Equal(t, tasks.Failures(), 0)
	assert.Equal(t, output.accepted, true)
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, alias.Valid(), false)
	assert.Equal(t, artifact.HistoryIdentity(source), manifest.SourceIdentity{})
	assert.Equal(t, slices.Contains(output.SourcePaths(), expected), true)
	acquired := &SourceArtifact{
		Kind:    source.Kind,
		URL:     "https://fixture.invalid/acquired",
		Key:     "remote",
		Content: "revision",
		FS:      fstest.MapFS{},
	}
	s.SetArtifact(source, acquired)
	assert.Equal(t, slices.Contains(output.SourcePaths(), expected), true)
	var replacement ReadSet
	replacement.Init(s, root.Named(), nil)
	defer replacement.Retire()
	_, _, next := replacement.GetEmission(root.Named())
	defer next.Finish()
	next.Children, next.Complete = children, true
	s.RecordEmitted(root.Named(), &next, nil)
	assert.Equal(t, next.accepted, true)
	current := manifest.SourceEvidence{Source: acquired.HistoryIdentity(source), Path: "workload", Subtree: true}
	assert.Equal(t, slices.Contains(next.SourcePaths(), current), true)
	assert.Equal(t, slices.Contains(next.SourcePaths(), expected), false)
	assert.Equal(t, slices.Contains(output.SourcePaths(), expected), true)
	changed := root.Clone()
	changed.Path = "replacement"
	s.AddObject(changed)
	assert.Equal(t, next.SourcePaths() == nil, true)
}
