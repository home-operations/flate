package store

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_EquivalentCoveragePreservesDeclaration(t *testing.T) {
	t.Parallel()

	s := New()
	source := &manifest.GitRepository{Name: "repo", Namespace: "apps"}
	parsed, err := manifest.ParseDoc(map[string]any{
		"apiVersion": manifest.SourceExtensionsDomain + "/v1beta1",
		"kind":       manifest.KindArtifactGenerator,
		"metadata":   map[string]any{"name": "generator", "namespace": "apps"},
		"spec": map[string]any{"sources": []any{
			map[string]any{"alias": "repo", "kind": manifest.KindGitRepository, "name": source.Name},
		}},
	}, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	owner := parsed.Object.Named()
	bare := owner
	bare.Namespace = ""
	s.AddObject(source)
	s.AddObject(parsed.Object)
	s.SetArtifact(source.Named(), &SourceArtifact{Key: "repo", Content: "head"})
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare: {Productions: []manifest.Production{{
			Owner: owner, Source: source.Named(), Authored: bare, Path: "generator.yaml", File: "generator.yaml",
		}}},
	}})

	artifact := &manifest.ExternalArtifact{Name: "output", Namespace: owner.Namespace}
	output := artifact.Named()
	payload := &SourceArtifact{Key: "output", Content: "first"}
	publish := func(reads *ReadSet, complete bool, payloads map[manifest.NamedResource]*SourceArtifact) {
		t.Helper()
		_, _, emission := reads.GetEmission(owner)
		defer emission.Finish()
		emission.Children = []Child{{Obj: artifact}}
		emission.Complete = complete
		reads.Bind(&emission)
		assert.FatalIf(t, !reads.Valid(), "publication inputs are stale")
		s.RecordEmitted(owner, &emission, payloads)
		assert.FatalIf(t, complete && !emission.accepted, "complete publication was rejected")
	}

	var producer, consumer ReadSet
	producer.Init(s, owner, nil)
	defer producer.Retire()
	consumer.Init(s, output, nil)
	defer consumer.Retire()
	publish(&producer, false, map[manifest.NamedResource]*SourceArtifact{output: payload})
	assert.Equal(t, consumer.GetArtifact(output, ArtifactContent), Artifact(payload))

	publish(&producer, true, nil)
	assert.FatalIf(t, !producer.Valid(), "equivalent completion invalidated the producer declaration")
	assert.Equal(t, consumer.Valid(), true)

	var successor ReadSet
	successor.Init(s, owner, nil)
	defer successor.Retire()
	next := &SourceArtifact{Key: "output", Content: "second"}
	publish(&successor, true, map[manifest.NamedResource]*SourceArtifact{output: next})
	assert.Equal(t, successor.Valid(), true)
	assert.Equal(t, consumer.Valid(), false)
	assert.Equal(t, s.GetArtifact(output, ArtifactContent), Artifact(next))
}
