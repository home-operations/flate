package manifest

import (
	"slices"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
)

func TestGitRepository_IncludeReferences(t *testing.T) {
	t.Parallel()
	git := &GitRepository{
		Name: "parent", Namespace: "apps",
		SecretRef: &LocalObjectReference{Name: "auth"}, ProxySecretRef: &LocalObjectReference{Name: "proxy"},
		Include: []sourcev1.GitRepositoryInclude{
			{GitRepositoryRef: LocalObjectReference{Name: "input"}},
			{GitRepositoryRef: LocalObjectReference{Name: "input"}, FromPath: "dir/file", ToPath: "exact/file"},
		},
	}
	composition := git.Composition()
	assert.Equal(t, composition.Policy, "source")
	assert.Equal(t, len(composition.Includes), 2)
	assert.Equal(t, composition.Includes[0].FromPath, ".")
	assert.Equal(t, composition.Includes[0].ToPath, "input")
	assert.Equal(t, composition.Includes[1].FromPath, "dir/file")
	assert.Equal(t, composition.Includes[1].ToPath, "exact/file")
	for _, view := range []ReferenceView{ReferenceRelations, ReferenceAll} {
		refs := slices.Collect(References(git, view))
		assert.Equal(t, len(refs), 4)
		assert.Equal(t, git.ReferenceCapacity().References, 4)
		for i, ref := range refs[2:] {
			assert.Equal(
				t,
				ref.NamedResource,
				NamedResource{Group: SourceDomain, Kind: KindGitRepository, Namespace: "apps", Name: "input"},
			)
			assert.Equal(t, ref.Position, i)
			assert.Equal(t, ref.BindsSource(), true)
			assert.Equal(t, ref.ReadsSourceTree(), false)
			assert.Equal(t, ref.Content, true)
			assert.Equal(t, ref.Artifact, true)
			assert.Equal(t, ref.ArtifactOnly, true)
			assert.Equal(t, ref.RequiresDeclaration, true)
			assert.Equal(t, ref.Gate, GateArtifacts)
			assert.Equal(t, ref.ArtifactStages, GateArtifacts)
			assert.Diff(t, ref.Files, []string{composition.Includes[i].FromPath})
			assert.Equal(t, ref.Subtree, true)
			assert.Equal(t, ref.Acquisition, false)
		}
		for stop := range refs {
			count := 0
			References(git, view)(func(ref Reference) bool {
				assert.Equal(t, cmp.Diff(refs[count], ref, cmp.AllowUnexported(Reference{})), "")
				count++
				return count <= stop
			})
			assert.Equal(t, count, stop+1)
		}
	}
	before := git.Content(sourcev1.GitRepositoryRef{Branch: "main"})
	changed := *git
	changed.Include = []sourcev1.GitRepositoryInclude{{GitRepositoryRef: LocalObjectReference{Name: "other"}}}
	assert.Diff(t, changed.Content(sourcev1.GitRepositoryRef{Branch: "main"}), before)
	changed.SecretRef = &LocalObjectReference{Name: "changed-auth"}
	assert.Equal(
		t,
		Fingerprint(changed.Content(sourcev1.GitRepositoryRef{Branch: "main"})) == Fingerprint(before),
		false,
	)
}

type compositionSource struct {
	GitRepository

	facts ArtifactComposition
}

func (s *compositionSource) Composition() ArtifactComposition { return s.facts }

func TestReferences_CompositionOverride(t *testing.T) {
	t.Parallel()
	input := (&GitRepository{
		Namespace: "apps",
		Include:   []sourcev1.GitRepositoryInclude{{GitRepositoryRef: LocalObjectReference{Name: "input"}}},
	}).Composition()
	input.Includes[0].Reference.Kind = KindBucket
	source := &compositionSource{facts: input}
	refs := slices.Collect(References(source, ReferenceAll))
	assert.Equal(t, cmp.Diff([]Reference{input.Includes[0].Reference}, refs, cmp.AllowUnexported(Reference{})), "")
}
