package depwait

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestClassify_RequiredDeclaration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                          string
		declaration, content, promote bool
		status                        store.Status
		level                         manifest.DrainLevel
		want                          schedule.Kind
	}{
		{name: "missing", want: schedule.Blocked},
		{name: "missing at drain", level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "stale content", content: true, want: schedule.Blocked},
		{name: "stale content at drain", content: true, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "stale status and content", status: store.StatusReady, content: true, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "declared pending", declaration: true, want: schedule.Blocked},
		{name: "ready without content", declaration: true, status: store.StatusReady, want: schedule.DependencyFailed},
		{name: "content without ready", declaration: true, content: true, want: schedule.Terminal},
		{name: "content with failed ready", declaration: true, status: store.StatusFailed, content: true, want: schedule.Terminal},
		{name: "failed acquisition", declaration: true, status: store.StatusFailed, want: schedule.DependencyFailed},
		{name: "promote before stale content", promote: true, content: true, want: schedule.Terminal},
		{name: "promote without content", promote: true, want: schedule.Blocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			obj := &manifest.GitRepository{Name: "input", Namespace: "apps"}
			if tc.declaration {
				s.AddObject(obj)
			}
			if tc.content {
				s.SetArtifact(obj.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
			}
			if tc.status != "" {
				s.UpdateStatus(obj.Named(), store.StatusInfo{Status: tc.status, Message: "acquisition failed"})
			}
			reads := captureState(t, s)
			waiter := Waiter{
				Store:     reads,
				Admission: selection.PrepareRequest{Reads: reads},
				Missing:   NewMissingPolicy(nil),
			}
			promotions := 0
			waiter.Existence = &artifactPromotion{promote: func(id manifest.NamedResource) bool {
				promotions++
				assert.Equal(t, id, obj.Named())
				if tc.promote {
					s.AddObject(obj)
					reads.Retire()
					reads.Init(s, manifest.NamedResource{}, nil)
				}
				return tc.promote
			}}
			result := waiter.Classify(t.Context(), manifest.DependencyRef{
				NamedResource: obj.Named(), Artifact: true, ArtifactOnly: true, RequiresDeclaration: true,
			}, tc.level)
			kind := result.Kind
			if result.Err != nil {
				kind = schedule.DependencyFailed
			}
			assert.Equal(t, kind, tc.want)
			wantPromotions := 1
			if tc.declaration {
				wantPromotions = 0
			}
			assert.Equal(t, promotions, wantPromotions)
		})
	}
}

func TestClassify_PromotionInvalidatesCapturedReadiness(t *testing.T) {
	t.Parallel()
	for _, content := range []bool{false, true} {
		name := "without content"
		if content {
			name = "readable content"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{FS: fstest.MapFS{}})
			input := &manifest.GitRepository{Name: "input", Namespace: "apps"}
			if content {
				st.SetArtifact(input.Named(), &store.SourceArtifact{Content: "input", FS: fstest.MapFS{}})
			}
			reads := captureState(t, st)
			waiter := Waiter{
				Store:   reads,
				Missing: NewMissingPolicy(nil),
				Existence: &artifactPromotion{promote: func(id manifest.NamedResource) bool {
					assert.Equal(t, id, input.Named())
					reads.Admit(store.Child{Obj: input}, manifest.BootstrapSourceID, manifest.NamedResource{})
					return st.GetDeclaration(id) == input
				}},
			}
			dependency := manifest.DependencyRef{
				NamedResource:       input.Named(),
				Artifact:            true,
				ArtifactOnly:        true,
				RequiresDeclaration: true,
			}
			reads.Require(dependency)
			waiter.Classify(t.Context(), dependency, manifest.DrainCascade)
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, st.GetDeclaration(input.Named()), manifest.BaseManifest(input))
			assert.Equal(t, reads.GetObject(input.Named()), manifest.BaseManifest(nil))
			st.SetArtifact(input.Named(), &store.SourceArtifact{Content: "input", FS: fstest.MapFS{}})
			st.UpdateStatus(input.Named(), store.StatusInfo{Status: store.StatusReady})
			assert.Equal(t, reads.Valid(), false)
			waiter.Store, waiter.Existence = captureState(t, st), nil
			assert.Equal(t, waiter.Classify(t.Context(), dependency, manifest.DrainCascade).Kind, schedule.Terminal)
			st.DeleteObject(input.Named())
			waiter.Store = captureState(t, st)
			assert.Equal(
				t,
				waiter.Classify(t.Context(), dependency, manifest.DrainCascade).Kind,
				schedule.DependencyFailed,
			)
		})
	}
}
