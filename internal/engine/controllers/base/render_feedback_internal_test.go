package base

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestExecution_RenderFeedbackTracksInputOwner(t *testing.T) {
	t.Parallel()
	st := store.New()
	release := &manifest.HelmRelease{Namespace: "apps", Name: "generator"}
	root := &manifest.Kustomization{Namespace: "apps", Name: "sync", Path: "apps"}
	parent := &manifest.Kustomization{Namespace: "apps", Name: "values", Path: "values"}
	values := &manifest.ConfigMap{Namespace: "apps", Name: "generated-values"}
	independent := &manifest.ConfigMap{Namespace: "apps", Name: "independent-values"}
	st.AddObject(release)
	st.AddObject(independent)
	render := &store.Emission{Complete: true, Children: []store.Child{{Obj: root, VisibleGeneration: true}}}
	st.RecordEmitted(release.Named(), render, nil)
	st.RecordEmitted(root.Named(), &store.Emission{Complete: true, Children: []store.Child{{Obj: parent}}}, nil)
	st.RecordEmitted(parent.Named(), &store.Emission{Complete: true, Children: []store.Child{{Obj: values}}}, nil)

	controller := New(st, nil)
	before := &Execution{Controller: controller, childGeneration: map[manifest.NamedResource]preparedGeneration{
		root.Named(): {output: store.ChildDocuments{Children: render.Children}},
	}}
	before.Reads.Init(st, release.Named(), nil)
	t.Cleanup(before.Close)
	before.AcceptRenderFeedback("first", render)
	before.AcceptRenderFeedback("second", render)

	current := &Execution{Controller: controller}
	current.Reads.Init(st, release.Named(), nil)
	t.Cleanup(current.Close)
	current.Take(schedule.Result{Assessment: before})
	assert.Equal(t, before.feedback, nil)
	current.Reads.GetObject(values.Named())
	assert.Equal(t, errors.Is(current.CheckRenderFeedback("first"), manifest.ErrInput), true)
	assert.NoError(t, current.CheckRenderFeedback("second"))

	next := &Execution{Controller: controller}
	next.Reads.Init(st, release.Named(), nil)
	t.Cleanup(next.Close)
	next.Take(schedule.Result{Assessment: current})
	next.Reads.GetObject(independent.Named())
	assert.NoError(t, next.CheckRenderFeedback("first"))
	next.AcceptRenderFeedback("first", render)
	assert.FatalIf(t, next.feedback == nil, "independent rendering lost its accepted history")
	assert.Equal(t, len(next.feedback.seen), 1)

	empty := &store.Emission{Complete: true}
	st.RecordEmitted(release.Named(), empty, nil)
	next.childGeneration = nil
	next.AcceptRenderFeedback("third", empty)
	assert.Equal(t, next.feedback, nil)
}
