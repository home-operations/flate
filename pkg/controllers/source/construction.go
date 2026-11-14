package source

import (
	"context"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Construct reconciles local material before execution membership exists.
// The orchestrator MUST serialize this phase with runtime source reconciliation.
func (c *Controller) Construct(ctx context.Context, id manifest.NamedResource) error {
	return c.construct(ctx, id, true)
}

func (c *Controller) construct(ctx context.Context, id manifest.NamedResource, construction bool) error {
	ex := &execution{Controller: c}
	ex.run.Controller = c.Controller
	ex.run.Reads.Init(c.Store, id, nil)
	defer ex.run.Retire()
	child, selected, err := c.admission.Resolve(ctx, id, nil, &ex.run.Reads)
	obj := child.Obj

	if err != nil {
		return err
	}
	if selected == nil || obj == nil {
		artifact, _ := c.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		if construction && artifact != nil && !artifact.Reads.Valid() {
			c.Store.SetArtifact(id, nil)
		}
		return nil
	}
	if !ex.run.Reads.Valid() {
		return nil
	}
	facts := obj.(sourceFacts) //nolint:errcheck // The source controller accepts only source manifests.
	// Composition gates MUST recapture the admitted declaration before reading input artifacts.
	if len(facts.Composition().Includes) != 0 && ex.run.Reads.GetDeclaration(id) == nil {
		ex.run.Reads.Admit(child, manifest.BootstrapSourceID, manifest.NamedResource{})
		return nil
	}
	if err := ex.completeFetch(ctx, obj, fetchCompletion{artifact: selected, level: manifest.DrainNone}); err != nil {
		return err
	}
	if construction && ex.run.Reads.Valid() {
		c.Store.UpdateStatus(
			id,
			store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSucceeded, Message: "local entry source"},
		)
	}
	return nil
}
