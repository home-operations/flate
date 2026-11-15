package kustomize

import (
	"context"
	"iter"
	"testing"

	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func acceptedEmission(
	tb testing.TB,
	owner manifest.NamedResource,
	receipt *manifest.RenderReceipt,
	content any,
) *store.Emission {
	tb.Helper()
	emission := &store.Emission{
		Render:      receipt,
		Fingerprint: manifest.Fingerprint(content),
		Complete:    true,
	}
	st := store.New()
	st.AddObject(
		&manifest.RawObject{
			APIVersion: owner.Group + "/v1",
			Kind:       owner.Kind,
			Name:       owner.Name,
			Namespace:  owner.Namespace,
		},
	)
	controller := base.New(st, task.NewBounded(2))
	if err := controllertest.Emit(tb.Context(), controller, owner, emission); err != nil {
		tb.Fatal(err)
	}
	if emission.Footprint() == nil || !emission.Reads.Valid() {
		tb.Fatal("fixture emission was not accepted with valid reads")
	}
	return emission
}

func (c *TreeCache) previousHistory(
	ctx context.Context,
	incoming *RenderRequest,
	owner manifest.NamedResource,
	content any,
) (
	iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
	bool,
) {
	request := *incoming
	_, receipt, found := c.PreviousRender(ctx, request)
	if !found {
		return nil, false
	}
	return c.RenderHistory(owner, receipt, content)
}

func acceptanceForEmission(owner manifest.NamedResource, emission *store.Emission, content any) RenderAcceptance {
	if emission == nil {
		return RenderAcceptance{}
	}
	if emission.Fingerprint == "" {
		content = nil
	}
	return RenderAcceptance{
		Owner:       owner,
		Receipt:     emission.Render,
		Records:     emission.Footprint(),
		Content:     content,
		Reads:       emission.Reads,
		OutputReads: emission.OutputReads,
	}
}
