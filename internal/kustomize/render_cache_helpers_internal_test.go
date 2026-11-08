package kustomize

import (
	"context"
	"iter"
	"testing"

	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:gochecknoglobals // Accepted-history fixtures must share the identical immutable render input.
var acceptedInput = PrepareShape(&RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec}, nil).Content()

func acceptedEmission(
	tb testing.TB,
	owner manifest.NamedResource,
	receipt *manifest.RenderReceipt,
	children []store.Child,
) *store.Emission {
	tb.Helper()
	emission := &store.Emission{
		Render:      receipt,
		Fingerprint: manifest.Fingerprint(acceptedInput),
		Complete:    true,
		Children:    children,
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

func previousHistory(
	ctx context.Context,
	cache *TreeCache,
	incoming *RenderRequest,
	owner manifest.NamedResource,
) (
	iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
	bool,
) {
	request := *incoming
	_, receipt, found := cache.PreviousRender(ctx, request)
	if !found {
		return nil, false
	}
	content := acceptedInput.(renderContent)
	content.Dependencies = request.DependencySubstitutions
	return cache.RenderHistory(owner, receipt, content)
}

func acceptanceForEmission(owner manifest.NamedResource, emission *store.Emission) RenderAcceptance {
	if emission == nil {
		return RenderAcceptance{}
	}
	var content any
	if emission.Fingerprint != "" {
		content = acceptedInput
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
