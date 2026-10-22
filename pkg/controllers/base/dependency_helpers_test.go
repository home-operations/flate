package base

import (
	"context"
	"errors"
	"slices"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"

	"github.com/home-operations/flate/pkg/manifest"
)

type indexedOrderingDeps map[manifest.NamedResource]bool

func (i indexedOrderingDeps) IsFileIndexed(id manifest.NamedResource) bool { return i[id] }
func (indexedOrderingDeps) Promote(context.Context, *store.ReadSet, manifest.NamedResource) bool {
	return false
}

func parentIndexForTest(parents map[manifest.NamedResource]manifest.NamedResource) *manifest.DiscoveryIndex {
	index := &manifest.DiscoveryIndex{Records: make(map[manifest.NamedResource]manifest.DiscoveryRecord, len(parents))}
	for id, parent := range parents {
		index.Records[id] = manifest.DiscoveryRecord{Parent: parent, Productions: []manifest.Production{{Owner: parent}}}
	}
	return index
}

func (configured *Execution) dependenciesForTest(obj manifest.BaseManifest) []manifest.DependencyRef {
	c := gateExecution(configured.Controller, obj.Named())
	defer c.Reads.Retire()
	deps := c.dependencies(obj, manifest.GateInputs)
	provider := values.NewStoreProvider(&c.Reads, false, c.SubstituteFrom)
	inputs := c.DataInputs(context.TODO(), obj, provider)
	inputs = slices.DeleteFunc(inputs, func(input depwait.DataDependency) bool { return input.Optional })
	for _, input := range inputs {
		if !input.AllowMissing && !input.Input.Private && !input.Input.Generated && !slices.Contains(input.Input.Materializers, obj.Named()) {
			deps = append(deps, input.DependencyRef)
		}
	}
	err := c.AwaitData(context.TODO(), DataWait{Inputs: inputs, Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error { return nil }}, manifest.DrainNone)
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
		for _, id := range blocked.Deps {
			if !slices.ContainsFunc(deps, func(dep manifest.DependencyRef) bool { return dep.NamedResource == id }) {
				deps = append(deps, manifest.DependencyRef{NamedResource: id})
			}
		}
	}

	deps = append(deps, c.parents(obj.Named(), obj)...)
	artifacts := c.dependencies(obj, manifest.GateArtifacts)
	for _, dep := range artifacts {
		dep.Artifact, dep.ChartSource, dep.UnknownArtifactRoot = false, false, false
		if !slices.ContainsFunc(deps, func(existing manifest.DependencyRef) bool {
			return existing.NamedResource == dep.NamedResource && cmp.Equal(existing.ReadyExpr, dep.ReadyExpr)
		}) {
			deps = append(deps, dep)
		}
	}
	for i := range deps {
		deps[i].Artifact, deps[i].ChartSource, deps[i].UnknownArtifactRoot = false, false, false
	}
	return deps
}

func sortedDependencies(deps []manifest.DependencyRef) []manifest.DependencyRef {
	deps = slices.Clone(deps)
	slices.SortStableFunc(deps, func(a, b manifest.DependencyRef) int { return a.Compare(b.NamedResource) })
	return deps
}

func gateExecution(c *Controller, id manifest.NamedResource) *Execution {
	e := &Execution{}
	e.Init(c, id)
	return e
}

func dependencyStore(objects ...manifest.BaseManifest) *store.Store {
	s := store.New()
	for _, obj := range objects {
		s.AddObject(obj)
	}
	return s
}
