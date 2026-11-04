package store

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRequire_RetainsPredicates(t *testing.T) {
	t.Parallel()
	for _, expressions := range [][]string{
		{"false", ""},
		{"", "false"},
		{"true", "false"},
		{"false", "true", "false", "", "true", "1 == 2", "1 == 2", ""},
	} {
		t.Run(strings.Join(expressions, "/"), func(t *testing.T) {
			t.Parallel()
			testRequireRetainsPredicatesCase(t, &expressions)
		},
		)
	}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("facts/reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			testRequireRetainsPredicatesCase2(t, &reverse)
		},
		)
	}
}

func TestRequire_ReleasesPredicates(t *testing.T) {
	t.Parallel()
	s := New()
	var reads ReadSet
	inputs := make([]Input, 1)
	for _, retirement := range []bool{false, true} {
		reads.Init(s, ownerID("reader"), inputs)
		for _, id := range []manifest.NamedResource{ownerID("first"), ownerID("second")} {
			reads.Require(manifest.DependencyRef{NamedResource: id, ReadyExpr: new("true")})
			reads.Require(manifest.DependencyRef{NamedResource: id, ReadyExpr: new("false")})
		}
		if retirement {
			reads.Retire()
			for input := reads.inputs; input != nil; input = input.next {
				{
					details := input.details.Load()
					assert.FatalIf(
						t,
						details != nil && details.gates != nil,
						"%s",
						"retirement retained overflow predicates",
					)
				}
			}
		}
	}
	reads.Init(s, ownerID("reader"), inputs)
	assert.Equal(t, len(slices.Collect(reads.Gates())), 0)
	assert.FatalIf(
		t,
		reads.localDetails.gates != nil || inputs[0].details.Load() != nil,
		"%s",
		"reset retained overflow predicates",
	)
}

func TestRequire_CapturedIdentityDuringAdmission(t *testing.T) {
	t.Parallel()
	s := New()
	obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(nil))
	ref := manifest.DependencyRef{
		NamedResource: obj.Named(),
		ReadyExpr:     new("true"),
		Artifact:      true,
		ChartSource:   true,
		ArtifactOnly:  true,
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		reads.Require(ref)
	})
	workers.Go(func() {
		<-start
		s.AddObject(obj)
	})
	close(start)
	workers.Wait()
	assert.Diff(t, slices.Collect(reads.Gates()), []Gate{{DependencyRef: ref}})
	assert.Equal(t, s.GetObject(obj.Named()), manifest.BaseManifest(obj))
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(nil))
}

func testRequireRetainsPredicatesCase(t *testing.T, testCaseInput *[]string,
) {
	t.Helper()

	s := New()
	obj := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
	s.AddObject(obj)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	var want []Gate
	for _, expr := range *testCaseInput {
		ref := manifest.DependencyRef{NamedResource: obj.Named(), ReadyExpr: new(expr)}
		reads.Require(ref)
		if !slices.ContainsFunc(
			want,
			func(prior Gate) bool { return sameReadinessGate(new(prior.DependencyRef), new(ref)) },
		) {
			want = append(want, Gate{DependencyRef: ref})
		}
	}
	assert.Diff(t, slices.Collect(reads.Gates()), want)
	assert.Diff(t, slices.Collect(reads.Required()), []manifest.NamedResource{obj.Named()})
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(obj))
	assert.FatalIf(
		t,
		reads.inputs != &reads.local || reads.inputs.next != nil,
		"%s",
		"predicates created multiple captured authorities for one identity",
	)
	for range reads.Gates() {
		break
	}
	got := 0
	for range reads.Gates() {
		got++
		if got == 2 {
			break
		}
	}
	assert.Equal(t, got, 2)
}

func testRequireRetainsPredicatesCase2(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	s := New()
	obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	s.AddObject(obj)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	refs := []manifest.DependencyRef{
		{NamedResource: obj.Named()},
		{NamedResource: obj.Named(), ReadyExpr: new("")},
		{NamedResource: obj.Named(), ReadyExpr: new("true")},
		{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true},
		{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true, ChartSource: true},
		{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true, ArtifactOnly: true},
	}
	if *testCaseInput {
		slices.Reverse(refs)
		reads.RequireMaterializer(obj.Named())
	}
	for _, ref := range refs {
		reads.Require(ref)
	}
	for _, ref := range refs {
		if ref.ReadyExpr != nil {
			ref.ReadyExpr = new(*ref.ReadyExpr)
		}
		reads.Require(ref)
	}
	reads.RequireMaterializer(obj.Named())
	want := make([]Gate, 0, len(refs)+1)
	for _, ref := range refs {
		want = append(want, Gate{DependencyRef: ref})
	}
	materializer := Gate{NamedResource: obj.Named(), Materializer: true}
	if *testCaseInput {
		want = append([]Gate{materializer}, want...)
	} else {
		want = append(want, materializer)
	}
	assert.Diff(t, slices.Collect(reads.Gates()), want)
	assert.Diff(t, slices.Collect(reads.Required()), []manifest.NamedResource{obj.Named()})
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(obj))
	assert.FatalIf(
		t,
		reads.inputs != &reads.local || reads.inputs.next != nil,
		"%s",
		"gate facts created more than one captured authority",
	)
}
