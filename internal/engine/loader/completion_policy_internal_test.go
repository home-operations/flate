package loader

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCompleteDeclarations_IgnorePolicies(t *testing.T) {
	t.Parallel()
	for _, ignored := range []string{"before", "after", "both"} {
		t.Run(ignored, func(t *testing.T) { t.Parallel(); testCompleteDeclarationsIgnorePoliciesCase(t, &ignored) })
	}
}

func testCompleteDeclarationsIgnorePoliciesCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s, namespace: apps}\n"
	before := fstest.MapFS{"input.yaml": {Data: fmt.Appendf(nil, body, "before")}}
	after := fstest.MapFS{"input.yaml": {Data: fmt.Appendf(nil, body, "after")}}
	if (*testCaseInput) != "after" {
		before[".sourceignore"] = &fstest.MapFile{Data: []byte("input.yaml\n")}
	}
	if (*testCaseInput) != "before" {
		after[".sourceignore"] = &fstest.MapFile{Data: []byte("input.yaml\n")}
	}
	beforeView, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Source, Current: before})
	assert.NoError(t, err)
	afterView, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Source, Current: after})
	assert.NoError(t, err)
	set, err := change.Detect(
		t.Context(),
		&store.SourceArtifact{LocalPath: "/before", FS: beforeView},
		&store.SourceArtifact{LocalPath: "/after", FS: afterView},
		change.DetectConfig{},
	)
	assert.NoError(t, err)
	assert.Equal(t, set.Contains("input.yaml"), (*testCaseInput) != "both")
	if (*testCaseInput) != "both" {
		side, name := 1, "after"
		if (*testCaseInput) == "after" {
			side, name = 0, "before"
		}
		_, artifacts, _ := set.ForSource(manifest.BootstrapSourceID)
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, artifacts[side])
		l := New(st)
		l.SourceRoot = artifacts[side].LocalPath
		index, err := l.CompleteDeclarations(
			t.Context(),
			selection.DemandDeclarations(manifest.TreeSide(side), set, l.Index),
		)
		assert.NoError(t, err)
		assert.Diff(
			t,
			slices.SortedFunc(
				maps.Keys(index.Declarations[manifest.BootstrapSourceID]["input.yaml"].Objects),
				manifest.NamedResource.Compare,
			),
			[]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}},
		)
	}
	assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
}
