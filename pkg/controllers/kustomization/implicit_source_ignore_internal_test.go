package kustomization

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_ImplicitSourceIgnore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		ignore  string
		exclude bool
	}{
		{name: "included", ignore: "# no exclusions\n"},
		{name: "excluded", ignore: "apps/cm.yaml\n", exclude: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, st, root := newControllerWithFixture(t)
			testutil.WriteFile(t, root, ".sourceignore", tc.ignore)
			source := *st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			source.FS, source.Content, source.InputAlias = os.DirFS(root), "snapshot", false
			st.SetArtifact(manifest.BootstrapSourceID, &source)
			owner := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
			st.AddObject(owner)
			c.Tasks.Go(t.Context(), owner.Named().String(), func(ctx context.Context) {
				result := c.ReconcileNode(ctx, owner.Named(), manifest.DrainNone, schedule.Result{})
				defer result.Close()
			})
			c.Tasks.BlockTillDone()
			assert.Equal(t, c.Tasks.Failures(), 0)
			info, _ := st.GetStatus(owner.Named())
			if tc.exclude {
				assert.Equal(t, info.Status, store.StatusFailed)
				assert.Equal(t, strings.Contains(info.Message, "cm.yaml"), true)
				assert.Equal(t, strings.Contains(info.Message, "file does not exist"), true)
				assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent), nil)
				return
			}
			assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v", info)
			emission, _ := st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
			assert.FatalIf(t, emission == nil || len(emission.Manifests) != 1, "missing complete rendered output")
			assert.Equal(t, emission.Complete, true)
			output, err := yaml.Marshal(emission.Manifests[0].Document)
			assert.NoError(t, err)
			assert.Equal(t, string(output), `apiVersion: v1
data:
  greeting: hi
kind: ConfigMap
metadata:
  labels:
    kustomize.toolkit.fluxcd.io/name: apps
    kustomize.toolkit.fluxcd.io/namespace: flux-system
  name: hello
  namespace: default
`)
		})
	}
}

func TestPreviousRender_ImplicitSourceIgnore(t *testing.T) {
	t.Parallel()
	for _, replacement := range []string{"source", "ignore policy"} {
		t.Run(replacement, func(t *testing.T) {
			t.Parallel()
			testPreviousRenderImplicitSourceIgnore(t, replacement)
		})
	}
}

func testPreviousRenderImplicitSourceIgnore(t *testing.T, replacement string) {
	t.Helper()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	var first []byte
	for run := range 2 {
		fixture, st, root := newControllerWithFixture(t)
		trees := kustomize.NewTreeCache(cachetest.New(t, t.TempDir(), sourcecache.New), nil, fixture.Tasks)
		t.Cleanup(trees.Close)
		c := New(st, fixture.Tasks, trees)
		c.Configure(Options{}, sourcecache.RetryConfig{})
		c.StartLifecycle()
		testutil.WriteFile(t, root, ".sourceignore", "# included\n")
		freezeSourceForTest(t, st)
		source := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		assert.Equal(t, source.Content != "", true)
		assert.Equal(t, source.InputAlias, false)
		owner := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
		st.AddObject(owner)
		info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
			Tasks: c.Tasks, Store: st, ID: owner.Named(), ReconcileNode: c.ReconcileNode,
		})
		assert.Equal(t, info.Status, store.StatusReady)
		emission := st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
		assert.Equal(t, emission.Complete, true)
		assert.Equal(t, len(emission.Manifests), 1)
		body, err := json.Marshal(emission.Manifests)
		assert.NoError(t, err)
		if run == 0 {
			first = body
		} else {
			assert.Equal(t, string(body), string(first))
		}
		footprint := maps.Collect(st.ChildFootprint(owner.Named(), emission.Children, nil))
		for range 2 {
			history := c.PreviousRender(t.Context(), owner, schedule.Result{})
			assert.Equal(t, history.Complete, persistent)
			assert.Equal(t, history.Records != nil, persistent)
			if history.Records != nil {
				assert.Diff(t, maps.Collect(history.Records), footprint)
			}
			assert.FatalIf(t, history.Assessment == nil, "missing history assessment")
			history.Assessment.Close()
		}

		if replacement == "source" {
			testutil.WriteFile(
				t,
				root,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: default}\ndata: {greeting: changed}\n",
			)
		} else {
			testutil.WriteFile(t, root, ".sourceignore", "apps/cm.yaml\n")
		}
		updated := *source
		updated.FS = os.DirFS(root)
		st.SetArtifact(manifest.BootstrapSourceID, &updated)
		freezeSourceForTest(t, st)
		history := c.PreviousRender(t.Context(), owner, schedule.Result{})
		assert.Equal(t, history.Complete, false)
		assert.Equal(t, history.Records == nil, true)
		assert.FatalIf(t, history.Assessment == nil, "missing replacement assessment")
		history.Assessment.Close()
	}
}
