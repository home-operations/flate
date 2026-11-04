package helm

import (
	"reflect"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type changingChartReader struct {
	sourceReader

	artifacts []*store.SourceArtifact
	calls     int
}

//nolint:ireturn // The upstream interface contract requires this return type.
func (r *changingChartReader) GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact {
	artifact := r.artifacts[min(r.calls, len(r.artifacts)-1)]
	r.calls++
	return artifact
}

func TestTemplateCache_ActionHookPolicy(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Log("render persistence ineligible; running warm/fresh functional assertions without claiming disk hits")
	}
	resolver := localChartResolver(t, "chart-repo", "flux-system", helmChartDirectory(t))
	warm := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	warmSources := resolver
	for i, state := range []struct {
		install, upgrade, wantHook bool
	}{
		{false, true, true},
		{true, false, false},
		{false, false, true},
	} {
		opts := Options{}
		want := []string{"ConfigMap/demo-cm"}
		if state.wantHook {
			want = append(want, "ConfigMap/demo-pre")
		}
		var first []map[string]any
		for repeat := range 2 {
			hr := newHR()
			hr.Install = &helmv2.Install{DisableHooks: state.install}
			hr.Upgrade = &helmv2.Upgrade{DisableHooks: state.upgrade}
			out, receipt32, err := testutil.RenderMaps(
				warm.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: warmSources}),
			)
			if err == nil {
				{
					recordErr := warm.RecordRender(
						t.Context(),
						RenderAcceptance{Owner: hr.Named(), Receipt: receipt32, Records: emptyRenderInventory},
					)
					assert.NoError(t, recordErr)
				}
			}

			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, out), want)
			fresh := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			freshSources := resolver
			freshHR := newHR()
			freshHR.Install = &helmv2.Install{DisableHooks: state.install}
			freshHR.Upgrade = &helmv2.Upgrade{DisableHooks: state.upgrade}
			uncached, receipt33, err := testutil.RenderMaps(
				fresh.Template(t.Context(), freshHR, nil, RenderOptions{Options: opts, Sources: freshSources}),
			)
			if err == nil {
				{
					recordErr := fresh.RecordRender(
						t.Context(),
						RenderAcceptance{Owner: freshHR.Named(), Receipt: receipt33, Records: emptyRenderInventory},
					)
					assert.NoError(t, recordErr)
				}
			}

			assert.NoError(t, err)
			assert.Diff(t, templateResources(t, uncached), want)
			assert.Diff(t, out, uncached)
			if repeat == 0 {
				first = out
			} else {
				assert.ErrorIf(
					t,
					!reflect.DeepEqual(out, first),
					"state %d repeated output differs: %q != %q",
					i,
					out,
					first,
				)
			}
		}
	}
}
