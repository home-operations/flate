package discovery

import (
	"testing"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResult_PreparationTransfer(t *testing.T) {
	t.Parallel()
	fixture := newPreparedConstructionFixture(t, false)
	cfg, st := fixture.cfg, fixture.cfg.Store
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	defer result.Close()
	id := fixture.parent.Named()
	assert.Equal(t, (*Result)(nil).Preparation(id) == nil, true)
	assert.Equal(t, result.Preparation(testutil.KustomizationID("apps", "missing")) == nil, true)
	slot := result.Preparation(id)
	assert.FatalIf(t, slot == nil, "frozen owner has no preparation slot")
	owner, ok := st.GetObject(id).(*manifest.Kustomization)
	assert.Equal(t, ok, true)
	source, _, err := kustomize.SourceInput(st, owner)
	assert.NoError(t, err)
	request := kustomize.RenderRequest{
		Source: source, Spec: owner, SubPath: owner.Path,
		ApplyIgnore: kustomize.ApplySourceIgnore(owner, source),
	}
	assert.Equal(t, slot.Matches(cfg.Trees, &request), true)
	var destination base.Preparation[kustomize.Preparation]
	var execution base.Execution
	execution.Init(base.New(st, task.NewBounded(2)), id)
	defer execution.Close()
	request.Preparation = execution.Prepare(&destination, slot, func(p *kustomize.Preparation) bool {
		return p.Matches(cfg.Trees, &request)
	})
	assert.Equal(t, result.Preparation(id), slot)
	assert.Equal(t, slot.Matches(cfg.Trees, &request), false)
	assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), true)
	result.Close()
	assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), true)
	docs, _, err := kustomize.Render(t.Context(), cfg.Trees, request, cfg.RemoteRetry)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, docs[0].Document["kind"], any(manifest.KindKustomization))
	execution.Retire()
	assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), false)
}
