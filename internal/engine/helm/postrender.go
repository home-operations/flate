package helm

import (
	"bytes"
	"fmt"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/engine/kustomize/patch"
	"github.com/home-operations/flate/pkg/manifest"
)

// kustomizePostRenderer pipes helm's rendered output through one or
// more kustomize PostRenderer.Kustomize specs (patches + images),
// matching helm-controller's behavior. Multiple post-renderers chain:
// the output of one feeds the next.
type kustomizePostRenderer struct {
	renderers []helmv2.PostRenderer
	work      *manifest.WorkRecord
}

func newPostRenderer(rs []helmv2.PostRenderer) *kustomizePostRenderer {
	for _, r := range rs {
		if r.Kustomize != nil {
			return &kustomizePostRenderer{renderers: rs}
		}
	}
	return nil
}

func (k *kustomizePostRenderer) Run(in *bytes.Buffer) (*bytes.Buffer, error) {
	current := in
	for _, r := range k.renderers {
		if r.Kustomize == nil {
			continue
		}
		out, err := patch.ApplyPatches(&patch.PatchRequest{
			Documents: current.Bytes(), Patches: r.Kustomize.Patches,
			Images: r.Kustomize.Images, Work: k.work,
		})
		if err != nil {
			return nil, fmt.Errorf("postRenderer: %w", err)
		}
		current = bytes.NewBuffer(out)
	}
	return current, nil
}
