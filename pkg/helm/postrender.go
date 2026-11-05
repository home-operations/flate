package helm

import (
	"bytes"
	"fmt"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"helm.sh/helm/v4/pkg/postrenderer"

	flatekustomize "github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
)

type kustomizePostRenderer struct {
	renderers []helmv2.PostRenderer
	work      *manifest.WorkRecord
}

//nolint:ireturn // The interface result preserves a nil renderer when rendering is disabled.
func newPostRenderer(rs []helmv2.PostRenderer) postrenderer.PostRenderer {
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
		out, err := flatekustomize.PatchDocuments(flatekustomize.DocumentsInput{
			Work: k.work, Documents: current.Bytes(), Patches: r.Kustomize.Patches, Images: r.Kustomize.Images,
		})
		if err != nil {
			return nil, fmt.Errorf("postRenderer: %w", err)
		}
		current = bytes.NewBuffer(out)
	}
	return current, nil
}
