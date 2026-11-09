package helm

import (
	"bytes"
	"encoding/json"
	"fmt"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"
	"helm.sh/helm/v4/pkg/postrenderer"
	"sigs.k8s.io/kustomize/api/resmap"
	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	flatekustomize "github.com/home-operations/flate/pkg/kustomize"
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

// newPostRenderer returns nil when rs has no usable entries — callers
// pass nil to action.Install.PostRenderer to mean "no post-render".
//
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
		out, err := runKustomizePostRender(current, r.Kustomize, k.work)
		if err != nil {
			return nil, err
		}
		current = out
	}
	return current, nil
}

func runKustomizePostRender(in *bytes.Buffer, k *helmv2.Kustomize, work *manifest.WorkRecord) (*bytes.Buffer, error) {
	fs := filesys.MakeFsInMemory()
	const inputFile = "helm-output.yaml"
	if err := fs.WriteFile(inputFile, in.Bytes()); err != nil {
		return nil, fmt.Errorf("postRenderer: write helm output: %w", err)
	}

	patches := make([]kustypes.Patch, len(k.Patches))
	for i, p := range k.Patches {
		patches[i] = kustypes.Patch{
			Patch:  p.Patch,
			Target: flatekustomize.AdaptSelector(p.Target),
		}
	}
	cfg := kustypes.Kustomization{
		APIVersion: kustypes.KustomizationVersion,
		Kind:       kustypes.KustomizationKind,
		Resources:  []string{inputFile},
		Images:     adaptImages(k.Images),
		Patches:    patches,
	}

	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("postRenderer: marshal kustomization: %w", err)
	}
	if writeFileErr := fs.WriteFile("kustomization.yaml", raw); writeFileErr != nil {
		return nil, fmt.Errorf("postRenderer: write kustomization: %w", writeFileErr)
	}

	work.Add(&manifest.WorkCounts{Attempts: 1})
	rm, err := func() (resmap.ResMap, error) {
		kustomizestate.BuildMutex.Lock()
		defer kustomizestate.BuildMutex.Unlock()
		return kustomizestate.Build(fs, ".")
	}()
	work.Finish(err)
	if err != nil {
		return nil, fmt.Errorf("postRenderer: kustomize build: %w", err)
	}
	out, err := rm.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("postRenderer: marshal output: %w", err)
	}
	return bytes.NewBuffer(out), nil
}

func adaptImages(in []kustomize.Image) []kustypes.Image {
	if len(in) == 0 {
		return nil
	}
	out := make([]kustypes.Image, len(in))
	for i, im := range in {
		out[i] = kustypes.Image{
			Name:    im.Name,
			NewName: im.NewName,
			NewTag:  im.NewTag,
			Digest:  im.Digest,
		}
	}
	return out
}
