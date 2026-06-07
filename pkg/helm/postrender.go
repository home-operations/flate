package helm

import (
	"bytes"
	"encoding/json"
	"fmt"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"
	"helm.sh/helm/v4/pkg/postrenderer"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/resmap"
	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	flatekustomize "github.com/home-operations/flate/pkg/kustomize"
)

// kustomizePostRenderer pipes helm's rendered output through one or
// more kustomize PostRenderer.Kustomize specs (patches + images),
// matching helm-controller's behavior. Multiple post-renderers chain:
// the output of one feeds the next.
type kustomizePostRenderer struct {
	renderers []helmv2.PostRenderer
}

// newPostRenderer returns nil when rs has no usable entries — callers
// pass nil to action.Install.PostRenderer to mean "no post-render".
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
		out, err := runKustomizePostRender(current, r.Kustomize)
		if err != nil {
			return nil, err
		}
		current = out
	}
	return current, nil
}

func runKustomizePostRender(in *bytes.Buffer, k *helmv2.Kustomize) (*bytes.Buffer, error) {
	fs := filesys.MakeFsInMemory()
	const inputFile = "helm-output.yaml"
	if err := fs.WriteFile(inputFile, in.Bytes()); err != nil {
		return nil, fmt.Errorf("postRenderer: write helm output: %w", err)
	}

	patches := make([]kustypes.Patch, len(k.Patches))
	for i, p := range k.Patches {
		patches[i] = kustypes.Patch{
			Patch:  p.Patch,
			Target: adaptSelector(p.Target),
		}
	}
	cfg := kustypes.Kustomization{
		TypeMeta: kustypes.TypeMeta{
			APIVersion: kustypes.KustomizationVersion,
			Kind:       kustypes.KustomizationKind,
		},
		Resources: []string{inputFile},
		Images:    adaptImages(k.Images),
		Patches:   patches,
	}

	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("postRenderer: marshal kustomization: %w", err)
	}
	if err := fs.WriteFile("kustomization.yaml", raw); err != nil {
		return nil, fmt.Errorf("postRenderer: write kustomization: %w", err)
	}

	opts := &krusty.Options{
		LoadRestrictions: kustypes.LoadRestrictionsNone,
		PluginConfig:     kustypes.DisabledPluginConfig(),
	}
	// kustomize's krusty pipeline mutates package-global state that is
	// not goroutine-safe. Hold the shared build mutex so this HR
	// post-render never runs concurrently with a Kustomization's
	// build (or another HR's post-render) — see kustomize.BuildMutex.
	rm, err := func() (resmap.ResMap, error) {
		flatekustomize.BuildMutex.Lock()
		defer flatekustomize.BuildMutex.Unlock()
		return krusty.MakeKustomizer(opts).Run(fs, ".")
	}()
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

func adaptSelector(sel *kustomize.Selector) *kustypes.Selector {
	if sel == nil {
		return nil
	}
	out := &kustypes.Selector{
		LabelSelector:      sel.LabelSelector,
		AnnotationSelector: sel.AnnotationSelector,
	}
	out.Group = sel.Group
	out.Version = sel.Version
	out.Kind = sel.Kind
	out.Name = sel.Name
	out.Namespace = sel.Namespace
	return out
}
