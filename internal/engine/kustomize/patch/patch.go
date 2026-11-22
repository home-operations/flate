// Package patch applies Flux patches to an already constructed document set.
package patch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	"sigs.k8s.io/kustomize/api/builtins" //nolint:staticcheck // Krusty Run does not expose its resource map while applying patches.
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/api/resource"
	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/openapi"

	kustomizestate "github.com/home-operations/flate/internal/engine/kustomize/krusty"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// PatchRequest carries one ordered patch operation on a supplied set of documents.
//
//nolint:revive // The plan names this cross-package Kustomize request type.
type PatchRequest struct {
	Documents []byte
	Patches   []fluxapis.Patch
	Images    []fluxapis.Image
	Work      *manifest.WorkRecord
	Partial   bool
}

// ApplyPatches applies the upstream transformer to rendered documents.
func ApplyPatches(request *PatchRequest) ([]byte, error) {
	cfg := kustypes.Kustomization{
		APIVersion: kustypes.KustomizationVersion,
		Kind:       kustypes.KustomizationKind,
		Resources:  []string{"output.yaml"},
	}
	if len(request.Patches) != 0 {
		cfg.Patches = make([]kustypes.Patch, len(request.Patches))
		for i, item := range request.Patches {
			cfg.Patches[i] = kustypes.Patch{Patch: item.Patch, Target: AdaptSelector(item.Target)}
		}
	}
	if len(request.Images) != 0 {
		cfg.Images = make([]kustypes.Image, len(request.Images))
		for i, image := range request.Images {
			cfg.Images[i] = kustypes.Image{
				Name: image.Name, NewName: image.NewName, NewTag: image.NewTag, Digest: image.Digest,
			}
		}
	}
	request.Work.Add(&manifest.WorkCounts{Attempts: 1})
	rm, err := patchBuild(&cfg, request.Documents, request.Partial)
	request.Work.Finish(err)
	if err != nil {
		return nil, fmt.Errorf("kustomize build: %w", err)
	}
	out, err := rm.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("marshal output: %w", err)
	}
	return out, nil
}

//nolint:ireturn // The native Kustomize build returns this interface.
func patchBuild(cfg *kustypes.Kustomization, documents []byte, partial bool) (_ resmap.ResMap, err error) {
	patches := cfg.Patches
	if partial {
		cfg.Patches = nil
	}
	fs := filesys.MakeFsInMemory()
	if writeErr := fs.WriteFile(cfg.Resources[0], documents); writeErr != nil {
		return nil, fmt.Errorf("write patch input: %w", writeErr)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal kustomization: %w", err)
	}
	if writeErr := fs.WriteFile("kustomization.yaml", raw); writeErr != nil {
		return nil, fmt.Errorf("write kustomization: %w", writeErr)
	}

	kustomizestate.BuildMutex.Lock()
	defer kustomizestate.BuildMutex.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("recovered from kustomize build panic: %v", recovered)
			openapi.ResetOpenAPI()
		}
	}()
	if partial {
		documents, err = patchPartialDocuments(documents, patches)
		if err != nil {
			return nil, err
		}
		if writeErr := fs.WriteFile(cfg.Resources[0], documents); writeErr != nil {
			return nil, fmt.Errorf("write patched input: %w", writeErr)
		}
	}
	return kustomizestate.Build(fs, ".", false)
}

func patchPartialDocuments(documents []byte, patches []kustypes.Patch) ([]byte, error) {
	openapi.Schema()
	deps := provider.NewDefaultDepProvider()
	factory := resmap.NewFactory(deps.GetResourceFactory())
	helpers := resmap.NewPluginHelpers(nil, deps.GetFieldValidator(), factory, nil)
	rm, err := factory.NewResMapFromBytes(documents)
	if err != nil {
		return nil, fmt.Errorf("read patch input: %w", err)
	}
	for _, patch := range patches {
		if patchErr := applyPartialPatch(rm, helpers, patch); patchErr != nil {
			return nil, patchErr
		}
		rm.DropEmpties()
	}
	rm.RemoveBuildAnnotations()
	out, err := rm.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("marshal patch input: %w", err)
	}
	return out, nil
}

func applyPartialPatch(rm resmap.ResMap, helpers *resmap.PluginHelpers, patch kustypes.Patch) error {
	raw, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}
	transformer := builtins.NewPatchTransformerPlugin()
	if err := transformer.Config(helpers, raw); err != nil {
		return fmt.Errorf("configure patch: %w", err)
	}
	if patch.Target == nil {
		implicit, err := helpers.ResmapFactory().RF().SliceFromBytes([]byte(strings.TrimSpace(patch.Patch)))
		if err == nil && len(implicit) != 0 {
			return applyImplicit(rm, implicit)
		}
	}
	if err := transformer.Transform(rm); err != nil {
		return fmt.Errorf("transform patch: %w", err)
	}
	return nil
}

func applyImplicit(rm resmap.ResMap, patches []*resource.Resource) error {
	for _, change := range patches {
		id := change.OrgId()
		if len(rm.GetMatchingResourcesByAnyId(id.Equals)) == 0 {
			continue
		}
		target, err := rm.GetById(id)
		if err != nil {
			return fmt.Errorf("no resource matches strategic merge patch %q: %w", id, err)
		}
		if err := target.ApplySmPatch(change); err != nil {
			return fmt.Errorf("apply strategic merge patch %q: %w", id, err)
		}
	}
	return nil
}

// PrepareGeneration retains generated children and their visible occurrences after patching.
func PrepareGeneration(
	object manifest.BaseManifest,
	work manifest.WorkScope,
) (store.ChildDocuments, error) {
	recipe, err := manifest.GeneratedObjects(object)
	if err != nil {
		return store.ChildDocuments{}, err
	}
	if len(recipe.Patches) == 0 {
		out := store.ChildDocuments{Visible: recipe.Documents}
		if len(recipe.Objects) != 0 {
			out.Children = make([]store.Child, len(recipe.Objects))
		}
		for i, child := range recipe.Objects {
			out.Children[i] = store.Child{Obj: child, Generated: true}
			if i < len(recipe.Documents) {
				out.Children[i].Doc = recipe.Documents[i].Document
				out.Children[i].VisibleGeneration = true
			}
		}
		return out, nil
	}
	var input bytes.Buffer
	encoder := json.NewEncoder(&input)
	for _, doc := range recipe.Documents {
		if input.Len() != 0 {
			input.WriteString("---\n")
		}
		if encodeErr := encoder.Encode(doc.Document); encodeErr != nil {
			return store.ChildDocuments{}, fmt.Errorf("encode generated document: %w", encodeErr)
		}
	}
	out, err := ApplyPatches(&PatchRequest{
		Documents: input.Bytes(), Patches: recipe.Patches, Partial: true,
		Work: work.Record(manifest.WorkPreparation, "patches"),
	})
	if err != nil {
		return store.ChildDocuments{}, manifest.NewError(manifest.ErrInput, err, "")
	}
	docs, err := manifest.DecodeDocs(bytes.NewReader(out), manifest.RequiredDocuments)
	if err != nil {
		return store.ChildDocuments{}, fmt.Errorf("decode generated documents: %w", err)
	}
	children := loader.ParseChildren(object, docs, manifest.ParseDocOptions{}, nil)
	for i := range children.Children {
		children.Children[i].Generated = true
		children.Children[i].VisibleGeneration = true
	}
	return children, manifest.DocumentFailure(children.Failures)
}

// AdaptSelector converts a Flux selector into a kustomize selector. Mirrors
// flux's generator adaptSelector (nil in, nil out).
func AdaptSelector(selector *fluxapis.Selector) *kustypes.Selector {
	if selector == nil {
		return nil
	}
	out := &kustypes.Selector{}
	out.Group = selector.Group
	out.Kind = selector.Kind
	out.Version = selector.Version
	out.Name = selector.Name
	out.Namespace = selector.Namespace
	out.LabelSelector = selector.LabelSelector
	out.AnnotationSelector = selector.AnnotationSelector
	return out
}
