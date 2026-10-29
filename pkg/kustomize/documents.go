package kustomize

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/resmap"
	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
)

// DocumentsInput describes a document build and its ordered patch composition.
type DocumentsInput struct {
	Documents    []byte
	Patches      []fluxapis.Patch
	Images       []fluxapis.Image
	Namespace    string
	Transformers [][]byte
	// Subset omits targetless patches absent from the initial document identities.
	Subset bool
}

// PatchDocuments applies Flux patches and images in one kustomize build.
func PatchDocuments(in DocumentsInput) ([]byte, error) {
	fs := filesys.MakeFsInMemory()
	if err := fs.WriteFile("documents.yaml", in.Documents); err != nil {
		return nil, fmt.Errorf("write documents: %w", err)
	}
	cfg := kustypes.Kustomization{
		APIVersion: kustypes.KustomizationVersion,
		Kind:       kustypes.KustomizationKind,
		Resources:  []string{"documents.yaml"},
		Namespace:  in.Namespace,
		Images:     adaptImages(in.Images),
	}
	for i, transformer := range in.Transformers {
		name := "transformer-" + strconv.Itoa(i) + ".yaml"
		if err := fs.WriteFile(name, transformer); err != nil {
			return nil, fmt.Errorf("write transformer: %w", err)
		}
		cfg.Transformers = append(cfg.Transformers, name)
	}

	patches := in.Patches
	if in.Subset {
		var err error
		patches, err = func() ([]fluxapis.Patch, error) {
			kustomizestate.BuildMutex.Lock()
			defer kustomizestate.BuildMutex.Unlock()
			rf := provider.NewDefaultDepProvider().GetResourceFactory()
			initial, err := resmap.NewFactory(rf).NewResMapFromBytes(in.Documents)
			if err != nil {
				return nil, fmt.Errorf("parse documents: %w", err)
			}
			retained := make([]fluxapis.Patch, 0, len(in.Patches))
			for _, patch := range in.Patches {
				if patch.Target != nil {
					retained = append(retained, patch)
					continue
				}
				resources, err := rf.SliceFromBytes([]byte(patch.Patch))
				if err != nil {
					return nil, fmt.Errorf("parse patch: %w", err)
				}
				if len(resources) == 0 {
					retained = append(retained, patch)
				}
				for _, resource := range resources {
					id := resource.OrgId()
					if len(initial.GetMatchingResourcesByAnyId(id.Equals)) == 0 {
						slog.Debug("kustomize: skipping patch outside document set", "target", id.String())
						continue
					}
					if _, err := initial.GetById(id); err != nil {
						return nil, fmt.Errorf("resolve patch target: %w", err)
					}
					raw, err := resource.AsYAML()
					if err != nil {
						return nil, fmt.Errorf("marshal patch: %w", err)
					}
					retained = append(retained, fluxapis.Patch{Patch: string(raw)})
				}
			}
			return retained, nil
		}()
		if err != nil {
			return nil, err
		}
	}
	cfg.Patches = make([]kustypes.Patch, len(patches))
	for i, patch := range patches {
		cfg.Patches[i] = kustypes.Patch{Patch: patch.Patch, Target: AdaptSelector(patch.Target)}
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal kustomization: %w", err)
	}
	if err := fs.WriteFile("kustomization.yaml", raw); err != nil {
		return nil, fmt.Errorf("write kustomization: %w", err)
	}
	rm, err := func() (resmap.ResMap, error) {
		kustomizestate.BuildMutex.Lock()
		defer kustomizestate.BuildMutex.Unlock()
		return kustomizestate.Build(fs, ".")
	}()
	if err != nil {
		return nil, fmt.Errorf("kustomize build: %w", err)
	}
	out, err := rm.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("marshal output: %w", err)
	}
	return out, nil
}

func adaptImages(in []fluxapis.Image) []kustypes.Image {
	if len(in) == 0 {
		return nil
	}
	out := make([]kustypes.Image, len(in))
	for i, image := range in {
		out[i] = kustypes.Image{
			Name: image.Name, NewName: image.NewName, NewTag: image.NewTag, Digest: image.Digest,
		}
	}
	return out
}
