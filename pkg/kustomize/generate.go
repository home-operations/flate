package kustomize

// generate.go reproduces fluxcd/pkg/kustomize.Generator.GenerateManifest against
// an injected (in-memory) filesystem, so flate can merge a Flux Kustomization's
// spec into a kustomization.yaml entirely in RAM — no on-disk staging, no
// per-render mutation of a shared tree.
//
// Why a reimplementation rather than calling flux's Generator: flux v1.32.0's
// Generator picks its own filesystem in getFS() (a secure on-disk FS rooted at
// g.root, or a plain on-disk FS) and exposes no seam to inject one — the
// `WithFS` its doc comment references does not exist in this release. flux ships
// a memory-over-disk overlay (kustomize/filesys.MakeFsInMemory) but has not yet
// wired it into the Generator. So to render in memory we mirror GenerateManifest
// here. A golden byte-equivalence test (generate_test.go) pins this to flux's
// real Generator across the full field matrix.
//
// Scope: flate constructs the Generator via NewGenerator (no ignore, no filter),
// so only the filter==false path is reproduced. Source-controller's file
// exclusion (.sops.yaml, binaries, …) is applied earlier, when the tree is
// materialized into the fs (see tree.go) — uniformly for every source — so the
// Generator never sees an ignored file and needs no ignore logic of its own.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/kustomize/api/konfig"
	"sigs.k8s.io/kustomize/api/provider"
	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/source/sourceignore"
)

// Flux Kustomization spec field names (mirrors the unexported consts in
// fluxcd/pkg/kustomize/kustomize_generator.go).
const (
	specField             = "spec"
	targetNSField         = "targetNamespace"
	patchesField          = "patches"
	componentsField       = "components"
	ignoreComponentsField = "ignoreMissingComponents"
	patchesSMField        = "patchesStrategicMerge"
	patchesJSON6902Field  = "patchesJson6902"
	imagesField           = "images"
	namePrefixField       = "namePrefix"
	nameSuffixField       = "nameSuffix"
	buildMetadataField    = "buildMetadata"
)

// generateManifest merges the Flux Kustomization document `obj` into the
// kustomization.yaml that governs `dirPath` within fsys, and returns the merged
// YAML bytes plus the fs path the caller must write them to (the existing
// kustomization file when one is present, else <dirPath>/kustomization.yaml).
//
// It is a faithful port of Generator.GenerateManifest: same field order, same
// _placeholder / originAnnotations empty-build guards, same sigs.k8s.io/yaml
// encoder, so the output is byte-identical to flux's. ignore, when non-nil,
// reproduces flux's NewGeneratorWithIgnore behavior for the auto-generated
// (path: ./) case — source-controller-excluded files (.sops.yaml, binaries, …)
// are skipped while scanning for resources, so a working-tree source renders
// like a fetched artifact would (this is the bo0tzz fix). The base path comes
// from fsys.CleanedAbs (the real on-disk path under the memory-over-disk
// overlay), matching flux's filepath.Abs(dirPath).
func generateManifest(fsys filesys.FileSystem, dirPath string, obj map[string]any, ignore *sourceignore.Matcher) ([]byte, string, error) {
	data, kfile, foundExisting, err := findOrGenerateKustomization(fsys, dirPath, ignore)
	if err != nil {
		return nil, "", err
	}

	kus := kustypes.Kustomization{
		TypeMeta: kustypes.TypeMeta{
			APIVersion: kustypes.KustomizationVersion,
			Kind:       kustypes.KustomizationKind,
		},
	}
	if err := yaml.Unmarshal(data, &kus); err != nil {
		return nil, "", err
	}

	// An existing kustomization with no resources would fail krusty's
	// "kustomization.yaml is empty" check; originAnnotations keeps it buildable.
	// (The generated-from-scratch path uses a _placeholder namespace instead —
	// see findOrGenerateKustomization.)
	if foundExisting && len(kus.Resources) == 0 {
		kus.BuildMetadata = []string{"originAnnotations"}
	}

	if v, ok, err := unstructured.NestedString(obj, specField, targetNSField); err != nil {
		return nil, "", err
	} else if ok {
		kus.Namespace = v
	}
	if v, ok, err := unstructured.NestedString(obj, specField, namePrefixField); err != nil {
		return nil, "", err
	} else if ok {
		kus.NamePrefix = v
	}
	if v, ok, err := unstructured.NestedString(obj, specField, nameSuffixField); err != nil {
		return nil, "", err
	} else if ok {
		kus.NameSuffix = v
	}

	patches, err := decodeTypedSlice[fluxapis.Patch](obj, specField, patchesField)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get patches: %w", err)
	}
	for _, p := range patches {
		kus.Patches = append(kus.Patches, kustypes.Patch{
			Patch:  p.Patch,
			Target: adaptSelector(p.Target),
		})
	}

	components, _, err := unstructured.NestedStringSlice(obj, specField, componentsField)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get components: %w", err)
	}
	ignoreMissing, _, _ := unstructured.NestedBool(obj, specField, ignoreComponentsField)
	for _, component := range components {
		if !fluxkustomize.IsLocalRelativePath(component) {
			return nil, "", fmt.Errorf("component path '%s' must be local and relative", component)
		}
		if !fsys.Exists(filepath.Join(dirPath, component)) && ignoreMissing {
			continue
		}
		kus.Components = append(kus.Components, component)
	}

	patchesSM, err := decodeTypedSlice[apiextensionsv1.JSON](obj, specField, patchesSMField)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get patchesStrategicMerge: %w", err)
	}
	for _, p := range patchesSM {
		//nolint:staticcheck // byte-for-byte parity with flux's Generator, which populates this deprecated field
		kus.PatchesStrategicMerge = append(kus.PatchesStrategicMerge, kustypes.PatchStrategicMerge(p.Raw))
	}

	patchesJSON, err := decodeTypedSlice[fluxapis.JSON6902Patch](obj, specField, patchesJSON6902Field)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get patchesJson6902: %w", err)
	}
	for _, p := range patchesJSON {
		raw, err := json.Marshal(p.Patch)
		if err != nil {
			return nil, "", err
		}
		//nolint:staticcheck // byte-for-byte parity with flux's Generator, which populates this deprecated field
		kus.PatchesJson6902 = append(kus.PatchesJson6902, kustypes.Patch{
			Patch:  string(raw),
			Target: adaptSelector(&p.Target),
		})
	}

	images, err := decodeTypedSlice[fluxapis.Image](obj, specField, imagesField)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get images: %w", err)
	}
	for _, image := range images {
		newImage := kustypes.Image{
			Name:    image.Name,
			NewName: image.NewName,
			NewTag:  image.NewTag,
			Digest:  image.Digest,
		}
		if exists, index := findImage(kus.Images, image.Name); exists {
			kus.Images[index] = newImage
		} else {
			kus.Images = append(kus.Images, newImage)
		}
	}

	buildMetadata, _, err := unstructured.NestedStringSlice(obj, specField, buildMetadataField)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get buildMetadata: %w", err)
	}
	if len(buildMetadata) > 0 {
		kus.BuildMetadata = buildMetadata
	}

	manifest, err := yaml.Marshal(kus)
	if err != nil {
		return nil, "", err
	}
	return manifest, kfile, nil
}

// findOrGenerateKustomization returns the existing kustomization file's bytes
// (foundExisting=true) or, when none is present, synthesizes one from the YAML
// manifests in dirPath via scanManifests. Mirrors the like-named flux function.
func findOrGenerateKustomization(fsys filesys.FileSystem, dirPath string, ignore *sourceignore.Matcher) (data []byte, kfile string, foundExisting bool, err error) {
	for _, name := range konfig.RecognizedKustomizationFileNames() {
		kpath := filepath.Join(dirPath, name)
		if fsys.Exists(kpath) && !fsys.IsDir(kpath) {
			b, rerr := fsys.ReadFile(kpath)
			return b, kpath, true, rerr
		}
	}

	// flux uses filepath.Abs(dirPath); under the memory-over-disk overlay
	// CleanedAbs delegates to the disk layer, yielding the real absolute path
	// — also the prefix every walked path carries — so the TrimPrefix below
	// yields the same "./relative" resource entries flux produces on disk.
	base, _, err := fsys.CleanedAbs(dirPath)
	if err != nil {
		return nil, "", false, err
	}
	files, err := scanManifests(fsys, base.String(), ignore)
	if err != nil {
		return nil, "", false, err
	}

	kus := kustypes.Kustomization{
		TypeMeta: kustypes.TypeMeta{
			APIVersion: kustypes.KustomizationVersion,
			Kind:       kustypes.KustomizationKind,
		},
	}
	var resources []string
	for _, file := range files {
		// flux computes this as strings.Replace(file, base, ".", 1); since every
		// walked path is rooted at base, that equals "./" + <path relative to
		// base>. We spell it as a TrimPrefix so it also stays correct when base
		// is the in-memory fs root (reported as "/"), where flux's first-occurrence
		// replace would eat the leading separator and emit ".x.yaml".
		rel := strings.TrimPrefix(strings.TrimPrefix(file, base.String()), "/")
		resources = append(resources, "./"+rel)
	}
	if len(resources) == 0 {
		// A placeholder namespace avoids krusty's "kustomization.yaml is empty"
		// error when the directory has no resources.
		kus.Namespace = "_placeholder"
	} else {
		kus.Resources = resources
	}

	kfile = filepath.Join(dirPath, konfig.DefaultKustomizationFileName())
	data, err = yaml.Marshal(kus)
	return data, kfile, false, err
}

// scanManifests walks base collecting the YAML files usable as kustomization
// resources: a sub-directory that itself carries a kustomization file is added
// as a resource and not descended into; every other .yaml/.yml file is added
// once it parses as Kubernetes YAML. Mirrors the like-named flux function
// (filter==false: no sourceignore consulted — that filtering happened at
// materialize time).
func scanManifests(fsys filesys.FileSystem, base string, ignore *sourceignore.Matcher) ([]string, error) {
	var paths []string
	rf := provider.NewDefaultDepProvider().GetResourceFactory()

	err := fsys.Walk(base, func(path string, info os.FileInfo, err error) (walkErr error) {
		if err != nil {
			return err
		}
		if path == base {
			return nil
		}
		if info.IsDir() {
			for _, name := range konfig.RecognizedKustomizationFileNames() {
				if kpath := filepath.Join(path, name); fsys.Exists(kpath) && !fsys.IsDir(kpath) {
					paths = append(paths, path)
					return filepath.SkipDir
				}
			}
			return nil
		}
		if ext := filepath.Ext(path); ext != ".yaml" && ext != ".yml" {
			return nil
		}
		// Skip source-controller-excluded files (sourceignore defaults +
		// in-tree .sourceignore) for working-tree sources — flux's
		// filter==true behavior, which keeps a stray .sops.yaml or binary
		// out of an auto-generated kustomization's resource list.
		if ignore != nil {
			if rel, rerr := filepath.Rel(base, path); rerr == nil && ignore.Match(rel, false) {
				return nil
			}
		}
		contents, err := fsys.ReadFile(path)
		if err != nil {
			return err
		}
		// The kustomize YAML parser can panic on (accidentally) invalid object
		// data; recover so one bad file fails its own scan rather than the run.
		defer func() {
			if r := recover(); r != nil {
				walkErr = fmt.Errorf("recovered from panic while parsing YAML file %s: %v", filepath.Base(path), r)
			}
		}()
		if _, err := rf.SliceFromBytes(contents); err != nil {
			return fmt.Errorf("failed to decode Kubernetes YAML from %s: %w", path, err)
		}
		paths = append(paths, path)
		return nil
	})
	return paths, err
}

// decodeTypedSlice reads a nested []any field and converts each element into T
// via the unstructured converter. It is the shared body of flux's four
// near-identical getters (getPatches / getPatchesStrategicMerge /
// getPatchesJson6902 / getImages) — same converter, same accumulate-on-error
// semantics, same diagnostic text — collapsed into one generic helper. A
// missing field yields (nil, nil), matching flux's ok==false return.
func decodeTypedSlice[T any](obj map[string]any, fields ...string) ([]T, error) {
	raw, ok, err := unstructured.NestedSlice(obj, fields...)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	res := make([]T, 0, len(raw))
	var resultErr error
	for k, p := range raw {
		m, ok := p.(map[string]any)
		if !ok {
			resultErr = errors.Join(resultErr, fmt.Errorf("unable to convert patch %d to map[string]interface{}", k))
		}
		var t T
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &t); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
		res = append(res, t)
	}
	return res, resultErr
}

// adaptSelector converts a Flux selector into a kustomize selector. Mirrors
// flux's generator adaptSelector (nil in, nil out).
func adaptSelector(selector *fluxapis.Selector) *kustypes.Selector {
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

// findImage returns whether an image with the given name is already present in
// images, and its index. Mirrors flux's checkKustomizeImageExists (used to
// dedup spec.images by name, last write winning).
func findImage(images []kustypes.Image, name string) (bool, int) {
	for i, image := range images {
		if name == image.Name {
			return true, i
		}
	}
	return false, -1
}
