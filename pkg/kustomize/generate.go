package kustomize

// generate.go mirrors fluxcd/pkg/kustomize.Generator.GenerateManifest on an
// injected filesystem. The upstream generator constructs its own disk filesystem,
// so flate merges Flux specs into private construction overlays in memory.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

	"github.com/home-operations/flate/pkg/manifest"
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
// encoder, so the output is byte-identical to flux's. The admitted filesystem
// already applies source-controller's file exclusions before the scan.
// CleanedAbs supplies the mounted base path used by the resource scan.
func generateManifest(fsys filesys.FileSystem, dirPath string, obj map[string]any) ([]byte, string, error) {
	data, kfile, foundExisting, err := findOrGenerateKustomization(fsys, dirPath)
	if err != nil {
		return nil, "", err
	}

	kus := kustypes.Kustomization{
		APIVersion: kustypes.KustomizationVersion,
		Kind:       kustypes.KustomizationKind,
	}
	if err := yaml.Unmarshal(data, &kus); err != nil {
		return nil, "", err
	}

	// An existing kustomization with no resources would fail krusty's
	// "kustomization.yaml is empty" check; originAnnotations keeps it buildable.
	// (The generated-from-scratch path uses a _placeholder namespace instead;
	// see findOrGenerateKustomization.)
	if foundExisting && len(kus.Resources) == 0 {
		kus.BuildMetadata = []string{"originAnnotations"}
	}

	if err := setNestedString(&kus.Namespace, obj, specField, targetNSField); err != nil {
		return nil, "", err
	}
	if err := setNestedString(&kus.NamePrefix, obj, specField, namePrefixField); err != nil {
		return nil, "", err
	}
	if err := setNestedString(&kus.NameSuffix, obj, specField, nameSuffixField); err != nil {
		return nil, "", err
	}

	patches, err := decodeTypedSlice[fluxapis.Patch](obj, specField, patchesField)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get patches: %w", err)
	}
	for _, p := range patches {
		kus.Patches = append(kus.Patches, kustypes.Patch{
			Patch:  p.Patch,
			Target: AdaptSelector(p.Target),
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
			Target: AdaptSelector(&p.Target),
		})
	}

	images, err := decodeTypedSlice[fluxapis.Image](obj, specField, imagesField)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get images: %w", err)
	}
	for _, image := range images {
		if i := slices.IndexFunc(kus.Images, func(img kustypes.Image) bool {
			return img.Name == image.Name
		}); i >= 0 {
			if image.NewName != "" {
				kus.Images[i].NewName = image.NewName
			}
			// Tag and digest select one version; overriding either must clear
			// the other so a stale digest cannot defeat a new tag at pull time.
			if image.NewTag != "" || image.Digest != "" {
				kus.Images[i].NewTag = image.NewTag
				kus.Images[i].Digest = image.Digest
			}
		} else {
			kus.Images = append(kus.Images, kustypes.Image{
				Name:    image.Name,
				NewName: image.NewName,
				NewTag:  image.NewTag,
				Digest:  image.Digest,
			})
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

// setNestedString assigns the nested string at fields to *dst when present,
// leaving *dst untouched when the field is absent — mirroring flux's
// "set only when ok" handling of targetNamespace/namePrefix/nameSuffix.
func setNestedString(dst *string, obj map[string]any, fields ...string) error {
	v, ok, err := unstructured.NestedString(obj, fields...)
	if err != nil {
		return err
	}
	if ok {
		*dst = v
	}
	return nil
}

// kustomizationFileIn returns the path of the first recognized kustomization
// file directly inside dir (a regular file, not a directory), or ok=false when
// none is present. Mirrors the lookup flux performs in both findOrGenerate and
// its manifest scan.
func kustomizationFileIn(fsys filesys.FileSystem, dir string) (path string, ok bool) {
	for _, name := range konfig.RecognizedKustomizationFileNames() {
		kpath := filepath.Join(dir, name)
		if fsys.Exists(kpath) && !fsys.IsDir(kpath) {
			return kpath, true
		}
	}
	return "", false
}

// findOrGenerateKustomization returns the existing kustomization file's bytes
// (foundExisting=true) or, when none is present, synthesizes one from the YAML
// manifests in dirPath via scanManifests. Mirrors the like-named flux function.
func findOrGenerateKustomization(fsys filesys.FileSystem, dirPath string) (data []byte, kfile string, foundExisting bool, err error) {
	if kpath, ok := kustomizationFileIn(fsys, dirPath); ok {
		b, rerr := fsys.ReadFile(kpath)
		return b, kpath, true, rerr
	}

	// CleanedAbs and Walk MUST use the same mounted path prefix so resource
	// entries remain relative to the build root.
	base, _, err := fsys.CleanedAbs(dirPath)
	if err != nil {
		return nil, "", false, err
	}
	files, err := scanManifests(fsys, base.String())
	if err != nil {
		return nil, "", false, err
	}

	kus := kustypes.Kustomization{
		APIVersion: kustypes.KustomizationVersion,
		Kind:       kustypes.KustomizationKind,
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
// (filter==false: no sourceignore consulted — that filtering happens at the
// filesystem, see ignorefs.go).
func scanManifests(fsys filesys.FileSystem, base string) ([]string, error) {
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
			if _, ok := kustomizationFileIn(fsys, path); ok {
				paths = append(paths, path)
				return filepath.SkipDir
			}
			return nil
		}
		if ext := filepath.Ext(path); ext != ".yaml" && ext != ".yml" {
			return nil
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
			// A kustomize Kustomization/Component config decodes as a resource
			// without metadata.name and fails cryptically. The usual cause is a
			// misnamed kustomization file (e.g. "kustomzation.yaml"), which the
			// scan then treats as a resource — say so plainly instead.
			if kind := kustomizeConfigKind(contents); kind != "" {
				return fmt.Errorf("%s: looks like a misnamed kustomization file (kind %s); rename it to %s or remove it",
					path, kind, konfig.DefaultKustomizationFileName())
			}
			return fmt.Errorf("failed to decode Kubernetes YAML from %s: %w", path, err)
		}
		paths = append(paths, path)
		return nil
	})
	return paths, err
}

// kustomizeConfigKind reports the kustomize config kind (Kustomization or
// Component) when b is a kustomize.config.k8s.io document, else "". The resource
// scan uses it to recognize a misnamed kustomization file — which would
// otherwise fail to decode as a Kubernetes object on the missing metadata.name —
// and report it clearly.
func kustomizeConfigKind(b []byte) string {
	var head struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
	}
	if err := yaml.Unmarshal(b, &head); err != nil {
		return ""
	}
	if !strings.HasPrefix(head.APIVersion, manifest.KustomizeDomain+"/") {
		return ""
	}
	if head.Kind == kustypes.KustomizationKind || head.Kind == kustypes.ComponentKind {
		return head.Kind
	}
	return ""
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
