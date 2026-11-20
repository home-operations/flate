package manifest

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	operatorv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
)

type fluxInstanceGenerationSpec struct {
	Sync         *operatorv1.Sync `json:"sync,omitempty"         yaml:"sync"`
	Distribution *struct {
		Version string `json:"version" yaml:"version"`
	} `json:"distribution,omitempty" yaml:"distribution"`
	Cluster *struct {
		Multitenant bool `json:"multitenant" yaml:"multitenant"`
	} `json:"cluster,omitempty"      yaml:"cluster"`
	Kustomize *operatorv1.Kustomize `json:"kustomize,omitempty"    yaml:"kustomize"`
}

func (r *RawObject) isFluxInstance() bool {
	return r.Kind == "FluxInstance" && apiGroup(r.APIVersion) == FluxOperatorDomain
}

//nolint:nilnil // An absent sync field leaves an empty generation recipe.
func (r *RawObject) syncSpec() (*operatorv1.Sync, error) {
	if !r.isFluxInstance() || r.Spec["sync"] == nil {
		return nil, nil
	}
	data, err := json.Marshal(r.Spec["sync"])
	if err != nil {
		return nil, NewError(ErrInput, err, "")
	}
	var sync operatorv1.Sync
	if err := json.Unmarshal(data, &sync); err != nil {
		return nil, NewError(ErrInput, err, "")
	}
	return &sync, nil
}

func (r *RawObject) syncApplication(sync *operatorv1.Sync, err error) (EntryApplication, bool) {
	if err != nil {
		return EntryApplication{}, false
	}
	if sync == nil || sync.URL == "" || sync.Ref == "" || sync.Path == "" {
		return EntryApplication{}, false
	}
	name := cmp.Or(sync.Name, r.Namespace)
	if name == "" {
		return EntryApplication{}, false
	}
	switch sync.Kind {
	case KindGitRepository, KindOCIRepository, KindBucket:
		return EntryApplication{
			Source: NamedResource{Group: SourceDomain, Kind: sync.Kind, Name: name, Namespace: r.Namespace},
			Path:   sync.Path,
		}, true
	default:
		return EntryApplication{}, false
	}
}

// GeneratedTargets predicts possible sync identities from the unpatched declaration.
func (r *RawObject) GeneratedTargets() ([]ProducerTarget, error) {
	sync, err := r.syncSpec()
	application, valid := r.syncApplication(sync, err)
	if !valid {
		return nil, err
	}
	root := NamedResource{
		Group: FluxKustomizeDomain, Kind: KindKustomization, Name: application.Source.Name, Namespace: r.Namespace,
	}
	return []ProducerTarget{{NamedResource: application.Source}, {NamedResource: root}}, nil
}

// GeneratedObjects derives the FluxInstance sync recipe from the effective declaration.
func (r *RawObject) GeneratedObjects() (Generation, error) {
	sync, err := r.syncSpec()
	application, valid := r.syncApplication(sync, err)
	if !valid {
		return Generation{}, err
	}
	var spec fluxInstanceGenerationSpec
	projected := map[string]any{
		"distribution": r.Spec["distribution"], "cluster": r.Spec["cluster"], "kustomize": r.Spec["kustomize"],
	}
	data, err := json.Marshal(projected)
	if err == nil {
		err = json.Unmarshal(data, &spec)
	}
	if err != nil {
		return Generation{}, NewError(ErrInput, err, "")
	}
	version := ""
	if spec.Distribution != nil {
		version = spec.Distribution.Version
	}
	sourceVersion, err := syncSourceVersion(sync.Kind, version)
	if err != nil {
		return Generation{}, err
	}
	source := syncSourceSpec(sync)
	root := map[string]any{
		intervalKey: "10m0s", "path": application.Path, "prune": true,
		"sourceRef": map[string]any{kindKey: sync.Kind, nameKey: application.Source.Name},
	}
	if spec.Cluster != nil && spec.Cluster.Multitenant {
		root["serviceAccountName"] = "kustomize-controller"
	}
	id := application.Source
	rootID := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Name: id.Name, Namespace: id.Namespace}
	docs := []DocOccurrence{
		{Document: syncDocument(id, sourceVersion, source)},
		{Document: syncDocument(rootID, "v1", root), Ordinal: 1},
	}
	recipe := Generation{Documents: docs, Objects: make([]BaseManifest, 0, len(docs))}
	if spec.Kustomize != nil {
		recipe.Patches = spec.Kustomize.Patches
	}
	for _, doc := range docs {
		parsed, err := ParseDoc(doc.Document, ParseDocOptions{})
		if err != nil {
			return Generation{}, err
		}
		recipe.Objects = append(recipe.Objects, parsed.Object)
	}
	return recipe, nil
}

func syncDocument(id NamedResource, version string, spec map[string]any) map[string]any {
	return map[string]any{
		apiVersionKey: id.Group + "/" + version, kindKey: id.Kind, "spec": spec,
		metadataKey: map[string]any{nameKey: id.Name, "namespace": id.Namespace},
	}
}

func syncSourceSpec(sync *operatorv1.Sync) map[string]any {
	interval := time.Minute
	if sync.Interval != nil {
		interval = sync.Interval.Duration
	}
	spec := map[string]any{intervalKey: interval.String()}
	if sync.PullSecret != "" {
		spec["secretRef"] = map[string]any{nameKey: sync.PullSecret}
	}
	if sync.Provider != "" {
		spec["provider"] = sync.Provider
	}
	switch sync.Kind {
	case KindGitRepository:
		spec["url"], spec["ref"] = sync.URL, map[string]any{nameKey: sync.Ref}
	case KindOCIRepository:
		spec["url"], spec["ref"] = sync.URL, map[string]any{"tag": sync.Ref}
	case KindBucket:
		spec["endpoint"], spec["bucketName"] = sync.URL, sync.Ref
	}
	return spec
}

// The manifests in Flux Operator v0.63.0 determine the offline range match.
//
//nolint:gochecknoglobals // The pinned versions are immutable.
var syncDistributionV1 = []*semver.Version{
	semver.MustParse("2.10.1"), semver.MustParse("2.10.0"),
	semver.MustParse("2.9.6"), semver.MustParse("2.9.5"), semver.MustParse("2.9.4"),
	semver.MustParse("2.9.3"), semver.MustParse("2.9.2"), semver.MustParse("2.9.1"), semver.MustParse("2.9.0"),
	semver.MustParse("2.8.8"), semver.MustParse("2.8.7"), semver.MustParse("2.8.6"), semver.MustParse("2.8.5"),
	semver.MustParse("2.8.3"), semver.MustParse("2.8.2"), semver.MustParse("2.8.1"), semver.MustParse("2.8.0"),
	semver.MustParse("2.7.5"), semver.MustParse("2.7.4"), semver.MustParse("2.7.3"),
	semver.MustParse("2.7.2"), semver.MustParse("2.7.1"), semver.MustParse("2.7.0"),
	semver.MustParse("2.6.4"), semver.MustParse("2.6.3"), semver.MustParse("2.6.2"),
	semver.MustParse("2.6.1"), semver.MustParse("2.6.0"),
}

//nolint:gochecknoglobals // All sync sources share the pinned API boundary.
var syncRepositoryV1 = semver.MustParse("2.6.0")

func syncSourceVersion(kind, version string) (string, error) {
	if kind == KindGitRepository || version == "" {
		return "v1", nil
	}
	// The operator resolves a distribution range before choosing the API version.
	version, _, _ = strings.Cut(version, "@")
	if parsed, err := semver.NewVersion(version); err == nil {
		if parsed.Prerelease() == "" && !parsed.LessThan(syncRepositoryV1) {
			return "v1", nil
		}
		return "v1beta2", nil
	}
	constraint, err := semver.NewConstraint(version)
	if err != nil {
		return "", NewError(ErrInput, fmt.Errorf("invalid Flux distribution version %q: %w", version, err), "")
	}
	if slices.ContainsFunc(syncDistributionV1, constraint.Check) {
		return "v1", nil
	}
	return "v1beta2", nil
}
