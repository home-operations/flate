package manifest

import (
	"cmp"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	operatorv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
)

const kindFluxInstance = "FluxInstance"

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

// CanDeclareGeneratedEntry describes the kind before the sync specification is read.
func (n NamedResource) CanDeclareGeneratedEntry() bool {
	return n.isFluxInstance()
}

func (n NamedResource) isFluxInstance() bool {
	return n.Kind == kindFluxInstance && n.Group == FluxOperatorDomain
}

//nolint:nilnil // An absent sync field leaves an empty generation recipe.
func (r *RawObject) syncSpec() (*operatorv1.Sync, error) {
	if !r.Named().isFluxInstance() || r.Spec["sync"] == nil {
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
	if err != nil || !hasSyncSource(sync) {
		return EntryApplication{}, false
	}
	name := cmp.Or(sync.Name, r.Namespace)
	if name == "" {
		return EntryApplication{}, false
	}
	return EntryApplication{
		Source: NamedResource{Group: SourceDomain, Kind: sync.Kind, Name: name, Namespace: r.Namespace},
		Path:   sync.Path,
	}, true
}

func hasSyncSource(sync *operatorv1.Sync) bool {
	if !hasSyncLocation(sync) || sync.Path == "" {
		return false
	}
	switch sync.Kind {
	case KindGitRepository, KindOCIRepository, KindBucket:
		return true
	default:
		return false
	}
}

func hasSyncLocation(sync *operatorv1.Sync) bool {
	return sync != nil && sync.URL != "" && sync.Ref != ""
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

//nolint:gochecknoglobals // OCIRepository and Bucket share this API boundary.
var syncRepositoryV1 = semver.MustParse("2.6.0")

var syncVersionNumbers = regexp.MustCompile(`\d+(?:\.\d+){0,2}`)

func syncSourceVersion(kind, version string) (string, error) {
	if kind == KindGitRepository || version == "" {
		return "v1", nil
	}
	version, _, _ = strings.Cut(version, "@")
	constraint, err := semver.NewConstraint(version)
	if err != nil {
		return "", NewError(ErrInput, fmt.Errorf("invalid Flux distribution version %q: %w", version, err), "")
	}
	matches := func(candidate *semver.Version) bool {
		return !candidate.LessThan(syncRepositoryV1) && constraint.Check(candidate)
	}
	if matches(syncRepositoryV1) {
		return "v1", nil
	}

	// A comparison can change truth at a bound or the next patch, minor, or major boundary.
	for _, number := range syncVersionNumbers.FindAllString(version, -1) {
		bound, err := semver.NewVersion(number)
		if err != nil {
			continue
		}
		major, minor, patch := bound.Major(), bound.Minor(), bound.Patch()
		switch {
		case matches(bound):
			return "v1", nil
		case matches(semver.New(major, minor, patch+1, "", "")):
			return "v1", nil
		case matches(semver.New(major, minor+1, 0, "", "")):
			return "v1", nil
		case matches(semver.New(major+1, 0, 0, "", "")):
			return "v1", nil
		}
	}
	return "v1beta2", nil
}
