package manifest

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
)

type catalogSnapshot struct {
	Identity    NamedResource     `json:"Identity"`
	Labels      map[string]string `json:"Labels,omitempty"`
	Annotations map[string]string `json:"Annotations,omitempty"`
	Data        json.RawMessage   `json:"Data,omitempty"`
	Raw         []byte            `json:"Raw,omitempty"`
	Version     string            `json:"Version,omitempty"`
	Suspended   bool              `json:"Suspended,omitempty"`
}

type helmReleaseCatalog struct {
	Spec         helmv2.HelmReleaseSpec `json:"Spec"`
	ChartName    string                 `json:"ChartName,omitempty"`
	ChartVersion string                 `json:"ChartVersion,omitempty"`
}

// EncodeCatalog MUST preserve fields omitted or shadowed by declaration JSON tags.
func EncodeCatalog(object BaseManifest) ([]byte, error) {
	if object == nil {
		return nil, nil
	}
	snapshot := catalogSnapshot{Identity: object.Named(), Suspended: object.Suspended()}
	if labeled, ok := object.(interface{ GetLabels() map[string]string }); ok {
		snapshot.Labels = labeled.GetLabels()
	}
	if annotated, ok := object.(interface{ GetAnnotations() map[string]string }); ok {
		snapshot.Annotations = annotated.GetAnnotations()
	}
	var err error
	if raw, ok := object.(*RawObject); ok {
		snapshot.Version = raw.APIVersion
		doc := map[string]any{}
		if raw.Spec != nil {
			doc["spec"] = raw.Spec
		}
		snapshot.Raw, err = EncodeRender([]DocOccurrence{{Document: doc}})
	} else {
		var payload any = object
		if release, ok := object.(*HelmRelease); ok {
			payload = &helmReleaseCatalog{
				Spec: release.HelmReleaseSpec, ChartName: release.Chart.Name, ChartVersion: release.Chart.Version,
			}
		}
		snapshot.Data, err = jsonv2.Marshal(payload, json.DefaultOptionsV1(), jsonv2.OmitZeroStructFields(true))
	}
	if err != nil {
		return nil, NewError(ErrFlux, fmt.Errorf("encode catalog: %w", err), "")
	}
	data, err := jsonv2.Marshal(&snapshot, json.DefaultOptionsV1())
	if err != nil {
		return nil, NewError(ErrFlux, fmt.Errorf("encode catalog: %w", err), "")
	}
	return data, nil
}

// DecodeCatalog grants no execution authority; callers must hydrate authored declarations.
//
//nolint:ireturn // Catalog restoration preserves the sealed manifest contract.
func DecodeCatalog(data []byte) (BaseManifest, error) {
	var s catalogSnapshot
	if len(data) != 0 {
		if err := jsonv2.Unmarshal(data, &s, json.DefaultOptionsV1()); err != nil {
			return nil, NewError(ErrFlux, fmt.Errorf("decode catalog: %w", err), "")
		}
	}
	if s.Identity == (NamedResource{}) {
		return nil, nil //nolint:nilnil // An absent catalog has no declaration or failure.
	}
	if s.Raw != nil {
		docs, err := DecodeRender(s.Raw)
		if err != nil {
			return nil, err
		}
		if len(docs) != 1 {
			return nil, NewError(ErrFlux, nil, "decode catalog: expected one raw specification")
		}
		spec, _ := docs[0].Document["spec"].(map[string]any)
		return &RawObject{
			Kind:       s.Identity.Kind,
			APIVersion: s.Version,
			Name:       s.Identity.Name,
			Namespace:  s.Identity.Namespace,
			Spec:       spec,
		}, nil
	}
	var object BaseManifest
	index, known := kindIndex[s.Identity.Kind]
	if !known || kindTable[index].Group != s.Identity.Group {
		return nil, NewError(ErrFlux, nil, "decode catalog: unsupported identity")
	}
	switch index {
	case kindKustomization:
		object = &Kustomization{Labels: s.Labels, Annotations: s.Annotations}
	case kindHelmRelease:
		return s.helmRelease()
	case kindGitRepository:
		object = &GitRepository{Labels: s.Labels}
	case kindOCIRepository:
		object = &OCIRepository{Labels: s.Labels}
	case kindHelmRepository:
		object = &HelmRepository{}
	case kindHelmChartSource:
		object = &HelmChartSource{Labels: s.Labels}
	case kindExternalArtifact:
		object = &ExternalArtifact{Labels: s.Labels}
	case kindBucket:
		object = &Bucket{Labels: s.Labels}
	case kindResourceSet:
		object = &ResourceSet{Labels: s.Labels, Suspend: s.Suspended}
	case kindResourceSetInputProvider:
		object = &ResourceSetInputProvider{Labels: s.Labels}
	case kindConfigMap:
		object = &ConfigMap{}
	case kindSecret:
		object = &Secret{}
	default:
		return nil, NewError(ErrFlux, nil, "decode catalog: unsupported kind")
	}
	if err := jsonv2.Unmarshal(s.Data, object, json.DefaultOptionsV1()); err != nil {
		return nil, NewError(ErrFlux, fmt.Errorf("decode catalog: %w", err), "")
	}
	if object.Named() != s.Identity {
		return nil, NewError(ErrFlux, nil, "decode catalog: identity mismatch")
	}
	return object.catalog(s.Identity), nil
}

//nolint:ireturn // Catalog restoration preserves the sealed manifest contract.
func (s *catalogSnapshot) helmRelease() (BaseManifest, error) {
	var release helmReleaseCatalog
	if err := jsonv2.Unmarshal(s.Data, &release, json.DefaultOptionsV1()); err != nil {
		return nil, NewError(ErrFlux, fmt.Errorf("decode catalog: %w", err), "")
	}
	object := &HelmRelease{
		Name: s.Identity.Name, Namespace: s.Identity.Namespace, Labels: s.Labels,
		HelmReleaseSpec: release.Spec, Chart: HelmChart{Name: release.ChartName, Version: release.ChartVersion},
	}
	return object.catalog(s.Identity), nil
}
