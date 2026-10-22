package manifest

import (
	"iter"
	"strings"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// KindInfo contains group-aware admission and reconciliation facts.
type KindInfo struct {
	Group, Kind     string
	Tracked         bool
	Runnable        bool
	Status          bool
	Discovery       bool
	Test            bool
	Parent          bool
	Data            bool
	Structural      bool
	SourceOnly      bool
	TransientOutput bool
	Render          bool
	SourceArtifact  bool
	apiVersion      string
}

var (
	kustomizeAPIVersion = kustomizev1.GroupVersion.String()
	helmAPIVersion      = helmv2.GroupVersion.String()
	sourceAPIVersion    = sourcev1.GroupVersion.String()
)

// APIVersion returns the Flux API version used by readiness projections.
// Other API groups have no readiness projection.
func (k KindInfo) APIVersion() string {
	switch k.Group {
	case FluxKustomizeDomain:
		return kustomizeAPIVersion
	case HelmReleaseDomain:
		return helmAPIVersion
	case SourceDomain:
		return sourceAPIVersion
	}
	return ""
}

type kindEntry struct {
	KindInfo
	decode func(map[string]any) (BaseManifest, error)
}

const (
	kindKustomization = iota
	kindHelmRelease
	kindGitRepository
	kindOCIRepository
	kindHelmRepository
	kindHelmChartSource
	kindExternalArtifact
	kindBucket
	kindResourceSet
	kindResourceSetInputProvider
	kindConfigMap
	kindSecret
	kindArtifactGenerator
)

var (
	kindTable [kindArtifactGenerator + 1]kindEntry
	kindIndex map[string]int
)

// Decoders initialize after package variables because parsing reads this registry.
func init() {
	kindTable = [...]kindEntry{
		kindKustomization:    {KindInfo: KindInfo{Group: FluxKustomizeDomain, Kind: KindKustomization, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, Parent: true, Structural: true, Render: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseKustomization(d) }},
		kindHelmRelease:      {KindInfo: KindInfo{Group: HelmReleaseDomain, Kind: KindHelmRelease, Tracked: true, Runnable: true, Status: true, Test: true, Parent: true, SourceOnly: true, TransientOutput: true, Render: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmRelease(d) }},
		kindGitRepository:    {KindInfo: KindInfo{Group: SourceDomain, Kind: KindGitRepository, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, SourceArtifact: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseGitRepository(d) }},
		kindOCIRepository:    {KindInfo: KindInfo{Group: SourceDomain, Kind: KindOCIRepository, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, SourceArtifact: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseOCIRepository(d) }},
		kindHelmRepository:   {KindInfo: KindInfo{Group: SourceDomain, Kind: KindHelmRepository, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmRepository(d) }},
		kindHelmChartSource:  {KindInfo: KindInfo{Group: SourceDomain, Kind: KindHelmChart, Tracked: true, Runnable: true, Status: true, Discovery: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmChartSource(d) }},
		kindExternalArtifact: {KindInfo: KindInfo{Group: SourceDomain, Kind: KindExternalArtifact, Tracked: true, Runnable: true, Status: true, Discovery: true, SourceArtifact: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseExternalArtifact(d) }},
		kindBucket:           {KindInfo: KindInfo{Group: SourceDomain, Kind: KindBucket, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, SourceArtifact: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseBucket(d) }},
		kindResourceSet:      {KindInfo: KindInfo{Group: FluxOperatorDomain, Kind: KindResourceSet, Tracked: true, Runnable: true, Status: false, Discovery: true, Parent: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseResourceSet(d) }},
		kindResourceSetInputProvider: {KindInfo: KindInfo{Group: FluxOperatorDomain, Kind: KindResourceSetInputProvider, Tracked: true, Runnable: false, Status: false, Discovery: true}, decode: func(d map[string]any) (BaseManifest, error) {
			return parseResourceSetInputProvider(d)
		}},
		kindConfigMap: {KindInfo: KindInfo{Group: "", Kind: KindConfigMap, Tracked: true, Runnable: false, Status: false, Data: true}, decode: func(d map[string]any) (BaseManifest, error) {
			return parseConfigMap(d)
		}},
		kindArtifactGenerator: {KindInfo: KindInfo{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Tracked: true, Discovery: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseRawObject(d) }},
		kindSecret:            {KindInfo: KindInfo{Group: "", Kind: KindSecret, Tracked: true, Runnable: false, Status: false, Data: true}, decode: func(d map[string]any) (BaseManifest, error) { return parseSecret(d) }},
	}

	kindIndex = make(map[string]int, len(kindTable))
	for i := range kindTable {
		entry := &kindTable[i]
		kindIndex[entry.Kind] = i
	}
}

// Kinds yields immutable registry values in a stable order.
func Kinds() iter.Seq[KindInfo] {
	return func(yield func(KindInfo) bool) {
		for i := range kindTable {
			if !yield(kindTable[i].KindInfo) {
				return
			}
		}
	}
}

// KindOf returns the actual object's facts. Unknown kinds are untracked and
// preserve their authored namespace.
func KindOf(obj BaseManifest) KindInfo {
	if obj == nil {
		return KindInfo{}
	}
	return obj.kind()
}

// KindInfo returns canonical registry facts for an identity, including absent objects.
func (n NamedResource) KindInfo() KindInfo {
	if i, ok := kindIndex[n.Kind]; ok && kindTable[i].Group == n.Group {
		return kindTable[i].KindInfo
	}
	return KindInfo{Group: n.Group, Kind: n.Kind}
}

// DecodeIdentity uses the registered decoder and canonical API version to create a typed identity.
// Synthetic source fields satisfy strict decoders without relaxing authored validation.
func (n NamedResource) DecodeIdentity() (BaseManifest, error) {
	i, ok := kindIndex[n.Kind]
	if !ok || kindTable[i].Group != n.Group || n.Name == "" || n.Namespace == "" {
		return nil, inputf("invalid source identity %s", n)
	}
	entry := kindTable[i]
	version := entry.APIVersion()
	if version == "" {
		return nil, inputf("no canonical API version for %s", n)
	}
	return entry.decode(map[string]any{
		"apiVersion": version, "kind": n.Kind,
		"metadata": map[string]any{"name": n.Name, "namespace": n.Namespace},
		"spec":     map[string]any{"url": "file://.", "endpoint": "file://.", "bucketName": n.Name},
	})
}

// Projection stays allocation-free; document admission validates API versions.
func apiGroup(apiVersion string) string {
	before, _, ok := strings.Cut(apiVersion, "/")
	if !ok {
		return ""
	}
	return before
}

func referenceGroup(kind, apiVersion string) string {
	if apiVersion != "" {
		gv, _ := schema.ParseGroupVersion(apiVersion)
		return gv.Group
	}
	if i, ok := kindIndex[kind]; ok {
		return kindTable[i].Group
	}
	return ""
}
