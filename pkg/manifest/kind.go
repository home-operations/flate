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
	Group, Kind string
	Tracked     bool
	Runnable    bool
	Status      bool
	includes    uint16
}

const (
	inDiscovery uint16 = 1 << iota
	inTest
	inParent
	inData
	inRefire
	inStructural
	inSourceOnly
	inReportHidden
	inTransientOutput
)

// Includes reports membership in a semantic roster: discovery, test, parent
// (KS/HR/RS), data (ConfigMap/Secret), refire (sources except HelmChart, plus KS),
// structural (KS), source-only or transient-output (HR), report-hidden (HelmChart),
// or render (KS/HR).
// Untracked documents remain available in the document cache.
func (k KindInfo) Includes(use string) bool {
	switch use {
	case "discovery":
		return k.includes&inDiscovery != 0
	case "test":
		return k.includes&inTest != 0
	case "parent":
		return k.includes&inParent != 0
	case "data":
		return k.includes&inData != 0
	case "refire":
		return k.includes&inRefire != 0
	case "structural":
		return k.includes&inStructural != 0
	case "source-only":
		return k.includes&inSourceOnly != 0
	case "report-hidden":
		return k.includes&inReportHidden != 0
	case "transient-output":
		return k.includes&inTransientOutput != 0
	case "render":
		return k.Status && k.Group != SourceDomain
	}
	return false
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
)

var (
	kindTable [kindSecret + 1]kindEntry
	kindIndex map[string]int
)

// Decoders initialize after package variables because parsing reads this registry.
func init() {
	kindTable = [...]kindEntry{
		kindKustomization:    {KindInfo: KindInfo{FluxKustomizeDomain, KindKustomization, true, true, true, inDiscovery | inTest | inParent | inRefire | inStructural}, decode: func(d map[string]any) (BaseManifest, error) { return parseKustomization(d) }},
		kindHelmRelease:      {KindInfo: KindInfo{HelmReleaseDomain, KindHelmRelease, true, true, true, inTest | inParent | inSourceOnly | inTransientOutput}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmRelease(d) }},
		kindGitRepository:    {KindInfo: KindInfo{SourceDomain, KindGitRepository, true, true, true, inDiscovery | inTest | inRefire}, decode: func(d map[string]any) (BaseManifest, error) { return parseGitRepository(d) }},
		kindOCIRepository:    {KindInfo: KindInfo{SourceDomain, KindOCIRepository, true, true, true, inDiscovery | inTest | inRefire}, decode: func(d map[string]any) (BaseManifest, error) { return parseOCIRepository(d) }},
		kindHelmRepository:   {KindInfo: KindInfo{SourceDomain, KindHelmRepository, true, true, true, inDiscovery | inTest | inRefire}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmRepository(d) }},
		kindHelmChartSource:  {KindInfo: KindInfo{SourceDomain, KindHelmChart, true, true, true, inDiscovery | inReportHidden}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmChartSource(d) }},
		kindExternalArtifact: {KindInfo: KindInfo{SourceDomain, KindExternalArtifact, true, true, true, inDiscovery | inRefire}, decode: func(d map[string]any) (BaseManifest, error) { return parseExternalArtifact(d) }},
		kindBucket:           {KindInfo: KindInfo{SourceDomain, KindBucket, true, true, true, inDiscovery | inTest | inRefire}, decode: func(d map[string]any) (BaseManifest, error) { return parseBucket(d) }},
		kindResourceSet:      {KindInfo: KindInfo{FluxOperatorDomain, KindResourceSet, true, true, false, inDiscovery | inParent}, decode: func(d map[string]any) (BaseManifest, error) { return parseResourceSet(d) }},
		kindResourceSetInputProvider: {KindInfo: KindInfo{FluxOperatorDomain, KindResourceSetInputProvider, true, false, false, inDiscovery}, decode: func(d map[string]any) (BaseManifest, error) {
			return parseResourceSetInputProvider(d)
		}},
		kindConfigMap: {KindInfo: KindInfo{"", KindConfigMap, true, false, false, inData}, decode: func(d map[string]any) (BaseManifest, error) {
			return parseConfigMap(d)
		}},
		kindSecret: {KindInfo: KindInfo{"", KindSecret, true, false, false, inData}, decode: func(d map[string]any) (BaseManifest, error) { return parseSecret(d) }},
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

// Projection stays allocation-free; document admission validates API versions.
func apiGroup(apiVersion string) string {
	group, _, ok := strings.Cut(apiVersion, "/")
	if !ok {
		return ""
	}
	return group
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
