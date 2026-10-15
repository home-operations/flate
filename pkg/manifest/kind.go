package manifest

import (
	"iter"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// KindInfo contains group-aware admission and reconciliation facts.
type KindInfo struct {
	Group, Kind string
	Tracked     bool
	Runnable    bool
	Status      bool
	includes    uint8
}

const (
	inDiscovery uint8 = 1 << iota
	inTest
)

// Includes reports membership in "discovery" or "test" roster.
// Untracked documents remain available in the document cache.
func (k KindInfo) Includes(use string) bool {
	switch use {
	case "discovery":
		return k.includes&inDiscovery != 0
	case "test":
		return k.includes&inTest != 0
	}
	return false
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

var kindTable = [...]kindEntry{
	kindKustomization:    {KindInfo: KindInfo{FluxKustomizeDomain, KindKustomization, true, true, true, inDiscovery | inTest}, decode: func(d map[string]any) (BaseManifest, error) { return parseKustomization(d) }},
	kindHelmRelease:      {KindInfo: KindInfo{HelmReleaseDomain, KindHelmRelease, true, true, true, inTest}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmRelease(d) }},
	kindGitRepository:    {KindInfo: KindInfo{SourceDomain, KindGitRepository, true, true, true, inDiscovery | inTest}, decode: func(d map[string]any) (BaseManifest, error) { return parseGitRepository(d) }},
	kindOCIRepository:    {KindInfo: KindInfo{SourceDomain, KindOCIRepository, true, true, true, inDiscovery | inTest}, decode: func(d map[string]any) (BaseManifest, error) { return parseOCIRepository(d) }},
	kindHelmRepository:   {KindInfo: KindInfo{SourceDomain, KindHelmRepository, true, true, true, inDiscovery | inTest}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmRepository(d) }},
	kindHelmChartSource:  {KindInfo: KindInfo{SourceDomain, KindHelmChart, true, true, true, inDiscovery}, decode: func(d map[string]any) (BaseManifest, error) { return parseHelmChartSource(d) }},
	kindExternalArtifact: {KindInfo: KindInfo{SourceDomain, KindExternalArtifact, true, true, true, inDiscovery}, decode: func(d map[string]any) (BaseManifest, error) { return parseExternalArtifact(d) }},
	kindBucket:           {KindInfo: KindInfo{SourceDomain, KindBucket, true, true, true, inDiscovery | inTest}, decode: func(d map[string]any) (BaseManifest, error) { return parseBucket(d) }},
	kindResourceSet:      {KindInfo: KindInfo{FluxOperatorDomain, KindResourceSet, true, true, false, inDiscovery}, decode: func(d map[string]any) (BaseManifest, error) { return parseResourceSet(d) }},
	kindResourceSetInputProvider: {KindInfo: KindInfo{FluxOperatorDomain, KindResourceSetInputProvider, true, false, false, inDiscovery}, decode: func(d map[string]any) (BaseManifest, error) {
		return parseResourceSetInputProvider(d)
	}},
	kindConfigMap: {KindInfo: KindInfo{"", KindConfigMap, true, false, false, 0}, decode: func(d map[string]any) (BaseManifest, error) {
		return parseConfigMap(d)
	}},
	kindSecret: {KindInfo: KindInfo{"", KindSecret, true, false, false, 0}, decode: func(d map[string]any) (BaseManifest, error) { return parseSecret(d) }},
}

var kindIndex = func() map[string]int {
	kinds := make(map[string]int, len(kindTable))
	for i := range kindTable {
		entry := &kindTable[i]
		kinds[entry.Kind] = i
	}
	return kinds
}()

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

func apiGroup(apiVersion string) string {
	gv, _ := schema.ParseGroupVersion(apiVersion)
	return gv.Group
}
