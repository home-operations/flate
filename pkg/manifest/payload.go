package manifest

import "maps"

// OverlayPayload overlays obj's sanitized payload onto doc. Secret output owns
// its top-level map and replacement payloads; other fields remain borrowed and
// MUST stay immutable. Other kinds return doc unchanged.
func OverlayPayload(obj BaseManifest, doc map[string]any) map[string]any {
	return obj.overlayPayload(doc)
}

func (s *Secret) overlayPayload(doc map[string]any) map[string]any {
	doc = maps.Clone(doc)
	snapshot := s.Snapshot()
	for _, key := range []string{"data", "stringData", "type"} {
		delete(doc, key)
		if value, ok := snapshot[key]; ok {
			doc[key] = value
		}
	}
	return doc
}

func (*Kustomization) overlayPayload(doc map[string]any) map[string]any            { return doc }
func (*HelmRelease) overlayPayload(doc map[string]any) map[string]any              { return doc }
func (*HelmRepository) overlayPayload(doc map[string]any) map[string]any           { return doc }
func (*HelmChartSource) overlayPayload(doc map[string]any) map[string]any          { return doc }
func (*GitRepository) overlayPayload(doc map[string]any) map[string]any            { return doc }
func (*OCIRepository) overlayPayload(doc map[string]any) map[string]any            { return doc }
func (*ExternalArtifact) overlayPayload(doc map[string]any) map[string]any         { return doc }
func (*Bucket) overlayPayload(doc map[string]any) map[string]any                   { return doc }
func (*ResourceSet) overlayPayload(doc map[string]any) map[string]any              { return doc }
func (*ResourceSetInputProvider) overlayPayload(doc map[string]any) map[string]any { return doc }
func (*ConfigMap) overlayPayload(doc map[string]any) map[string]any                { return doc }
func (*RawObject) overlayPayload(doc map[string]any) map[string]any                { return doc }
