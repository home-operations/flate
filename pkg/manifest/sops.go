package manifest

// IsEncryptedSecret reports whether doc looks like a SOPS-encrypted
// Kubernetes resource. SOPS appends a top-level `sops` map containing
// its metadata (mac, kms/age/pgp blocks, version) after encrypting the
// document's body; presence of that map with a `mac` or `version`
// field is the unambiguous signal.
//
// flate runs offline and cannot decrypt; the kustomization and
// helmrelease controllers call this to fail-loud when their rendered
// output still contains encrypted content, mirroring Flux's
// kustomize-controller refusal to apply un-decrypted Secrets when
// spec.decryption is absent.
func IsEncryptedSecret(doc map[string]any) bool {
	sops, ok := doc["sops"].(map[string]any)
	if !ok {
		return false
	}
	if _, ok := sops["mac"]; ok {
		return true
	}
	if _, ok := sops["version"]; ok {
		return true
	}
	return false
}

// SubstituteAnnotationKey is Flux kustomize-controller's per-resource
// opt-out for postBuild substitution. A resource carrying this label
// or annotation with value "disabled" is excluded from envsubst,
// commonly used for ConfigMaps that embed shell scripts whose
// $${VAR[@]} bash array expansions would otherwise crash the parser.
const SubstituteAnnotationKey = "kustomize.toolkit.fluxcd.io/substitute"

// SubstituteDisabledValue is the literal value that opts a resource
// out of postBuild substitution. Matches Flux's `DisabledValue`.
const SubstituteDisabledValue = "disabled"

// HasSubstituteDisabled reports whether a manifest doc carries the
// substitute-disabled label or annotation. flate skips envsubst on
// such resources, mirroring Flux's per-resource opt-out.
func HasSubstituteDisabled(doc map[string]any) bool {
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		return false
	}
	for _, field := range []string{"labels", "annotations"} {
		m, _ := md[field].(map[string]any)
		if v, _ := m[SubstituteAnnotationKey].(string); v == SubstituteDisabledValue {
			return true
		}
	}
	return false
}
