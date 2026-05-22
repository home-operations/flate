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
