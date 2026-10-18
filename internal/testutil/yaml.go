package testutil

// KustomizationYAML wraps spec in a Flux Kustomization document.
func KustomizationYAML(name, namespace, spec string) string {
	metadata := "metadata:\n  name: " + name + "\n"
	if namespace != "" {
		metadata += "  namespace: " + namespace + "\n"
	}
	return "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n" + metadata + "spec:\n" + spec
}
