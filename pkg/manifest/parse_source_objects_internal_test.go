package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseResourceSetInputProvider_PreservesEmptyNamespace(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata:
  name: apps
spec:
  type: Static
`)
	rsip, err := parseResourceSetInputProvider(doc)
	{
		assert.FatalIf(t, err != nil, "parseResourceSetInputProvider: %v", err)
	}
	if rsip.Namespace != "" {
		t.Errorf("namespace=%q want empty before loader inheritance/defaulting", rsip.Namespace)
	}
}

func TestParseExternalArtifact(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: ExternalArtifact
metadata:
  name: pulled-tarball
  namespace: apps
spec:
  sourceRef:
    apiVersion: image.toolkit.fluxcd.io/v1beta2
    kind: ImageUpdateAutomation
    name: app-image
    namespace: apps
`)
	ea, err := parseExternalArtifact(doc)
	{
		assert.FatalIf(t, err != nil, "parseExternalArtifact: %v", err)
	}
	{
		assert.ErrorIf(t, ea.Name != "pulled-tarball" || ea.Namespace != "apps", "identity: %+v", ea)
	}
	if ea.SourceRef == nil || ea.SourceRef.Kind != "ImageUpdateAutomation" {
		t.Errorf("sourceRef: %+v", ea.SourceRef)
	}
}
