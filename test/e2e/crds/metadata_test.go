package crds

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_ControllerMetadataOwnership(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [root.yaml]\n")
	testutil.WriteFile(t, root, "entry/root.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {interval: 1m, url: https://example.invalid/repo.git, ref: {branch: main}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: flux-system}
spec:
  interval: 1m
  path: ./app
  targetNamespace: apps
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild:
    substitute: {VALUE: expanded}
  commonMetadata:
    labels: {kustomize.toolkit.fluxcd.io/name: ks-common}
    annotations: {note: '${VALUE}'}
`)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [objects.yaml]\n")
	testutil.WriteFile(t, root, "app/objects.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: from-ks, labels: {boolean: true, number: 1.00}, annotations: {boolean: false, number: 1e3}}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: disabled
  annotations: {kustomize.toolkit.fluxcd.io/substitute: disabled}
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release}
spec:
  interval: 1m
  chart:
    spec:
      chart: ./chart
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  commonMetadata:
    labels: {helm.toolkit.fluxcd.io/name: hr-common}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: set}
spec:
  commonMetadata:
    labels: {resourceset.fluxcd.controlplane.io/name: rs-common}
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: from-rs}
`)
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: metadata\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "chart/templates/config.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-hr}\n")
	args := []string{"build", "all", "--path", filepath.Join(root, "entry"), "--cache-dir", t.TempDir(), "--concurrency=2", "--no-progress", "--skip-kinds="}
	first, _ := harness.RequireCLIOK(t, args...)
	second, _ := harness.RequireCLIOK(t, args...)
	assert.Equal(t, second, first)
	docs, err := manifest.DecodeDocs(strings.NewReader(first))
	assert.NoError(t, err)
	seen := map[string]bool{}
	for _, doc := range docs {
		if manifest.DocKind(doc) != "ConfigMap" {
			continue
		}
		name, namespace := manifest.DocMetadata(doc)
		metadata := doc["metadata"].(map[string]any)
		labels, _ := metadata["labels"].(map[string]any)
		annotations, _ := metadata["annotations"].(map[string]any)
		switch name {
		case "from-ks", "disabled":
			if name == "from-ks" {
				assert.Equal(t, labels["boolean"], any("true"))
				assert.Equal(t, labels["number"], any("1.00"))
				assert.Equal(t, annotations["boolean"], any("false"))
				assert.Equal(t, annotations["number"], any("1e3"))
			}
			assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/name"], any("ks-common"))
			want := "expanded"
			if name == "disabled" {
				want = "${VALUE}"
			}
			assert.Equal(t, annotations["note"], any(want))
			assert.Equal(t, namespace, "apps")
		case "from-hr":
			assert.Equal(t, labels[manifest.HelmReleaseDomain+"/name"], any("release"))
			assert.Equal(t, namespace, "apps")
		case "from-rs":
			assert.Equal(t, labels["resourceset.fluxcd.controlplane.io/name"], any("rs-common"))
			assert.Equal(t, namespace, "")
		default:
			t.Fatalf("unexpected ConfigMap %s", name)
		}
		if seen[name] {
			t.Fatalf("duplicate ConfigMap %s", name)
		}
		seen[name] = true
	}
	assert.Diff(t, seen, map[string]bool{"from-ks": true, "disabled": true, "from-hr": true, "from-rs": true})
}
