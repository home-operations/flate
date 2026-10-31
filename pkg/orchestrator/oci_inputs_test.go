package orchestrator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_OCIInputs(t *testing.T) {
	t.Parallel()
	var listings atomic.Int64
	digest := "sha256:" + strings.Repeat("a", 64)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/tags/list"):
			listings.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"images","tags":["release-a","skip","release-b"]}`)
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", digest)
			w.Header().Set("Content-Length", "2")
			fmt.Fprint(w, "{}")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [provider.yaml, set.yaml]\n")
	testutil.WriteFile(t, root, "provider.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: images, namespace: apps}
spec:
  type: OCIArtifactTag
  url: oci://%s/images
  insecure: true
  filter: {includeTag: '^release-', limit: 1}
  defaultValues: {color: blue, tag: wrong, digest: wrong, id: wrong}
`, strings.TrimPrefix(server.URL, "https://")))
	testutil.WriteFile(t, root, "set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: matrix, namespace: apps}
spec:
  inputsFrom: [{name: images}]
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: '<< inputs.tag >>'}
    data: {tag: '<< inputs.tag >>', digest: '<< inputs.digest >>', id: '<< inputs.id >>', color: '<< inputs.color >>'}
`)
	var previous string
	for run := range 2 {
		rendered, err := RenderTrees(t.Context(), []Tree{{Path: root, RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{Concurrency: 2, CacheDir: t.TempDir()})
		assert.NoError(t, err)
		if len(rendered) != 1 {
			t.Fatalf("rendered %d trees", len(rendered))
		}
		var docs []map[string]any
		for _, batch := range outputByParent(rendered[0].Result.Manifests) {
			for _, doc := range batch {
				if doc["kind"] == manifest.KindConfigMap {
					docs = append(docs, doc)
				}
			}
		}
		if len(docs) != 1 {
			t.Fatalf("registry provider rendered %d resources, want 1", len(docs))
		}
		data := docs[0]["data"].(map[string]any)
		assert.Equal(t, data["tag"], any("release-b"))
		assert.Equal(t, data["digest"], any(digest))
		assert.Equal(t, data["color"], any("blue"))
		if data["id"] == "wrong" || data["id"] == "" {
			t.Fatalf("generated id=%v", data["id"])
		}
		for _, warning := range rendered[0].Result.Warnings {
			if warning.Category == manifest.WarnOfflineInputProvider {
				t.Fatalf("evaluated registry provider warned offline: %+v", warning)
			}
		}
		body, err := json.Marshal(docs)
		assert.NoError(t, err)
		if run > 0 {
			assert.Equal(t, string(body), previous)
		}
		previous = string(body)
	}
	assert.Equal(t, listings.Load(), int64(2))
}

func TestRenderTrees_ChangedOnlyAndDiff_OCIInputs(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/tags/list"):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"images","tags":["selected"]}`)
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", "sha256:"+strings.Repeat("a", 64))
			w.Header().Set("Content-Length", "2")
			fmt.Fprint(w, "{}")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	for _, route := range []string{"named", "selector"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			for _, scenario := range []string{"edit", "delete", "retarget"} {
				t.Run(scenario, func(t *testing.T) {
					t.Parallel()
					dirs := [2]string{t.TempDir(), t.TempDir()}
					for side, root := range dirs {
						testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux/producer.yaml, flux/consumer.yaml, flux/sibling.yaml]\n")
						for _, name := range []string{"producer", "consumer", "sibling"} {
							testutil.WriteFile(t, root, "flux/"+name+".yaml", testutil.KustomizationYAML(name, "apps", "  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
						}
						target, password := "registry-auth", "before"
						ref := "  secretRef: {name: registry-auth}\n"
						if side == 1 {
							switch scenario {
							case "edit":
								password = "after"
							case "delete":
								ref = ""
							case "retarget":
								target = "new-auth"
								ref = "  secretRef: {name: new-auth}\n"
							}
						}
						auth := fmt.Sprintf(`{"auths":{%q:{"username":"user","password":%q}}}`, strings.TrimPrefix(server.URL, "https://"), password)
						testutil.WriteFile(t, root, "producer/docker.json", auth)
						testutil.WriteFile(t, root, "producer/kustomization.yaml", fmt.Sprintf("namespace: apps\ngeneratorOptions: {disableNameSuffixHash: true}\nsecretGenerator:\n- name: %s\n  type: kubernetes.io/dockerconfigjson\n  files: [.dockerconfigjson=docker.json]\n", target))
						testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [provider.yaml, set.yaml]\n")
						testutil.WriteFile(t, root, "consumer/provider.yaml", fmt.Sprintf("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: images, namespace: apps, labels: {role: images}}\nspec:\n  type: OCIArtifactTag\n  url: oci://%s/images\n  insecure: true\n%s", strings.TrimPrefix(server.URL, "https://"), ref))
						inputRef := "{name: images}"
						if route == "selector" {
							inputRef = "{selector: {matchLabels: {role: images}}}"
						}
						testutil.WriteFile(t, root, "consumer/set.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: matrix, namespace: apps}\nspec:\n  inputsFrom: ["+inputRef+"]\n  resourcesTemplate: |\n    apiVersion: v1\n    kind: ConfigMap\n    metadata: {name: '<< inputs.tag >>', namespace: apps}\n")
						testutil.WriteFile(t, root, "sibling/kustomization.yaml", "resources: [result.yaml]\n")
						testutil.WriteFile(t, root, "sibling/result.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: untouched}\n")
					}
					rendered, err := RenderTrees(t.Context(), []Tree{{Path: dirs[0], RepoRoot: dirs[0], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}}, {Path: dirs[1], RepoRoot: dirs[1], ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{Concurrency: 2})
					assert.NoError(t, err)
					assert.Equal(t, len(rendered), 2)
					for _, side := range rendered {
						producer := testutil.KustomizationID("apps", "producer")
						consumer := testutil.KustomizationID("apps", "consumer")
						sibling := testutil.KustomizationID("apps", "sibling")
						provider := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "images"}
						set := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "matrix"}
						assert.Equal(t, side.filter.ShouldReconcile(producer), scenario != "delete" || side.ID.Side == manifest.TreeBase)
						assert.Equal(t, side.filter.ShouldReconcile(consumer), true)
						assert.Equal(t, side.filter.ShouldReconcile(sibling), false)
						assert.Equal(t, side.filter.ShouldReconcile(provider), true)
						assert.Equal(t, side.filter.ShouldReconcile(set), true)
						var outputNames []string
						for _, docs := range outputByParent(side.Result.Manifests) {
							for _, doc := range docs {
								if doc["kind"] == manifest.KindConfigMap {
									outputNames = append(outputNames, doc["metadata"].(map[string]any)["name"].(string))
								}
							}
						}
						assert.Diff(t, outputNames, []string{"selected"})
					}
				})
			}
		})
	}
}
