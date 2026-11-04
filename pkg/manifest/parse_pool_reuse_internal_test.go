package manifest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseDoc_PoolReuseConsistency(t *testing.T) {
	t.Parallel()
	hr := []byte(`
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: default
spec:
  chart:
    spec:
      chart: app-template
      version: "3.5.1"
      sourceRef:
        kind: HelmRepository
        name: bjw-s
        namespace: flux-system
  values:
    replicas: 2
    env:
      TZ: UTC
`)
	ks := []byte(`
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild:
    substitute:
      X: y
`)
	cm := []byte(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm
  namespace: ns
data:
  key1: value1
  key2: value2
`)
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: ns}\ndata: {key: dmFsdWU=}\n"
	raw := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: app}\nspec: {value: keep}\n"
	for _, input := range []string{string(hr), string(ks), string(cm), secret, raw} {
		t.Run(DocKind(mustYAML(t, input)), func(t *testing.T) {
			t.Parallel()
			testParseDocPoolReuseConsistencyCase(t, &input)
		},
		)
	}

	for _, input := range []string{string(hr), string(ks), string(cm), secret, raw} {
		docs, err := decodeDocMaps(strings.NewReader(input))
		assert.NoError(t, err)
		doc := docs[0]
		delete(doc["metadata"].(map[string]any), "namespace")
		before := DeepCopyMap(doc)
		opts := ParseDocOptions{WipeSecrets: true, DefaultNamespace: "apps"}
		parsed, err := ParseDoc(doc, opts)
		obj, authoritative, changed := parsed.Object, parsed.Document, parsed.Changed
		assert.NoError(t, err)
		assert.Equal(t, obj.Named().Namespace, "apps")
		assert.Equal(t, changed, true)
		parsed2, err := ParseDoc(authoritative, opts)
		replay, retained, changed := parsed2.Object, parsed2.Document, parsed2.Changed
		assert.NoError(t, err)
		assert.Equal(t, replay.Named(), obj.Named())
		assert.Equal(t, changed, false)
		{
			next, err := decodeDocMaps(
				strings.NewReader(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next, namespace: other}\ndata: {key: next}\n",
				),
			)
			assert.NoError(t, err)
			_, err = ParseDoc(next[0], opts)
			assert.NoError(t, err)
			ReleaseDoc(next[0])
		}
		assert.Diff(t, doc, before)
		expected := DeepCopyMap(before)
		expected["metadata"].(map[string]any)["namespace"] = "apps"
		assert.Diff(t, authoritative, expected)
		assert.Diff(t, retained, expected)
		if obj, ok := obj.(*RawObject); ok {
			assert.Diff(t, obj.Spec, before["spec"])
		}
	}
}

func testParseDocPoolReuseConsistencyCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	docs, err := decodeDocMaps(strings.NewReader((*testCaseInput)))
	assert.NoError(t, err)
	parsed4, err := ParseDoc(docs[0], defaultParseDocOptions())
	obj := parsed4.Object
	assert.NoError(t, err)
	before := Fingerprint(obj)
	var projected map[string]any
	if k, ok := obj.(*Kustomization); ok {
		projected, err = k.RenderDocument()
		assert.NoError(t, err)
	}
	ReleaseDoc(docs[0])
	next, err := decodeDocMaps(
		strings.NewReader("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next}\ndata: {key: next}\n"),
	)
	assert.NoError(t, err)
	ReleaseDoc(next[0])
	assert.Equal(t, Fingerprint(obj), before)
	switch v := obj.(type) {
	case *Kustomization:
		doc, err := v.RenderDocument()
		assert.NoError(t, err)
		assert.Diff(t, doc, projected)
		start := make(chan struct{})
		done := make(chan error, 2)
		for i := range 2 {
			changed := v.Clone()
			changed.Path = fmt.Sprintf("typed-%d", i)
			changed.PostBuild.Substitute["X"] = changed.Path
			go func() {
				<-start
				edited, renderDocumentErr := changed.RenderDocument()
				if renderDocumentErr == nil && edited["spec"].(map[string]any)["path"] != changed.Path {
					renderDocumentErr = fmt.Errorf("projection did not reflect typed path %q", changed.Path)
				}
				done <- renderDocumentErr
			}()
		}
		close(start)
		for range 2 {
			assert.NoError(t, <-done)
		}
		unchanged, err := v.RenderDocument()
		assert.NoError(t, err)
		assert.Diff(t, unchanged, projected)
		assert.Diff(t, doc, projected)
		assert.Equal(t, v.PostBuild.Substitute["X"], "y")
	case *ConfigMap:
		assert.Equal(t, v.Data["key1"], any("value1"))
	case *Secret:
		assert.Equal(t, v.Data["key"], any("dmFsdWU="))
	case *RawObject:
		assert.Equal(t, v.Spec["value"], any("keep"))
	}
}
