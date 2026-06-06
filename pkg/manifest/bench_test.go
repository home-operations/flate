package manifest

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// BenchmarkDecodeDocs_LargeFile measures DecodeDocs against a 50-doc
// multi-document YAML stream — the typical size of a chart-rendered
// or List-bundled file the loader hands to the manifest parser. Each
// iteration releases the docs back to the internal pool to model the
// loader's parseFile → ReleaseDoc lifecycle.
func BenchmarkDecodeDocs_LargeFile(b *testing.B) {
	var buf bytes.Buffer
	for i := range 50 {
		if i > 0 {
			buf.WriteString("---\n")
		}
		fmt.Fprintf(&buf, `apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-%d
  namespace: ns
data:
  key-%d: value-%d
  count: "%d"
  enabled: "true"
`, i, i, i, i)
	}
	data := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		docs, err := DecodeDocs(bytes.NewReader(data))
		if err != nil {
			b.Fatalf("DecodeDocs: %v", err)
		}
		if len(docs) != 50 {
			b.Fatalf("expected 50 docs, got %d", len(docs))
		}
		for _, d := range docs {
			ReleaseDoc(d)
		}
	}
}

// renderDocsBlob builds a 50-doc multi-document YAML stream resembling
// a chart render that a controller retains on its artifact.
func renderDocsBlob() []byte {
	var buf bytes.Buffer
	for i := range 50 {
		if i > 0 {
			buf.WriteString("---\n")
		}
		fmt.Fprintf(&buf, `apiVersion: apps/v1
kind: Deployment
metadata:
  name: app-%d
  namespace: ns
  labels:
    app.kubernetes.io/name: app-%d
spec:
  replicas: %d
  selector:
    matchLabels:
      app.kubernetes.io/name: app-%d
  template:
    metadata:
      labels:
        app.kubernetes.io/name: app-%d
    spec:
      containers:
        - name: main
          image: registry.example.com/app:%d
          env:
            - name: FOO
              value: bar-%d
`, i, i, i, i, i, i, i)
	}
	return buf.Bytes()
}

// BenchmarkArtifactRetain_Current models the production controller path:
// SplitDocs then retain the slice on the artifact WITHOUT releasing the
// pooled maps (they are owned by the artifact for the run's lifetime).
//
// BenchmarkArtifactRetain_DeepCopyRelease models the audit's proposed
// "fix" — deep-copy each retained doc into a fresh non-pooled map, then
// release the pooled originals so the pool can reuse them. The pair
// proves the fix is net-NEGATIVE: returning the pooled map for reuse
// can at best save one 16-bucket alloc per doc, while the deep copy
// allocates the entire nested tree per doc. So flate keeps the docs
// straight off the pool (drawing from it still skips the initial map
// alloc; retaining them is correct, not a leak). See io.go.
func BenchmarkArtifactRetain_Current(b *testing.B) {
	data := renderDocsBlob()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		docs, err := SplitDocs(data)
		if err != nil {
			b.Fatalf("SplitDocs: %v", err)
		}
		_ = docs // retained on the artifact; not released
	}
}

func BenchmarkArtifactRetain_DeepCopyRelease(b *testing.B) {
	data := renderDocsBlob()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		docs, err := SplitDocs(data)
		if err != nil {
			b.Fatalf("SplitDocs: %v", err)
		}
		retained := make([]map[string]any, len(docs))
		for i, d := range docs {
			retained[i] = DeepCopyMap(d) // break the pool alias
			ReleaseDoc(d)                // return the original for reuse
		}
		_ = retained
	}
}

// BenchmarkParseDoc_Kustomization measures ParseDoc dispatch + the
// kustomize-controller typed decode for a Flux Kustomization document.
func BenchmarkParseDoc_Kustomization(b *testing.B) {
	doc := mustDecodeSingle(b, `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
  postBuild:
    substitute:
      CLUSTER_NAME: home
    substituteFrom:
    - kind: ConfigMap
      name: cluster-settings
  dependsOn:
  - name: cluster-config
    namespace: flux-system
`)
	opts := defaultParseDocOptions()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := ParseDoc(doc, opts); err != nil {
			b.Fatalf("ParseDoc: %v", err)
		}
	}
}

// BenchmarkParseDoc_HelmRelease measures the HelmRelease typed-decode
// path. Includes spec.chart, spec.install, spec.upgrade, and spec.values.
func BenchmarkParseDoc_HelmRelease(b *testing.B) {
	doc := mustDecodeSingle(b, `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app-template
  namespace: default
spec:
  interval: 15m
  chart:
    spec:
      chart: app-template
      version: "3.5.1"
      sourceRef:
        kind: HelmRepository
        name: bjw-s
        namespace: flux-system
  install:
    crds: CreateReplace
    remediation:
      retries: 3
  upgrade:
    cleanupOnFail: true
    remediation:
      strategy: rollback
      retries: 3
  values:
    controllers:
      main:
        replicas: 2
        containers:
          main:
            image:
              repository: nginx
              tag: latest
            env:
              TZ: UTC
            resources:
              requests:
                cpu: 10m
                memory: 64Mi
              limits:
                memory: 256Mi
`)
	opts := defaultParseDocOptions()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := ParseDoc(doc, opts); err != nil {
			b.Fatalf("ParseDoc: %v", err)
		}
	}
}

// BenchmarkParseDoc_GitRepository measures the source-controller
// typed-decode path for a GitRepository CR.
func BenchmarkParseDoc_GitRepository(b *testing.B) {
	doc := mustDecodeSingle(b, `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 10m
  url: https://github.com/example/k8s-gitops
  ref:
    branch: main
  ignore: |
    /*
    !/apps
    !/clusters
  secretRef:
    name: flux-system
`)
	opts := defaultParseDocOptions()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := ParseDoc(doc, opts); err != nil {
			b.Fatalf("ParseDoc: %v", err)
		}
	}
}

// BenchmarkParseDoc_ConfigMap measures the core/v1 ConfigMap fast path.
// The bag of `data` entries dominates the parse cost.
func BenchmarkParseDoc_ConfigMap(b *testing.B) {
	var data strings.Builder
	for i := range 20 {
		fmt.Fprintf(&data, "  key-%d: \"value-%d\"\n", i, i)
	}
	doc := mustDecodeSingle(b, `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
`+data.String())
	opts := defaultParseDocOptions()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := ParseDoc(doc, opts); err != nil {
			b.Fatalf("ParseDoc: %v", err)
		}
	}
}

// BenchmarkParseDoc_Secret measures the core/v1 Secret parse — the
// `data` + `stringData` walk plus the wipe-cleartext placeholder
// pass.
func BenchmarkParseDoc_Secret(b *testing.B) {
	var data strings.Builder
	for i := range 8 {
		fmt.Fprintf(&data, "  key-%d: %s\n", i, "dGVzdA==") // base64("test")
	}
	doc := mustDecodeSingle(b, `apiVersion: v1
kind: Secret
metadata:
  name: app-secrets
  namespace: default
type: Opaque
data:
`+data.String())
	opts := defaultParseDocOptions()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := ParseDoc(doc, opts); err != nil {
			b.Fatalf("ParseDoc: %v", err)
		}
	}
}

// mustDecodeSingle parses a single-document YAML literal into a map.
// Lives in the bench file so the production parse path uses the same
// DecodeDocs entry the loader does — only the per-iteration cost in
// the benchmark loop is measured.
func mustDecodeSingle(b *testing.B, body string) map[string]any {
	b.Helper()
	docs, err := SplitDocs([]byte(body))
	if err != nil {
		b.Fatalf("SplitDocs: %v\n%s", err, body)
	}
	if len(docs) != 1 {
		b.Fatalf("expected 1 doc, got %d", len(docs))
	}
	return docs[0]
}
