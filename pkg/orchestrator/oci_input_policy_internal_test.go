package orchestrator

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_OCIInputCredentialPolicy(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                                string
		secretRef, secret, malformed, allow bool
		status                              int
		failure, unavailable                bool
	}{
		{name: "anonymous private registry", status: http.StatusUnauthorized, unavailable: true},
		{name: "default missing Secret", secretRef: true, status: http.StatusUnauthorized, failure: true},
		{name: "allowed missing Secret", secretRef: true, allow: true, status: http.StatusUnauthorized, unavailable: true},
		{name: "malformed Secret remains an error", secretRef: true, secret: true, malformed: true, allow: true, status: http.StatusUnauthorized, failure: true},
		{name: "supplied credential rejected", secretRef: true, secret: true, status: http.StatusUnauthorized, failure: true},
		{name: "unrelated registry failure", allow: true, status: http.StatusServiceUnavailable, failure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderTreesOCIInputCredentialPolicyCase(t, &tc)
		},
		)
	}
}

func testRenderTreesOCIInputCredentialPolicyCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		secretRef   bool
		secret      bool
		malformed   bool
		allow       bool
		status      int
		failure     bool
		unavailable bool
	},
) {
	t.Helper()

	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		if testCaseInput.status == http.StatusUnauthorized {
			w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
		}
		w.WriteHeader(testCaseInput.status)
	}))
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "https://")
	root := t.TempDir()
	members := "providers.yaml, set.yaml"
	if testCaseInput.secret {
		members += ", secret.yaml"
		credentials := fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":"fixture"}}}`, host)
		if testCaseInput.malformed {
			credentials = `{"auths":`
		}
		testutil.WriteFile(
			t,
			root,
			"secret.yaml",
			fmt.Sprintf(
				"apiVersion: v1\nkind: Secret\nmetadata: {name: registry-auth, namespace: apps}\nstringData:\n  .dockerconfigjson: %q\n",
				credentials,
			),
		)
	}
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: ["+members+"]\n")
	ref := ""
	if testCaseInput.secretRef {
		ref = "  secretRef: {name: registry-auth}\n"
	}
	testutil.WriteFile(t, root, "providers.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: a-static, namespace: apps}
spec:
  type: Static
  defaultValues: {tag: available}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: b-registry, namespace: apps}
spec:
  type: OCIArtifactTag
  url: oci://%s/images
  insecure: true
%s`, host, ref))
	testutil.WriteFile(t, root, "set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: consumer, namespace: apps}
spec:
  inputsFrom: [{name: a-static}, {name: b-registry}]
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: '<< inputs.tag >>'}
`)
	registryConfig := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, registryConfig, `{}`)
	cfg := Config{Concurrency: 2, RegistryConfig: registryConfig, CacheDir: t.TempDir()}
	if testCaseInput.allow {
		cfg.AllowMissingKinds = []string{manifest.KindSecret}
	}
	rendered, err := renderTreesForTest(t.Context(), t,
		[]Tree{{Path: root, RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		cfg,
	)
	assert.FatalIf(
		t,
		(err != nil) != testCaseInput.failure || len(rendered) != 1 || rendered[0].Result == nil,
		"render error=%v, want failure=%t; trees=%v",
		err,
		testCaseInput.failure,
		rendered,
	)
	rsID := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "consumer",
	}
	providerID := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSetInputProvider,
		Namespace: "apps",
		Name:      "b-registry",
	}
	info, _ := rendered[0].Store().GetStatus(rsID)
	wantStatus, wantDocs := store.StatusReady, 1
	if testCaseInput.failure {
		wantStatus, wantDocs = store.StatusFailed, 0
	}
	assert.FatalIf(
		t,
		info.Status != wantStatus || info.Outcome() == store.OutcomeSkipped,
		"consumer status=%+v, want %s without a whole-set skip",
		info,
		wantStatus,
	)
	var docs []map[string]any
	for _, batch := range outputByParent(rendered[0].Result.Manifests) {
		for _, doc := range batch {
			if manifest.DocKind(doc) == manifest.KindConfigMap {
				docs = append(docs, doc)
			}
		}
	}
	assert.FatalIf(t, len(docs) != wantDocs, "rendered %d consumer documents, want %d", len(docs), wantDocs)
	if wantDocs != 0 {
		metadata := manifest.DocMetadata(docs[0])
		name := metadata.Name
		assert.Equal(t, name, "available")
	}
	assertOCIInputWarnings(t, rendered[0].Result.Warnings, providerID, testCaseInput.unavailable)
	if (requests.Load() == 0) != testCaseInput.malformed {
		t.Fatalf("registry requests=%d, malformed=%t", requests.Load(), testCaseInput.malformed)
	}
}

func assertOCIInputWarnings(
	t *testing.T,
	warnings []manifest.Warning,
	providerID manifest.NamedResource,
	wantUnavailable bool,
) {
	t.Helper()
	var unavailable []manifest.Warning
	{
		warningValues := warnings
		for warningIndex := range warningValues {
			warning := warningValues[warningIndex]

			assert.FatalIf(
				t,
				warning.Category == manifest.WarnOfflineInputProvider,
				"registry acquisition warned offline: %+v",
				warning,
			)
			if warning.Category == manifest.WarnSourceCredentialsUnavailable {
				unavailable = append(unavailable, warning)
			}
		}
	}
	if wantUnavailable {
		assert.FatalIf(t, len(unavailable) != 1 || unavailable[0].Resource != providerID ||
			unavailable[0].Subject.ID != providerID, "unavailable provider diagnostic=%+v", unavailable)
	} else {
		assert.FatalIf(t, len(unavailable) != 0, "hard failure became unavailable: %+v", unavailable)
	}
}
