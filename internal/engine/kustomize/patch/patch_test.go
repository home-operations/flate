package patch_test

import (
	"bytes"
	"strings"
	"testing"

	fluxapis "github.com/fluxcd/pkg/apis/kustomize"

	"github.com/home-operations/flate/internal/engine/kustomize/patch"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

const syncDocuments = `apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: OCIRepository
metadata: {name: sync, namespace: control, labels: {role: source}}
spec: {url: "oci://example.invalid/first"}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sync, namespace: control}
spec: {path: ./first, prune: true, sourceRef: {kind: OCIRepository, name: sync}}
`

func TestApplyPatches_SyncDocuments(t *testing.T) {
	t.Parallel()
	rename := func(selector, name string) fluxapis.Patch {
		return fluxapis.Patch{
			Target: &fluxapis.Selector{Kind: manifest.KindOCIRepository, LabelSelector: selector},
			Patch:  "- op: replace\n  path: /metadata/name\n  value: " + name,
		}
	}
	first := rename("role=source", "middle")
	second := rename("role=source", "final")
	implicit := fluxapis.Patch{Patch: `apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: OCIRepository
metadata: {name: middle, namespace: control}
spec: {url: "oci://example.invalid/last"}
`}
	missing := fluxapis.Patch{Patch: "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: source-controller}"}
	root := fluxapis.Patch{Patch: `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sync, namespace: control}
spec:
  $patch: replace
  path: ./last
  sourceRef: {kind: OCIRepository, name: final}
`}
	for _, tt := range []struct {
		name   string
		input  string
		list   []fluxapis.Patch
		err    string
		verify func(*testing.T, []manifest.DocOccurrence)
	}{
		{
			name: "historical identity and ordered mixed patches", input: syncDocuments,
			list: []fluxapis.Patch{first, missing, second, implicit, root},
			verify: func(t *testing.T, docs []manifest.DocOccurrence) {
				t.Helper()
				assert.Equal(t, len(docs), 2)
				assert.Equal(t, manifest.DocIdentity(docs[0].Document).Name, "final")
				assert.Diff(t, docs[0].Document["spec"], map[string]any{"url": "oci://example.invalid/last"})
				assert.Diff(t, docs[1].Document["spec"], map[string]any{
					"path": "./last", "sourceRef": map[string]any{"kind": "OCIRepository", "name": "final"},
				})
			},
		},
		{
			name: "ambiguous historical identity",
			input: syncDocuments + `---
apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: OCIRepository
metadata: {name: alternate, namespace: control, labels: {role: other}}
spec: {url: "oci://example.invalid/other"}
`,
			list: []fluxapis.Patch{first, second, rename("role=other", "middle"), implicit},
			err:  "multiple matches",
		},
		{
			name: "namespace mismatch", input: syncDocuments,
			list: []fluxapis.Patch{{Patch: strings.ReplaceAll(root.Patch, "namespace: control", "namespace: other")}},
			verify: func(t *testing.T, docs []manifest.DocOccurrence) {
				t.Helper()
				assert.Equal(t, docs[1].Document["spec"].(map[string]any)["path"], any("./first"))
			},
		},
		{
			name: "delete source and root", input: syncDocuments,
			list: []fluxapis.Patch{{Patch: `$patch: delete
apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: OCIRepository
metadata: {name: sync, namespace: control}
---
$patch: delete
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sync, namespace: control}
`}},
			verify: func(t *testing.T, docs []manifest.DocOccurrence) {
				t.Helper()
				assert.Equal(t, len(docs), 0)
			},
		},
		{
			name: "malformed input", input: syncDocuments,
			list: []fluxapis.Patch{{Patch: "["}}, err: "unable to parse",
		},
		{
			name: "missing JSON target", input: syncDocuments,
			list: []fluxapis.Patch{{Patch: "- op: remove\n  path: /spec"}}, err: "must specify a target",
		},
		{
			name: "invalid selector", input: syncDocuments,
			list: []fluxapis.Patch{{Patch: root.Patch, Target: &fluxapis.Selector{Name: "["}}},
			err:  "error parsing regexp",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			raw, err := patch.ApplyPatches(&patch.PatchRequest{
				Documents: []byte(tt.input), Patches: tt.list, Partial: true,
			})
			if tt.err != "" {
				assert.FatalIf(
					t,
					err == nil || !strings.Contains(err.Error(), tt.err),
					"error = %v, want %q",
					err,
					tt.err,
				)
				return
			}
			assert.NoError(t, err)
			docs, err := manifest.DecodeDocs(bytes.NewReader(raw), manifest.RequiredDocuments)
			assert.NoError(t, err)
			tt.verify(t, docs)
		})
	}
}

func TestApplyPatches_CompleteDocuments(t *testing.T) {
	t.Parallel()
	raw, err := patch.ApplyPatches(&patch.PatchRequest{
		Documents: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: example}\ndata: {value: before}\n"),
		Patches: []fluxapis.Patch{{Patch: `apiVersion: v1
kind: ConfigMap
metadata: {name: example}
data: {value: middle}
`}, {
			Target: &fluxapis.Selector{Kind: "ConfigMap", Name: "example"},
			Patch:  "- op: replace\n  path: /data/value\n  value: after",
		}},
	})
	assert.NoError(t, err)
	assert.Equal(t, string(raw), "apiVersion: v1\ndata:\n  value: after\nkind: ConfigMap\nmetadata:\n  name: example\n")

	_, err = patch.ApplyPatches(&patch.PatchRequest{Documents: []byte(syncDocuments), Patches: []fluxapis.Patch{{
		Patch: "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: missing}",
	}}})
	assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), "no matches"), "unexpected error: %v", err)
}
