package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

func TestFinishSelected_WarningScope(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "config"}
	object := manifest.Warning{
		Resource: id, Category: manifest.WarnInputParseFailed,
		Message: "input parse failed", Detail: []string{"token"}, Count: 1,
	}
	consumer := manifest.Warning{
		Resource: testutil.KustomizationID("apps", "consumer"),
		Category: manifest.WarnUnresolvedSubstitution, Message: "absent input", Related: []manifest.NamedResource{id},
	}
	inv := selectedInvocation{}
	expected := output.Report{}
	for group, cluster := range []string{"alpha", "beta"} {
		selected := selectedGroup{cluster: cluster}
		expected.Groups = append(expected.Groups, output.ReportGroup{Cluster: cluster})
		for _, context := range []string{"orig snapshot", "current snapshot"} {
			selected.sides = append(selected.sides, &selection.Result{
				Warnings: []manifest.Warning{object, consumer},
				Subjects: map[manifest.NamedResource]manifest.DiagnosticIdentity{id: manifest.DiagnosticFor(id, nil)},
			})
			expected.Warnings = append(expected.Warnings,
				output.Warning{Warning: object, Group: group, Context: context},
				output.Warning{Warning: consumer, Group: group, Context: context})
		}
		inv.groups = append(inv.groups, selected)
	}
	var stderr, want bytes.Buffer
	assert.NoError(
		t,
		finishSelected(&selectedRequest{pair: true, stdout: io.Discard, stderr: &stderr}, &inv, &selectedPayload{}),
	)
	expected.Prepare(nil)
	assert.NoError(t, (output.Encoder{Writer: &want}).Report(&expected, output.Encoder{Writer: io.Discard}))
	assert.Diff(t, stderr.String(), want.String())
	for _, group := range inv.groups {
		for _, side := range group.sides {
			assert.Diff(t, side.Warnings, []manifest.Warning{object, consumer})
		}
	}
}

func TestCollectRendered_OrderAndImmutability(t *testing.T) {
	t.Parallel()
	secret := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "z"}}
	config := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}
	identities := []manifest.NamedResource{
		{Kind: manifest.KindSecret, Namespace: "other", Name: "z"},
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "a"},
	}
	result := &selection.Result{Manifests: []manifest.DocumentBatch{{
		Parent:    &manifest.Kustomization{Name: "report", Namespace: "reporting"},
		Documents: []map[string]any{secret, config}, Identities: identities,
	}}}
	for _, tc := range []struct {
		name  string
		kinds []string
		want  []int
	}{
		{name: "sorted", want: []int{1, 0}},
		{name: "only secret", kinds: []string{manifest.KindSecret}, want: []int{0}},
		{name: "excluded", kinds: []string{"Deployment"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				docs, err := collectRendered(result, &buildFlags{onlyKinds: tc.kinds})
				assert.NoError(t, err)
				assert.Equal(t, len(docs), len(tc.want))
				for i, position := range tc.want {
					assert.Diff(t, docs[i], result.Manifests[0].Documents[position])
				}
				assert.Diff(t, result.Manifests[0].Documents, []map[string]any{secret, config})
				assert.Diff(t, result.Manifests[0].Identities, identities)
			}
		})
	}
	derived := []manifest.NamedResource{
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "a"},
		{Kind: manifest.KindSecret, Namespace: "apps", Name: "z"},
	}
	for _, tc := range []struct {
		name       string
		batches    []manifest.DocumentBatch
		kinds      []string
		documents  []map[string]any
		identities []manifest.NamedResource
		invalid    bool
	}{
		{name: "omitted", batches: []manifest.DocumentBatch{{Parent: &manifest.ResourceSet{Namespace: "apps"}, Documents: []map[string]any{secret, config}}}, documents: []map[string]any{config, secret}, identities: derived},
		{name: "empty identities", batches: []manifest.DocumentBatch{{Parent: &manifest.ResourceSet{Namespace: "apps"}, Documents: []map[string]any{secret, config}, Identities: []manifest.NamedResource{}}}, documents: []map[string]any{config, secret}, identities: derived},
		{name: "no parent", batches: []manifest.DocumentBatch{{Documents: []map[string]any{secret, config}}}, documents: []map[string]any{config, secret}, identities: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Name: "a"}, {Kind: manifest.KindSecret, Name: "z"}}},
		{name: "mixed", batches: []manifest.DocumentBatch{result.Manifests[0], {Parent: &manifest.ResourceSet{Namespace: "apps"}, Documents: []map[string]any{secret, config}}}, kinds: []string{manifest.KindSecret}, documents: []map[string]any{secret, secret}, identities: []manifest.NamedResource{identities[0], derived[1]}},
		{name: "malformed", batches: []manifest.DocumentBatch{{Documents: []map[string]any{secret, config}, Identities: identities[:1]}}, invalid: true},
		{name: "excluded malformed", batches: []manifest.DocumentBatch{{Documents: []map[string]any{secret, config}, Identities: identities[:1]}}, kinds: []string{"Deployment"}, invalid: true},
		{name: "empty malformed after valid batch", batches: []manifest.DocumentBatch{result.Manifests[0], {Identities: identities}}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := &selection.Result{Manifests: tc.batches}
			before, err := json.Marshal(input.Manifests)
			assert.NoError(t, err)
			for range 2 {
				docs, err := collectRendered(input, &buildFlags{onlyKinds: tc.kinds})
				if tc.invalid {
					assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
					assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				} else {
					assert.NoError(t, err)
				}
				assert.Diff(t, docs, tc.documents)
				after, err := json.Marshal(input.Manifests)
				assert.NoError(t, err)
				assert.Diff(t, after, before)
			}
		})
	}
}

//nolint:paralleltest // Run calls setLogLevel and updates the process egress policy.
func TestRun_OutputMembership(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"flux.yaml",
		testutil.KustomizationYAML(
			"root",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: config, namespace: apps}\ndata: {token: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n",
	)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "build", args: []string{"build"}},
		{name: "filtered build", args: []string{"build", "--only-kinds", "Deployment"}},
		{name: "resource rows", args: []string{"get", "ks"}},
		{name: "images", args: []string{"get", "images"}},
		{name: "test roster", args: []string{"test", "ks"}},
		{name: "document diff", args: []string{"diff", "--path-orig", t.TempDir(), "--output", "json"}},
		{name: "brief diff", args: []string{"diff", "--path-orig", t.TempDir(), "--output", "brief"}},
		{name: "image diff", args: []string{"diff", "images", "--path-orig", t.TempDir()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append(
				slices.Clone(tc.args),
				"--path",
				root,
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
				"--no-progress",
			)
			result := runCLI(t, args...)
			assert.Equal(t, result.code, 0)
			assert.Equal(t, strings.Contains(result.stderr, "encrypted"), false)
		})
	}
}
