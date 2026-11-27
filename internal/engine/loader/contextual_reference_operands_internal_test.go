package loader

import (
	"slices"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSubstitutionResolution_ReferenceOperands(t *testing.T) {
	t.Parallel()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"ACTIVE": "true"}}
	st := store.New()
	st.AddObject(owner)
	l := New(st)
	l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
	raw := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: apps}
spec:
  chart:
    spec:
      chart: "./${CHART:-charts/app}"
      sourceRef: {kind: GitRepository, name: "${SOURCE:-repo}"}
      valuesFiles: ["${FILE:-values.yaml}"]
  valuesFrom: [{kind: ConfigMap, name: "${INPUT:-vars}", valuesKey: "${KEY:-values.yaml}", optional: true}]
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: external, namespace: apps}
spec:
  target: {name: "${SECRET:-credentials}"}
  data: [{secretKey: "${KEY:-token}", remoteRef: {key: remote}}]
`
	resolution := substitutionResolution{loader: l}
	for raw := range strings.SplitSeq(raw, "\n---\n") {
		var doc map[string]any
		assert.NoError(t, yaml.NewDecoder(strings.NewReader(raw)).Decode(&doc))
		child := new(
			documentReader,
		).prepareStreamObject(manifest.ParseDocOptions{}, &manifest.DocOccurrence{Document: doc})
		assert.NoError(t, child.outcome.Err)
		fact := new(occurrenceFacts).compactOccurrence(child.Obj, manifest.TemplateProjection{})
		projected, err := resolution.resolveOccurrence(
			t.Context(),
			&fact,
			&graphVisit{owner: owner.Named()},
			child.version,
		)
		assert.NoError(t, err)
		if child.Obj.Named().Name == "release" {
			uses := []string{}
			for ref := range projected.references() {
				uses = append(uses, ref.Use)
				switch ref.Use {
				case "source":
					assert.Equal(t, ref.Name, "repo")
					assert.Equal(t, ref.ChartPath, "./charts/app")
					assert.Diff(t, ref.Files, []string{"values.yaml"})
				case "valuesFrom":
					assert.Equal(t, ref.Name, "vars")
					assert.Equal(t, ref.Key, "values.yaml")
					assert.Equal(t, ref.Optional, true)
				}
			}
			assert.Equal(t, slices.Contains(uses, "source"), true)
			assert.Equal(t, slices.Contains(uses, "valuesFrom"), true)
			for ref := range fact.references() {
				if ref.Use == "valuesFrom" {
					assert.Equal(t, ref.Name, "${INPUT:-vars}")
					assert.Equal(t, ref.Key, "${KEY:-values.yaml}")
				}
			}
		} else {
			assert.Equal(t, projected.facts.targets[0].Name, "credentials")
			assert.Diff(t, projected.facts.targets[0].Keys, manifest.KeySet{Names: []string{"token"}, Complete: true})
			assert.Equal(t, fact.facts.targets[0].Name, "${SECRET:-credentials}")
		}
	}
}
