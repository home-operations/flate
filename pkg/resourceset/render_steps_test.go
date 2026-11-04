package resourceset_test

import (
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_Steps(t *testing.T) {
	t.Parallel()
	resource := jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"first"}}`)
	template := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: second}"
	for _, tt := range []struct {
		name    string
		spec    fluxopv1.ResourceSetSpec
		names   []string
		wantErr string
	}{
		{name: "ordered steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource}},
			{Name: "config", ResourcesTemplate: template},
		}}, names: []string{"first", "second"}},
		{name: "dedup within step", spec: fluxopv1.ResourceSetSpec{Inputs: []fluxopv1.ResourceSetInput{{}}, Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource, resource}},
		}}, names: []string{"first"}},
		{name: "no-input duplicate inside a step keeps both", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource, resource}},
		}}, names: []string{"first", "first"}},
		{name: "duplicate across steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "first", Resources: []*apix.JSON{resource}},
			{Name: "second", Resources: []*apix.JSON{jsonTmpl(t, `{"apiVersion":"apps/v1beta2","kind":"Deployment","metadata":{"name":"first"}}`)}},
		}}, wantErr: "duplicate resource"},
		{name: "empty identity across steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "first", Resources: []*apix.JSON{jsonTmpl(t, `{}`)}},
			{Name: "second", Resources: []*apix.JSON{jsonTmpl(t, `{}`)}},
		}}, wantErr: "duplicate resource"},
		{name: "steps and resources", spec: fluxopv1.ResourceSetSpec{Resources: []*apix.JSON{resource}, Steps: []fluxopv1.ResourceSetStep{{Name: "deploy", Resources: []*apix.JSON{resource}}}}, wantErr: "mutually exclusive"},
		{name: "steps and template", spec: fluxopv1.ResourceSetSpec{ResourcesTemplate: template, Steps: []fluxopv1.ResourceSetStep{{Name: "deploy", Resources: []*apix.JSON{resource}}}}, wantErr: "mutually exclusive"},
		{name: "empty step with empty providers", spec: fluxopv1.ResourceSetSpec{InputsFrom: []fluxopv1.InputProviderReference{{Name: "empty"}}, Steps: []fluxopv1.ResourceSetStep{{Name: "empty"}}}, wantErr: "at least one"},
		{name: "step template error", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{{Name: "bad", ResourcesTemplate: "<< inputs.missing >>"}}}, wantErr: `step "bad"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{Name: "steps", Namespace: "default", ResourceSetSpec: tt.spec}
			docs, _, err := renderProviders(rs, nil, nil)
			if tt.wantErr != "" {
				{
					assert.FatalIf(
						t,
						err == nil || !strings.Contains(err.Error(), tt.wantErr),
						"error = %v, want %q",
						err,
						tt.wantErr,
					)
				}
				{
					assert.FatalIf(t, strings.Contains(err.Error(), "ResourceSet default/steps") ||
						strings.Contains(err.Error(), "flux error") ||
						strings.Contains(
							err.Error(),
							"input error",
						), "step diagnostic repeated owner or classification: %v", err)
				}
				return
			}
			{
				assert.NoError(t, err)
			}
			names := make([]string, 0, len(docs))
			rs.EmissionPolicy().Stamp(docs)
			for _, doc := range docs {
				name, ns := manifest.DocMetadata(doc.Document)
				{
					assert.ErrorIf(t, ns != "", "namespace = %q", ns)
				}
				labels := doc.Document["metadata"].(map[string]any)["labels"].(map[string]any)
				{
					assert.ErrorIf(t, labels[fluxopv1.OwnerLabelResourceSetName] != "steps", "labels = %v", labels)
				}
				names = append(names, name)
			}
			assert.Diff(t, names, tt.names)
		})
	}
}
