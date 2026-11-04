package resourceset_test

import (
	"encoding/json"
	"errors"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_Admission(t *testing.T) {
	t.Parallel()
	failure := errors.New("admission failed")
	for _, tc := range []struct {
		name string
		hit  bool
		err  error
	}{
		{"replay", true, nil},
		{"error", false, failure},
		{"template", false, manifest.ErrInput},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{
				Name:              "reader",
				Namespace:         "apps",
				ResourcesTemplate: "<< invalid template",
				Inputs: []fluxopv1.ResourceSetInput{
					{"name": jsonTmpl(t, `"inline"`)},
				},
				InputsFrom: []fluxopv1.InputProviderReference{{Name: "dynamic"}},
			}
			provider := &manifest.ResourceSetInputProvider{
				Name:      "dynamic",
				Namespace: "apps",
				Type:      fluxopv1.InputProviderGitHubBranch,
			}
			resolved := 0
			resolve := func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
				resolved++
				return []*manifest.ResourceSetInputProvider{provider}, nil
			}
			calls := 0
			docs, warnings, err := renderProviders(rs, resolve, func(any) (bool, error) {
				calls++
				if tc.name == "template" {
					return false, nil
				}
				return tc.hit, tc.err
			})
			assert.Equal(t, calls, 1)
			assert.Equal(t, resolved, 1)
			{
				assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnOfflineInputProvider ||
					warnings[0].Resource != rs.Named(), "admission lost provider warning: %+v", warnings)
			}
			{
				assert.FatalIf(t, docs != nil || (tc.name == "template" && err == nil) ||
					(tc.name != "template" && !errors.Is(err, tc.err)), "admission: documents=%v error=%v, want %v", docs, err, tc.err)
			}
		})
	}
	t.Run("canonical content stays immutable", func(t *testing.T) {
		t.Parallel()
		rs := &manifest.ResourceSet{
			Name:              "matrix",
			Namespace:         "apps",
			InputsFrom:        []fluxopv1.InputProviderReference{{Name: "images"}},
			ResourcesTemplate: "<< $_ := set inputs.nested \"value\" \"mutated\" >>apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.tag >>'}\n",
		}
		provider := &manifest.ResourceSetInputProvider{
			Name:      "images",
			Namespace: "apps",
			DefaultValues: fluxopv1.ResourceSetInput{
				"tag": jsonTmpl(t, `"selected"`), "nested": jsonTmpl(t, `{"value":"captured"}`),
			},
		}
		resolve := func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			return []*manifest.ResourceSetInputProvider{provider}, nil
		}
		var captured any
		var before []byte
		docs, _, err := renderProviders(rs, resolve, func(content any) (bool, error) {
			captured = content
			var err error
			before, err = json.Marshal(content)
			return false, err
		})
		assert.NoError(t, err)
		assert.Equal(t, len(docs), 1)
		after, err := json.Marshal(captured)
		assert.NoError(t, err)
		assert.Equal(t, string(after), string(before))
	})
	invalid := &manifest.ResourceSet{Name: "invalid", Steps: []fluxopv1.ResourceSetStep{{Name: "empty"}}}
	called := false
	_, _, err := renderProviders(invalid, nil, func(any) (bool, error) { called = true; return true, nil })
	{
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrInput) || called,
			"invalid steps reached admission: called=%v error=%v",
			called,
			err,
		)
	}
}
