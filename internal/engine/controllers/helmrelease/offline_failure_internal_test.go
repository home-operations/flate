package helmrelease

import (
	"os"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_OfflineFailureBeforePublication(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		template   string
		unreadable bool
		want       store.Outcome
	}{
		{name: "template", template: `{{ required "token required" .Values.token }}`, unreadable: true, want: store.OutcomeSkipped},
		{name: "document outcome", template: "null\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: broken}\ndata: {key: first, key: second}\n", unreadable: true, want: store.OutcomeSkipped},
		{name: "child parsing", template: "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: broken}\nspec: {path: {invalid: mapping}}\n", unreadable: true, want: store.OutcomeSkipped},
		{name: "readable", template: `{{ required "token required" .Values.token }}`, want: store.OutcomeFailed},
		{name: "successful unreadable", template: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fallback}\n", unreadable: true, want: store.OutcomePassed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, st := newTestController(t, nil)
			root := t.TempDir()
			testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: chart\nversion: 0.1.0\n")
			testutil.WriteFile(t, root, "chart/templates/failing.yaml", tc.template)
			testutil.WriteFile(
				t,
				root,
				"chart/templates/healthy.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy}\n",
			)
			source := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
			st.AddObject(source)
			st.SetArtifact(
				source.Named(),
				&store.SourceArtifact{Kind: manifest.KindGitRepository, FS: os.DirFS(root), LocalPath: root},
			)
			st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
			value := "token: ''\n"
			if tc.unreadable {
				value = "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"
			}
			st.AddObject(
				&manifest.Secret{Name: "input", Namespace: "apps", StringData: map[string]any{"values.yaml": value}},
			)
			owner := &manifest.HelmRelease{
				Name: "release", Namespace: "apps",
				Chart: manifest.HelmChart{Name: "chart", Source: source.Named()},
				HelmReleaseSpec: helmv2.HelmReleaseSpec{
					Chart: &helmv2.HelmChartTemplate{
						Spec: helmv2.HelmChartTemplateSpec{
							Chart: "chart",
							SourceRef: &helmv2.CrossNamespaceObjectReference{
								Kind:      manifest.KindGitRepository,
								Name:      source.Name,
								Namespace: source.Namespace,
							},
						},
					},
					ValuesFrom: []manifest.ValuesReference{
						{Kind: manifest.KindSecret, Name: "absent", Optional: true},
						{Kind: manifest.KindSecret, Name: "input"},
					},
				},
			}
			st.AddObject(owner)
			info := reconcileFixture(t, c, owner.Named())
			assert.Equal(t, info.Outcome(), tc.want)
			if tc.want == store.OutcomeSkipped {
				assert.FatalIf(
					t,
					!strings.HasPrefix(
						info.Message,
						"cannot render offline: unreadable inputs valuesFrom Secret apps/input key \"values.yaml\" (reference 2):",
					),
					"report=%s",
					info.Message,
				)
				_, _, output := st.GetEmission(owner.Named())
				assert.Equal(t, len(output.Children), 0)
				assert.Equal(t, len(output.Manifests), 0)
				assert.Equal(t, output.Render == nil, true)
				output.Finish()
				assert.Equal(
					t,
					st.GetObject(
						manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "healthy"},
					) == nil,
					true,
				)
				probe := c.PreviousRender(t.Context(), owner, schedule.Result{})
				assert.Equal(t, probe.Complete, false)
				assert.Equal(t, probe.Records == nil, true)
				probe.Assessment.Close()
			} else {
				assert.Equal(t, strings.Contains(info.Message, "cannot render offline"), false)
			}
			assert.Equal(t, len(st.Warnings()), 0)
		})
	}
}
