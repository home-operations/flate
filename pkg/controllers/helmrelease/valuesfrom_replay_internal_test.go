package helmrelease

import (
	"fmt"
	"os"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestPreviousRender_UnavailableValuesTransitions(t *testing.T) {
	t.Parallel()
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("mixed=%t", mixed), func(t *testing.T) {
			t.Parallel()
			for range 2 {
				testPreviousRenderUnavailableValuesTransitions(t, mixed)
			}
		})
	}
}

func testPreviousRenderUnavailableValuesTransitions(t *testing.T, mixed bool) {
	t.Helper()
	c, st := newTestControllerWithOptions(
		t,
		&base.Options{Missing: depwait.NewMissingPolicy([]string{manifest.KindSecret})},
	)
	c.Helm = helm.NewClient(cachetest.New(t, t.TempDir(), sourcecache.New))
	root := t.TempDir()
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		root,
		"chart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata:\n  value: {{ .Values.value | default \"fallback\" | quote }}\n  keep: {{ .Values.keep | default \"fallback\" | quote }}\n",
	)
	src := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(src)
	st.SetArtifact(
		src.Named(),
		&store.SourceArtifact{
			Kind:      manifest.KindGitRepository,
			FS:        os.DirFS(root),
			LocalPath: root,
			Content:   "chart-tree",
		},
	)
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady})
	hr := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		Chart:     manifest.HelmChart{Name: "chart", Source: src.Named()},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart: "chart",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       manifest.KindGitRepository,
						Name:       src.Name,
						Namespace:  src.Namespace,
						APIVersion: manifest.SourceDomain + "/v1",
					},
				},
			},
			ValuesFrom: []manifest.ValuesReference{
				{Kind: manifest.KindSecret, Name: "input", ValuesKey: "value", TargetPath: "value"},
			},
		},
	}
	if mixed {
		hr.ValuesFrom = append(
			hr.ValuesFrom,
			hr.ValuesFrom[0],
			manifest.ValuesReference{Kind: manifest.KindSecret, Name: "input", ValuesKey: "keep", TargetPath: "keep"},
		)
	}
	st.AddObject(hr)
	var previous base.History
	for _, state := range []string{"encrypted", "absent", "produced", "readable", "encrypted", "empty", "encrypted"} {
		publishUnavailableValuesState(t, c, state, mixed)
		if previous.Assessment != nil {
			assert.Equal(t, previous.Assessment.Reads.Valid(), false)
			previous.Assessment.Close()
		}
		info := reconcileFixture(t, c, hr.Named())
		if state == "empty" {
			assert.Equal(t, info.Status, store.StatusFailed)
			assert.Equal(t, strings.Contains(info.Message, `key "value" not found`), true)
			previous = c.PreviousRender(t.Context(), hr, schedule.Result{})
			assert.Equal(t, previous.Complete, false)
			continue
		}
		previous = assertUnavailableValuesState(t, c, state, mixed)
	}
	if previous.Assessment != nil {
		previous.Assessment.Close()
	}
}

func publishUnavailableValuesState(t *testing.T, c *Controller, state string, mixed bool) {
	t.Helper()
	st := c.Store
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	switch state {
	case "absent":
		st.DeleteObject(id)
	case "produced":
		st.DeleteObject(id)
		publishRawFixture(
			t,
			c,
			&manifest.RawObject{
				Kind:       "ExternalSecret",
				APIVersion: "external-secrets.io/v1",
				Name:       "cluster",
				Namespace:  id.Namespace,
				Spec:       map[string]any{"target": map[string]any{"name": id.Name}},
			},
		)
		st.UpdateStatus(testutil.KustomizationID("apps", "producer-root"), store.StatusInfo{Status: store.StatusReady})
	default:
		secret := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{}}
		if mixed {
			secret.StringData["keep"] = "retained"
		}
		var warnings []manifest.Warning
		switch state {
		case "readable":
			secret.StringData["value"] = "readable"
		case "encrypted":
			warnings = []manifest.Warning{
				{Resource: id, Category: manifest.WarnSOPSUnreadable, Detail: []string{"value"}},
			}
		}
		st.AddDeclaration(store.Declaration{Obj: secret, Warnings: warnings})
	}
}

func assertUnavailableValuesState(
	t *testing.T,
	c *Controller,
	state string,
	mixed bool,
) base.History {
	t.Helper()
	st := c.Store
	hr, _ := st.Get[*manifest.HelmRelease](testutil.HelmReleaseID("apps", "consumer"))
	info, _ := st.GetStatus(hr.Named())
	assert.FatalIf(t, info.Status != store.StatusReady, "state=%s status=%+v", state, info)
	artifact, ok := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok, "state=%s has no output", state)
	want := "fallback"
	if state == "readable" {
		want = "readable"
	}
	assert.Equal(t, renderedConfigMapValue(testutil.DocumentMaps(artifact.Manifests), "value"), want)
	if mixed && state != "absent" && state != "produced" {
		assert.Equal(t, renderedConfigMapValue(testutil.DocumentMaps(artifact.Manifests), "keep"), "retained")
	}
	var omissions []manifest.Warning
	warnings := st.Warnings()
	for i := range warnings {
		warning := &warnings[i]
		if warning.Category == manifest.WarnOmittedValues {
			omissions = append(omissions, *warning)
		}
	}
	wantOmissions := 1
	if state == "readable" {
		wantOmissions = 0
	}
	assert.Equal(t, len(omissions), wantOmissions)
	if len(omissions) != 0 {
		assert.Equal(t, strings.Contains(omissions[0].Message, "SOPS-encrypted"), state == "encrypted")
		assert.Equal(t, strings.Contains(omissions[0].Message, "produced in the cluster"), state == "produced")
	}
	previous := c.PreviousRender(t.Context(), hr, schedule.Result{})
	assert.Equal(t, previous.Complete, true)
	return previous
}
