package kustomization

import (
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_OfflineFailureBeforePublication(t *testing.T) {
	t.Parallel()
	c, st, root := newControllerWithFixture(t)
	testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: healthy}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: '${TOKEN}'}
`)
	owner := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "input"},
		}},
	}
	testutil.WriteFile(t, root, "dependent/kustomization.yaml", "resources: []\n")
	st.AddObject(owner)
	st.AddObject(&manifest.Secret{
		Name: "input", Namespace: "flux-system",
		StringData: map[string]any{"TOKEN": "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"},
	})
	var reports []store.StatusInfo
	stop := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, value any) {
		if id == owner.Named() {
			info := value.(store.StatusInfo)
			if strings.HasPrefix(info.Message, "cannot render offline") {
				reports = append(reports, info)
			}
		}
	}, false)
	defer stop()
	controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: c.Tasks, Store: st, ID: owner.Named(), ReconcileNode: c.ReconcileNode,
	})
	for range 2 {
		info, _ := st.GetStatus(owner.Named())
		assert.Equal(t, info.Status, store.StatusReady)
		assert.Equal(t, info.Reason, store.ReasonSkipped)
		assert.Equal(t, info.Outcome(), store.OutcomeSkipped)
		assert.FatalIf(
			t,
			!strings.HasPrefix(
				info.Message,
				"cannot render offline: unreadable inputs substituteFrom Secret flux-system/input key \"TOKEN\" (reference 1):",
			),
			"report=%s",
			info.Message,
		)
		assert.Equal(
			t,
			st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "healthy"}) == nil,
			true,
		)
		_, _, output := st.GetEmission(owner.Named())
		assert.Equal(t, len(output.Children), 0)
		assert.Equal(t, len(output.Manifests), 0)
		assert.Equal(t, output.Render == nil, true)
		output.Finish()
		probe := c.PreviousRender(t.Context(), owner, schedule.Result{})
		assert.Equal(t, probe.Complete, false)
		assert.Equal(t, probe.Records == nil, true)
		probe.Assessment.Close()
	}
	assert.Equal(t, len(reports), 1)
	dependent := &manifest.Kustomization{
		Name: "dependent", Namespace: owner.Namespace, Path: "./dependent",
		DependsOn: []kustomizev1.DependencyReference{{Name: owner.Name}},
	}
	st.AddObject(dependent)
	dependentInfo := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: dependent.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(t, dependentInfo.Outcome() != store.OutcomePassed, "dependent=%+v", dependentInfo)
	assert.Equal(t, len(st.Warnings()), 0)
	assert.Equal(t, c.Tasks.Failures(), int64(0))
}

func TestRender_ReadableSiblingFailureRemainsFatal(t *testing.T) {
	t.Parallel()
	c, st, root := newControllerWithFixture(t)
	testutil.WriteFile(t, root, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: healthy}
data: {token: '${TOKEN}'}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: failing}
data: {number: '${READABLE}'}
`)
	owner := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps",
		PostBuild: &kustomizev1.PostBuild{
			Substitute:     map[string]string{"READABLE": "["},
			SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindSecret, Name: "input"}},
		},
	}
	st.AddObject(owner)
	st.AddObject(&manifest.Secret{
		Name: "input", Namespace: "flux-system",
		StringData: map[string]any{"TOKEN": "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"},
	})
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: c.Tasks, Store: st, ID: owner.Named(), ReconcileNode: c.ReconcileNode,
	})
	assert.Equal(t, info.Status, store.StatusFailed)
	assert.Equal(t, strings.Contains(info.Message, "cannot render offline"), false)
}

func TestRender_UnreadableFailureBeforeReadableFailureRemainsFatal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		readable bool
	}{
		{name: "mixed failures", readable: true},
		{name: "unreadable only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, st, root := newControllerWithFixture(t)
			resource := `apiVersion: v1
kind: ConfigMap
metadata: {name: a-unreadable}
data: {'${TOKEN}': value}
`
			if tc.readable {
				resource += `---
apiVersion: v1
kind: ConfigMap
metadata: {name: b-readable}
data: {number: '${READABLE}'}
`
			}
			testutil.WriteFile(t, root, "apps/cm.yaml", resource)
			owner := &manifest.Kustomization{
				Name: "apps", Namespace: "flux-system", Path: "./apps",
				PostBuild: &kustomizev1.PostBuild{
					Substitute:     map[string]string{"READABLE": "["},
					SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindSecret, Name: "input"}},
				},
			}
			st.AddObject(owner)
			st.AddObject(&manifest.Secret{
				Name: "input", Namespace: "flux-system",
				StringData: map[string]any{"TOKEN": "ENC[AES256_GCM,data:secret,iv:x,tag:y,type:str]"},
			})
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
				Tasks: c.Tasks, Store: st, ID: owner.Named(), ReconcileNode: c.ReconcileNode,
			})
			if tc.readable {
				assert.Equal(t, info.Status, store.StatusFailed)
				assert.Equal(t, strings.Contains(info.Message, "cannot render offline"), false)
			} else {
				assert.Equal(t, info.Status, store.StatusReady)
				assert.Equal(t, info.Reason, store.ReasonSkipped)
				assert.Equal(t, strings.HasPrefix(
					info.Message,
					"cannot render offline: unreadable inputs substituteFrom Secret flux-system/input key \"TOKEN\" (reference 1):",
				), true)
			}
		})
	}
}
