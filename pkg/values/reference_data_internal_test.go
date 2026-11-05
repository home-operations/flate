package values

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestStoreProvider_OfflineCandidatePrecedenceAndWithdrawal(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	first := &manifest.Kustomization{Name: "z-first", Namespace: "apps"}
	second := &manifest.Kustomization{Name: "a-second", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{first, second})
	p := manifest.Production{Owner: first.Named(), Source: manifest.BootstrapSourceID, File: "first.yaml"}
	q := manifest.Production{Owner: second.Named(), Source: manifest.BootstrapSourceID, File: "second.yaml"}
	one := manifest.NewSecret(target, nil, map[string]any{"FIRST": "first"})
	two := manifest.NewSecret(target, nil, map[string]any{"SECOND": "second"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions: []manifest.Production{p, q},
			ReferenceData: []manifest.ReferenceData{
				{Production: p, OfflineData: one},
				{Production: q, OfflineData: two},
			},
		},
	}})
	supplied := manifest.NewSecret(target, nil, map[string]any{"FIRST": "supplied", "EXTRA": "extra"})
	st.AddDeclaration(
		store.Declaration{
			Obj:       supplied,
			Doc:       testutil.ObjectDocument(supplied),
			Origin:    manifest.SuppliedDeclaration,
			InputPath: "input.yaml",
		},
	)
	data := NewStoreProvider(st).Data(target)
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"EXTRA", "FIRST"})
	result1, err := data.Value("FIRST")
	value := result1.Value
	assert.NoError(t, err)
	assert.Equal(t, value, "first")
	_, _, firstOutput := st.GetEmission(first.Named())
	defer firstOutput.Finish()
	firstOutput.Complete = true
	st.RecordEmitted(first.Named(), &firstOutput, nil)
	data = NewStoreProvider(st).Data(target)
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"EXTRA", "FIRST", "SECOND"})
	result2, err := data.Value("SECOND")
	value = result2.Value
	assert.NoError(t, err)
	assert.Equal(t, value, "second")
	_, _, secondOutput := st.GetEmission(second.Named())
	defer secondOutput.Finish()
	secondOutput.Complete = true
	st.RecordEmitted(second.Named(), &secondOutput, nil)
	assert.Diff(t, NewStoreProvider(st).Data(target).Snapshot(), supplied.Snapshot())

	assert.FatalIf(
		t,
		!st.ReportHidden(target) || len(st.ListObjects("", manifest.KindSecret)) != 1,
		"%s",
		"offline data entered inventory",
	)
}

func TestStoreProvider_OfflineSourceAndRealDataInvalidateCapture(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	proof := manifest.Production{
		Source:   manifest.BootstrapSourceID,
		Path:     "input.yaml",
		File:     "input.yaml",
		Authored: target,
	}
	const cipher = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	result3, err := manifest.ParseDoc(map[string]any{
		"apiVersion": "v1", "kind": manifest.KindConfigMap,
		"metadata": map[string]any{"name": target.Name, "namespace": target.Namespace},
		"data":     map[string]any{"VALUE": "offline", "ZULU": cipher, "ALPHA": cipher},
	}, manifest.ParseDocOptions{})
	parsed, warnings := result3.Object, result3.Warnings

	assert.NoError(t, err)
	offline := parsed.(manifest.Data)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions:   []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{{Production: proof, OfflineData: offline, Warnings: warnings}},
		},
	}})
	st.AddObject(&manifest.Kustomization{Name: "consumer", Namespace: "apps"})
	var reads store.ReadSet
	reads.Init(
		st,
		manifest.NamedResource{
			Group:     manifest.FluxKustomizeDomain,
			Kind:      manifest.KindKustomization,
			Namespace: "apps",
			Name:      "consumer",
		},
		nil,
	)
	reads.GetObject(reads.ID())
	defer reads.Close()
	provider := NewStoreProvider(&reads)
	assert.FatalIf(t, provider.Data(target) != offline, "%s", "offline candidate unavailable")

	assert.Equal(t, len(st.Warnings()), 0)
	reads.Accept(store.AssessmentReconciled)
	advisories := st.Warnings()
	assert.FatalIf(t, len(advisories) != 1 || advisories[0].Category != manifest.WarnSOPSUnreadable ||
		advisories[0].Resource != target ||
		advisories[0].Count != 1 ||
		!slices.Equal(advisories[0].Detail, []string{"ALPHA", "ZULU"}),
		"accepted offline warnings: %+v", advisories)

	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Content: "new-source"})
	assert.FatalIf(t, reads.Valid(), "%s", "source arrival did not invalidate offline data")

	assert.Equal(t, len(st.Warnings()), 0)
	var fresh store.ReadSet
	fresh.Init(st, reads.ID(), nil)
	defer fresh.Close()
	next := NewStoreProvider(&fresh)
	assert.FatalIf(t, next.Data(target) != offline, "%s", "fresh lookup lost candidate")

	actual := manifest.NewConfigMap(target, map[string]any{"VALUE": "real"})
	st.AddObject(actual)
	assert.FatalIf(t, fresh.Valid(), "%s", "real data arrival did not invalidate offline capture")
	assert.FatalIf(
		t,
		provider.Data(target) != offline || next.Data(target) != offline,
		"%s",
		"captured lookup changed during expansion",
	)
	assert.FatalIf(t, NewStoreProvider(st).Data(target) != actual, "%s", "fresh lookup retained offline value")
}

func TestStoreProvider_PendingIndexedKeysAwaitPublication(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	st.AddObject(owner)
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	declaration := &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "external",
		Namespace:  "apps",
		Spec: map[string]any{
			"target": map[string]any{"name": target.Name},
			"data":   []any{map[string]any{"secretKey": "CURRENT"}},
		},
	}
	proof := manifest.Production{
		Owner:       owner.Named(),
		Declaration: declaration.Named(),
		Source:      manifest.BootstrapSourceID,
	}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions:   []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{{Production: proof}},
		},
	}})
	var reads store.ReadSet
	reads.Init(
		st,
		manifest.NamedResource{
			Group:     manifest.FluxKustomizeDomain,
			Kind:      manifest.KindKustomization,
			Name:      "consumer",
			Namespace: "apps",
		},
		nil,
	)
	defer reads.Close()
	input := NewStoreProvider(&reads).Lookup(target)
	assert.FatalIf(t, input.Data != nil || !input.Generated, "%s", "unsettled producer fabricated data")

	assert.Diff(t, input.Materializers, []manifest.NamedResource{owner.Named()})
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []store.Child{{Obj: declaration}}, true
	st.RecordEmitted(owner.Named(), &output, nil)
	assert.FatalIf(t, reads.Valid(), "%s", "producer publication did not invalidate pending lookup")

	var fresh store.ReadSet
	fresh.Init(st, reads.ID(), nil)
	defer fresh.Close()
	data := NewStoreProvider(&fresh).Data(target)
	assert.FatalIf(t, data != nil, "%s", "settled declaration fabricated data")
	assert.FatalIf(t, st.GetObject(target) != nil, "%s", "remote declaration entered inventory")
}
