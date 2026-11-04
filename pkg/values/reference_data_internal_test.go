package values

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestStoreProvider_IndexOnlyDeclaredKeys(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		keys []string
		wipe bool
	}{
		{name: "known", keys: []string{"VALUE"}, wipe: true},
		{name: "unknown", wipe: true},
		{name: "wipe_disabled", keys: []string{"VALUE"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testStoreProviderIndexOnlyDeclaredKeysCase(t, &tc)
		},
		)
	}
}

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
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: supplied, Path: "input.yaml"}})
	assert.NoError(t, err)
	st.SetSupplied(snapshot)
	data := NewStoreProvider(st, true).Data(target)
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"EXTRA", "FIRST"})
	value, _, err := data.Value("FIRST")
	assert.NoError(t, err)
	assert.Equal(t, value, "first")
	_, _, firstOutput := st.GetEmission(first.Named())
	defer firstOutput.Finish()
	firstOutput.Complete = true
	st.RecordEmitted(first.Named(), &firstOutput, nil)
	data = NewStoreProvider(st, true).Data(target)
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"EXTRA", "FIRST", "SECOND"})
	value, _, err = data.Value("SECOND")
	assert.NoError(t, err)
	assert.Equal(t, value, "second")
	_, _, secondOutput := st.GetEmission(second.Named())
	defer secondOutput.Finish()
	secondOutput.Complete = true
	st.RecordEmitted(second.Named(), &secondOutput, nil)
	assert.Diff(t, NewStoreProvider(st, true).Data(target).Snapshot(), supplied.Snapshot())
	{
		assert.FatalIf(
			t,
			st.GetObject(target) != nil || len(st.ListObjects("", manifest.KindSecret)) != 0,
			"%s",
			"offline data entered inventory",
		)
	}
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
	offline := manifest.NewConfigMap(target, map[string]any{"VALUE": "offline"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions:   []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{{Production: proof, OfflineData: offline}},
		},
	}})
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
	defer reads.Close()
	provider := NewStoreProvider(&reads, true)
	{
		assert.FatalIf(t, provider.Data(target) != offline, "%s", "offline candidate unavailable")
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Content: "new-source"})
	{
		assert.FatalIf(t, reads.Valid(), "%s", "source arrival did not invalidate offline data")
	}
	var fresh store.ReadSet
	fresh.Init(st, reads.ID(), nil)
	defer fresh.Close()
	next := NewStoreProvider(&fresh, true)
	{
		assert.FatalIf(t, next.Data(target) != offline, "%s", "fresh lookup lost candidate")
	}
	actual := manifest.NewConfigMap(target, map[string]any{"VALUE": "real"})
	st.AddObject(actual)
	{
		assert.FatalIf(t, fresh.Valid(), "%s", "real data arrival did not invalidate offline capture")
	}
	{
		assert.FatalIf(
			t,
			provider.Data(target) != offline || next.Data(target) != offline,
			"%s",
			"captured lookup changed during expansion",
		)
	}
	{
		assert.FatalIf(
			t,
			NewStoreProvider(st, true).Data(target) != actual,
			"%s",
			"fresh lookup retained offline value",
		)
	}
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
			ReferenceData: []manifest.ReferenceData{{Production: proof, DeclaredKeys: []string{"INDEXED"}}},
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
	input := NewStoreProvider(&reads, true).Lookup(target)
	{
		assert.FatalIf(t, input.Data != nil || !input.Generated, "%s", "unsettled producer supplied a placeholder")
	}
	assert.Diff(t, input.Materializers, []manifest.NamedResource{owner.Named()})
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []store.Child{{Obj: declaration}}, true
	st.RecordEmitted(owner.Named(), &output, nil)
	{
		assert.FatalIf(t, reads.Valid(), "%s", "producer publication did not invalidate pending lookup")
	}
	var fresh store.ReadSet
	fresh.Init(st, reads.ID(), nil)
	defer fresh.Close()
	data := NewStoreProvider(&fresh, true).Data(target)
	{
		assert.FatalIf(t, data == nil, "%s", "settled declaration supplied no placeholder")
	}
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"CURRENT"})
	{
		assert.FatalIf(t, st.GetObject(target) != nil, "%s", "placeholder entered inventory")
	}
}

func TestStoreProvider_CompetingKeysRemainSeparate(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	first := manifest.Production{
		Declaration: manifest.NamedResource{
			Group:     "external-secrets.io",
			Kind:      "ExternalSecret",
			Namespace: "apps",
			Name:      "z-first",
		},
	}
	second := first
	second.Declaration.Name = "a-second"
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{first, second}, ReferenceData: []manifest.ReferenceData{
			{
				Production:   first,
				DeclaredKeys: []string{"FIRST"},
			},
			{Production: second, DeclaredKeys: []string{"SECOND"}},
		}},
	}})
	data := NewStoreProvider(st, true).Data(target)
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"FIRST"})
	{
		assert.FatalIf(t, st.GetObject(target) != nil, "%s", "competing keys produced an object")
	}
}

func testStoreProviderIndexOnlyDeclaredKeysCase(
	t *testing.T,
	testCaseInput *struct {
		name string
		keys []string
		wipe bool
	},
) {
	t.Helper()

	st := store.New()
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	declaration := manifest.NamedResource{
		Group:     "external-secrets.io",
		Kind:      "ExternalSecret",
		Namespace: "apps",
		Name:      "external",
	}
	proof := manifest.Production{Declaration: declaration, Source: manifest.BootstrapSourceID}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions:   []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{{Production: proof, DeclaredKeys: testCaseInput.keys}},
		},
	}})
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
	defer reads.Close()
	input := NewStoreProvider(&reads, testCaseInput.wipe).Lookup(target)
	if testCaseInput.wipe && len(testCaseInput.keys) != 0 {
		{
			assert.FatalIf(t, input.Data == nil, "%s", "index-only key evidence was lost")
		}
		value, _, err := input.Data.Value("VALUE")
		assert.NoError(t, err)
		assert.Equal(t, value, manifest.ValuePlaceholder("VALUE"))
	} else {
		assert.FatalIf(t, input.Data != nil, "%s", "lookup fabricated unavailable key data")
	}
	{
		assert.FatalIf(
			t,
			st.GetObject(target) != nil || st.GetObject(declaration) != nil,
			"%s",
			"lookup published an object",
		)
	}
	st.AddObject(&manifest.RawObject{
		Kind:       declaration.Kind,
		APIVersion: "external-secrets.io/v1",
		Name:       declaration.Name,
		Namespace:  declaration.Namespace,
		Spec: map[string]any{
			"target": map[string]any{"name": target.Name},
			"data":   []any{map[string]any{"secretKey": "CURRENT"}},
		},
	})
	{
		assert.FatalIf(t, reads.Valid(), "%s", "declaration arrival did not invalidate indexed keys")
	}
	data := NewStoreProvider(st, true).Data(target)
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"CURRENT"})
	actual := manifest.NewSecret(target, nil, map[string]any{"REAL": "real"})
	st.AddObject(actual)
	{
		assert.FatalIf(
			t,
			NewStoreProvider(st, true).Data(target) != actual,
			"%s",
			"real data did not supersede indexed keys",
		)
	}
}
