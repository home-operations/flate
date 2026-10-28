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
			st := store.New()
			target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
			declaration := manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "apps", Name: "external"}
			proof := manifest.Production{Declaration: declaration, Source: manifest.BootstrapSourceID}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				target: {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{{Production: proof, DeclaredKeys: tc.keys}}},
			}})
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}, nil)
			defer reads.Close()
			input := NewStoreProvider(&reads, tc.wipe).Lookup(target)
			if tc.wipe && len(tc.keys) != 0 {
				if input.Data == nil {
					t.Fatal("index-only key evidence was lost")
				}
				value, _, err := input.Data.Value("VALUE")
				assert.NoError(t, err)
				assert.Equal(t, value, manifest.ValuePlaceholder("VALUE"))
			} else if input.Data != nil {
				t.Fatal("lookup fabricated unavailable key data")
			}
			if st.GetObject(target) != nil || st.GetObject(declaration) != nil {
				t.Fatal("lookup published an object")
			}
			st.AddObject(&manifest.RawObject{Kind: declaration.Kind, APIVersion: "external-secrets.io/v1", Name: declaration.Name, Namespace: declaration.Namespace,
				Spec: map[string]any{"target": map[string]any{"name": target.Name}, "data": []any{map[string]any{"secretKey": "CURRENT"}}}})
			if reads.Valid() {
				t.Fatal("declaration arrival did not invalidate indexed keys")
			}
			data := NewStoreProvider(st, true).Data(target)
			assert.Diff(t, slices.Sorted(data.Keys()), []string{"CURRENT"})
			actual := manifest.NewSecret(target, nil, map[string]any{"REAL": "real"})
			st.AddObject(actual)
			if NewStoreProvider(st, true).Data(target) != actual {
				t.Fatal("real data did not supersede indexed keys")
			}
		})
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
		target: {Productions: []manifest.Production{p, q}, ReferenceData: []manifest.ReferenceData{{Production: p, OfflineData: one}, {Production: q, OfflineData: two}}},
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
	if st.GetObject(target) != nil || len(st.ListObjects("", manifest.KindSecret)) != 0 {
		t.Fatal("offline data entered inventory")
	}
}

func TestStoreProvider_OfflineSourceAndRealDataInvalidateCapture(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	proof := manifest.Production{Source: manifest.BootstrapSourceID, Path: "input.yaml", File: "input.yaml", Authored: target}
	offline := manifest.NewConfigMap(target, map[string]any{"VALUE": "offline"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{{Production: proof, OfflineData: offline}}},
	}})
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}, nil)
	defer reads.Close()
	provider := NewStoreProvider(&reads, true)
	if provider.Data(target) != offline {
		t.Fatal("offline candidate unavailable")
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Content: "new-source"})
	if reads.Valid() {
		t.Fatal("source arrival did not invalidate offline data")
	}
	var fresh store.ReadSet
	fresh.Init(st, reads.ID(), nil)
	defer fresh.Close()
	next := NewStoreProvider(&fresh, true)
	if next.Data(target) != offline {
		t.Fatal("fresh lookup lost candidate")
	}
	actual := manifest.NewConfigMap(target, map[string]any{"VALUE": "real"})
	st.AddObject(actual)
	if fresh.Valid() {
		t.Fatal("real data arrival did not invalidate offline capture")
	}
	if provider.Data(target) != offline || next.Data(target) != offline {
		t.Fatal("captured lookup changed during expansion")
	}
	if NewStoreProvider(st, true).Data(target) != actual {
		t.Fatal("fresh lookup retained offline value")
	}
}

func TestStoreProvider_PendingIndexedKeysAwaitPublication(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	st.AddObject(owner)
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	declaration := &manifest.RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "external", Namespace: "apps",
		Spec: map[string]any{"target": map[string]any{"name": target.Name}, "data": []any{map[string]any{"secretKey": "CURRENT"}}}}
	proof := manifest.Production{Owner: owner.Named(), Declaration: declaration.Named(), Source: manifest.BootstrapSourceID}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{{Production: proof, DeclaredKeys: []string{"INDEXED"}}}},
	}})
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "consumer", Namespace: "apps"}, nil)
	defer reads.Close()
	input := NewStoreProvider(&reads, true).Lookup(target)
	if input.Data != nil || !input.Generated {
		t.Fatal("unsettled producer supplied a placeholder")
	}
	assert.Diff(t, input.Materializers, []manifest.NamedResource{owner.Named()})
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []store.Child{{Obj: declaration}}, true
	st.RecordEmitted(owner.Named(), &output, nil)
	if reads.Valid() {
		t.Fatal("producer publication did not invalidate pending lookup")
	}
	var fresh store.ReadSet
	fresh.Init(st, reads.ID(), nil)
	defer fresh.Close()
	data := NewStoreProvider(&fresh, true).Data(target)
	if data == nil {
		t.Fatal("settled declaration supplied no placeholder")
	}
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"CURRENT"})
	if st.GetObject(target) != nil {
		t.Fatal("placeholder entered inventory")
	}
}

func TestStoreProvider_CompetingKeysRemainSeparate(t *testing.T) {
	t.Parallel()
	st := store.New()
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	first := manifest.Production{Declaration: manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "apps", Name: "z-first"}}
	second := first
	second.Declaration.Name = "a-second"
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{first, second}, ReferenceData: []manifest.ReferenceData{
			{Production: first, DeclaredKeys: []string{"FIRST"}}, {Production: second, DeclaredKeys: []string{"SECOND"}},
		}},
	}})
	data := NewStoreProvider(st, true).Data(target)
	assert.Diff(t, slices.Sorted(data.Keys()), []string{"FIRST"})
	if st.GetObject(target) != nil {
		t.Fatal("competing keys produced an object")
	}
}
