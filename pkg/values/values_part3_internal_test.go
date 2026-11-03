package values

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestStoreProvider_SuppliedProductionReplacementAndWithdrawal(t *testing.T) {
	t.Parallel()
	s := store.New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	s.AddObject(owner)
	s.AddObject(consumer)
	supplied := &manifest.Secret{
		Name:       "credentials",
		Namespace:  "apps",
		StringData: map[string]any{"VALUE": "supplied"},
	}
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: supplied, Path: "credentials.yaml"}})
	assert.NoError(t, err)
	s.SetSupplied(snapshot)
	var first store.ReadSet
	first.Init(s, consumer.Named(), nil)
	defer first.Close()
	input := NewStoreProvider(&first, true).Lookup(supplied.Named())
	{
		assert.FatalIf(
			t,
			input.Data == nil || input.Private || len(input.Materializers) != 0 || s.GetObject(supplied.Named()) != nil,
			"supplied-only input = %+v",
			input,
		)
	}
	value, _, err := input.Data.Value("VALUE")
	assert.NoError(t, err)
	assert.Equal(t, value, "supplied")
	repository := &manifest.Secret{
		Name:       supplied.Name,
		Namespace:  supplied.Namespace,
		StringData: map[string]any{"VALUE": "repository"},
	}
	published := make(chan struct{})
	go func() {
		s.RecordEmitted(owner.Named(), &store.Emission{Children: []store.Child{{Obj: repository}}, Complete: true}, nil)
		close(published)
	}()
	<-published
	{
		assert.FatalIf(t, first.Valid(), "%s", "repository arrival did not invalidate supplied-only capture")
	}
	var second store.ReadSet
	second.Init(s, consumer.Named(), nil)
	second.GetObject(consumer.Named())
	defer second.Close()
	resolved := NewStoreProvider(&second, true).Data(supplied.Named())
	{
		assert.FatalIf(t, resolved != repository, "%s", "accepted repository data did not supersede supply")
	}
	second.Accept()
	assert.Diff(
		t,
		s.AdvisoryEvidence(),
		store.AdvisoryEvidence{
			Overrides: []store.SuppliedOverride{
				{Resource: supplied.Named(), SupplyPath: "credentials.yaml", Keys: []string{"VALUE"}},
			},
		},
	)
	assert.Equal(t, len(s.Warnings()), 0)
	s.RecordEmitted(owner.Named(), &store.Emission{Complete: true}, nil)
	{
		assert.FatalIf(t, second.Valid(), "%s", "withdrawal did not invalidate repository capture")
	}
	var third store.ReadSet
	third.Init(s, consumer.Named(), nil)
	defer third.Close()
	restored := NewStoreProvider(&third, true).Data(supplied.Named())
	value, _, err = restored.Value("VALUE")
	assert.NoError(t, err)
	assert.Equal(t, value, "supplied")
	{
		assert.FatalIf(
			t,
			s.GetObject(supplied.Named()) != nil || len(s.Warnings()) != 0 || len(s.AdvisoryEvidence().Overrides) != 0,
			"%s",
			"withdrawal retained canonical data or stale precedence",
		)
	}
}

func TestReferenceFacts_DataOnlyProviderPolicy(t *testing.T) {
	t.Parallel()
	st := store.New()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	raw := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"VALUE": "visible"}}
	owner := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Name:      "owner",
		Namespace: "apps",
	}
	st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	proof := manifest.Production{
		Owner:    owner,
		Source:   manifest.BootstrapSourceID,
		Authored: id,
		Path:     "input.yaml",
		File:     "input.yaml",
	}
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{proof}},
			},
		},
	)
	st.AddObject(raw)
	facts := st.ReferenceFacts(id, nil)
	{
		assert.FatalIf(t, facts.Data != raw || facts.Accepted, "covered facts=%+v", facts)
	}
	{
		assert.FatalIf(
			t,
			NewStoreProvider(st, true).Data(id) != raw,
			"%s",
			"data-only provider rejected visible prediction",
		)
	}
	var previous store.ReadSet
	previous.Init(st, owner, nil)
	defer previous.Retire()
	{
		assert.FatalIf(
			t,
			NewStoreProvider(&previous, true).Data(id) != raw,
			"%s",
			"captured provider rejected visible prediction",
		)
	}
	first, second := facts.Productions, facts.Productions
	assert.Diff(t, second, first)
	assert.Diff(t, st.Productions(id, nil), first)
	visits := 0
	for range facts.Productions {
		visits++
		break
	}
	assert.Equal(t, visits, 1)

	external := &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "external",
		Namespace:  id.Namespace,
		Spec: map[string]any{
			"target": map[string]any{"name": id.Name},
			"data":   []any{map[string]any{"secretKey": "VALUE"}},
		},
	}
	_, _, next := st.GetEmission(owner)
	defer next.Finish()
	next.Children, next.Complete = []store.Child{{Obj: external}}, true
	st.RecordEmitted(owner, &next, nil)
	{
		assert.FatalIf(t, previous.Valid(), "%s", "replacement retained captured file authority")
	}
	var current store.ReadSet
	current.Init(st, owner, nil)
	defer current.Retire()
	want := []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID, Declaration: external.Named()}}
	for _, reader := range []dataReader{st, &current} {
		assert.Diff(
			t,
			reader.ReferenceFacts(id, nil),
			store.ReferenceFacts{
				Productions:   want,
				ReferenceData: []manifest.ReferenceData{{Production: want[0], DeclaredKeys: []string{"VALUE"}}},
			},
		)
		assert.Diff(t, NewStoreProvider(reader, true).Data(id), &manifest.Secret{
			Name:       id.Name,
			Namespace:  id.Namespace,
			StringData: map[string]any{"VALUE": manifest.ValuePlaceholder("VALUE")},
		})
		{
			assert.FatalIf(
				t,
				NewStoreProvider(reader, false).Data(id) != nil,
				"%s",
				"provider supplied declared placeholders with wiping disabled",
			)
		}
	}
}

func TestStoreProvider_SharedSuppliedValues(t *testing.T) {
	t.Parallel()
	s := store.New()
	secret := &manifest.Secret{
		Name:       "settings",
		Namespace:  "apps",
		StringData: map[string]any{"VALUE": "supplied", "VALUES": "greeting: supplied\n", "tag": "supplied-tag"},
	}
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: secret}})
	assert.NoError(t, err)
	s.SetSupplied(snapshot)
	s.AddObject(
		&manifest.Secret{
			Name:       secret.Name,
			Namespace:  secret.Namespace,
			StringData: map[string]any{"VALUE": "repository", "tag": ""},
		},
	)
	provider := NewStoreProvider(s, true)
	ks := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "absent", Optional: true},
				{Kind: manifest.KindSecret, Name: secret.Name},
			},
			Substitute: map[string]string{"VALUE": "inline"},
		},
	}
	assert.NoError(t, ExpandPostBuildSubstituteReference(ks, provider))
	assert.Equal(t, ks.PostBuild.Substitute["VALUE"], "inline")
	hr := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		ValuesFrom: []manifest.ValuesReference{
			{Kind: manifest.KindConfigMap, Name: "absent", Optional: true},
			{Kind: manifest.KindSecret, Name: secret.Name, ValuesKey: "VALUES"},
			{Kind: manifest.KindSecret, Name: secret.Name, ValuesKey: "tag", TargetPath: "image.tag"},
		},
		Values: map[string]any{"greeting": "inline"},
	}
	omitted, err := ExpandValueReferences(hr, provider, nil)
	assert.NoError(t, err)
	{
		assert.FatalIf(t, len(omitted) != 0, "usable supply omitted: %v", omitted)
	}
	assert.Diff(t, hr.Values, map[string]any{"greeting": "inline", "image": map[string]any{"tag": ""}})
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestExpandPostBuildSubstituteReference_DiagnosticLogContext(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	owner := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      "FluxInstance",
		Namespace: "flux-system",
		Name:      "sync",
	}
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", owner).WithNamespace("sources").(*manifest.Kustomization)
	entry.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{
			{Kind: "Unsupported", Name: "ignored"},
			{Kind: manifest.KindConfigMap, Name: "missing"},
		},
	}
	{
		err := ExpandPostBuildSubstituteReference(entry, NewStoreProvider(store.New(), true))
		assert.NoError(t, err)
	}
	count := 0
	for line := range bytes.SplitSeq(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var record struct {
			ID string `json:"id"`
		}
		{
			err := json.Unmarshal(line, &record)
			assert.NoError(t, err)
		}
		{
			assert.FatalIf(t, record.ID != manifest.DiagnosticFor(entry.Named(), entry).String(), "substitution debug identity=%q", record.ID)
		}
		count++
	}
	{
		assert.FatalIf(t, count != 2, "debug records=%d", count)
	}
}
