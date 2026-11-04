package values

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
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
	s.AddDeclaration(
		store.Declaration{
			Obj:       supplied,
			Doc:       testutil.ObjectDocument(supplied),
			Origin:    manifest.SuppliedDeclaration,
			InputPath: "credentials.yaml",
		},
	)
	var first store.ReadSet
	first.Init(s, consumer.Named(), nil)
	defer first.Close()
	input := NewStoreProvider(&first).Lookup(supplied.Named())

	assert.FatalIf(
		t,
		input.Data == nil || input.Private || len(input.Materializers) != 0 || !s.ReportHidden(supplied.Named()),
		"supplied-only input = %+v",
		input,
	)

	valueResult, err := input.Data.Value("VALUE")
	value := valueResult.Value
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
	assert.FatalIf(t, first.Valid(), "%s", "repository arrival did not invalidate supplied-only capture")
	var second store.ReadSet
	second.Init(s, consumer.Named(), nil)
	second.GetObject(consumer.Named())
	defer second.Close()
	resolved := NewStoreProvider(&second).Data(supplied.Named())
	assert.FatalIf(t, resolved != repository, "%s", "accepted repository data did not supersede supply")
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
	assert.FatalIf(t, second.Valid(), "%s", "withdrawal did not invalidate repository capture")
	var third store.ReadSet
	third.Init(s, consumer.Named(), nil)
	defer third.Close()
	restored := NewStoreProvider(&third).Data(supplied.Named())
	valueResult2, valueResultErr := restored.Value("VALUE")
	value, err = valueResult2.Value, valueResultErr
	assert.NoError(t, err)
	assert.Equal(t, value, "supplied")

	assert.FatalIf(
		t,
		!s.ReportHidden(supplied.Named()) || len(s.Warnings()) != 0 || len(s.AdvisoryEvidence().Overrides) != 0,
		"%s",
		"withdrawal retained canonical data or stale precedence",
	)
}

func TestStoreProvider_SharedSuppliedValues(t *testing.T) {
	t.Parallel()
	s := store.New()
	secret := &manifest.Secret{
		Name:       "settings",
		Namespace:  "apps",
		StringData: map[string]any{"VALUE": "supplied", "VALUES": "greeting: supplied\n", "tag": "supplied-tag"},
	}
	s.AddDeclaration(
		store.Declaration{Obj: secret, Doc: testutil.ObjectDocument(secret), Origin: manifest.SuppliedDeclaration},
	)
	s.AddObject(
		&manifest.Secret{
			Name:       secret.Name,
			Namespace:  secret.Namespace,
			StringData: map[string]any{"VALUE": "repository", "tag": ""},
		},
	)
	provider := NewStoreProvider(s)
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
	err := ExpandValueReferences(hr, provider, nil)
	assert.NoError(t, err)
	assert.Diff(t, hr.Values, map[string]any{"greeting": "inline", "image": map[string]any{"tag": ""}})
}
