package values

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type selectedEvidenceCase struct {
	name, key, value  string
	input             store.ReferenceFacts
	demand            Demand
	allowed, optional bool
	effect            Effect
	present, certain  bool
	unreadable        []string
	cause             string
	malformed         bool
}

func TestResolve_SelectedEvidence(t *testing.T) {
	t.Parallel()
	var zero Result
	assert.Equal(t, zero.Effect() == Used, false)
	assert.Equal(t, zero.Admitted(), false)
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testResolveSelectedEvidenceKind(t, kind)
		})
	}
}

func testResolveSelectedEvidenceKind(t *testing.T, kind string) {
	t.Helper()
	id := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "input"}
	data := func(m map[string]any) manifest.Data {
		if kind == manifest.KindSecret {
			return manifest.NewSecret(id, nil, m)
		}
		return manifest.NewConfigMap(id, m)
	}
	const cipher = "ENC[AES256_GCM,data:opaque,iv:x,tag:y,type:str]"
	parsed, err := manifest.ParseDoc(
		map[string]any{
			"apiVersion": "v1",
			"kind":       kind,
			"metadata":   map[string]any{"name": id.Name, "namespace": id.Namespace},
			"data":       cipher,
		},
		manifest.ParseDocOptions{},
	)
	assert.NoError(t, err)
	opaque := parsed.Object.(manifest.Data)
	readable := data(map[string]any{"plain": "repository", "empty": ""})
	empty := data(nil)
	mixed := data(
		map[string]any{
			"plain":     "repository",
			"encrypted": "ENC[AES256_GCM,data:opaque,iv:x,tag:y,type:str]",
			"zeta":      "ENC[AES256_GCM,data:zeta,iv:x,tag:y,type:str]",
		},
	)
	malformed := data(map[string]any{"broken": []any{"invalid"}})
	actual := func(d manifest.Data) store.ReferenceFacts {
		return store.ReferenceFacts{Declared: true, Data: d, Original: d}
	}
	produced := func(keys manifest.KeySet) store.ReferenceFacts {
		return store.ReferenceFacts{Generated: true, Keys: keys}
	}
	cases := []selectedEvidenceCase{
		{name: "absent whole object", demand: ObjectDemand, effect: Failed, cause: "not found"},
		{name: "optional absent", demand: ObjectDemand, optional: true, effect: Omitted},
		{name: "permitted absent", demand: ObjectDemand, allowed: true, effect: Omitted},
		{name: "absent substitution", demand: SubstitutionDemand, effect: Failed, cause: "not found"},
		{name: "permitted absent substitution", demand: SubstitutionDemand, allowed: true, effect: Omitted},
		{name: "optional absent substitution", demand: SubstitutionDemand, optional: true, effect: Omitted},
		{
			name:    "empty whole object",
			input:   actual(empty),
			demand:  SubstitutionDemand,
			effect:  Used,
			present: true,
			certain: true,
		},
		{
			name:    "readable substitution",
			input:   actual(readable),
			demand:  SubstitutionDemand,
			effect:  Used,
			present: true,
			certain: true,
		},
		{
			name:       "mixed substitution",
			input:      actual(mixed),
			demand:     SubstitutionDemand,
			effect:     Used,
			present:    true,
			unreadable: []string{"encrypted", "zeta"},
		},
		{
			name:       "permitted mixed substitution",
			input:      actual(mixed),
			demand:     SubstitutionDemand,
			allowed:    true,
			effect:     Used,
			present:    true,
			unreadable: []string{"encrypted", "zeta"},
		},
		{
			name:    "unknown producer substitution",
			input:   produced(manifest.KeySet{}),
			demand:  SubstitutionDemand,
			effect:  Used,
			present: true,
		},
		{
			name:       "known producer substitution",
			input:      produced(manifest.KeySet{Names: []string{"encrypted", "zeta"}, Complete: true}),
			demand:     SubstitutionDemand,
			effect:     Used,
			present:    true,
			unreadable: []string{"encrypted", "zeta"},
		},
		{
			name:    "readable",
			input:   actual(readable),
			key:     "plain",
			value:   "repository",
			effect:  Used,
			present: true,
			certain: true,
		},
		{name: "readable empty", input: actual(readable), key: "empty", effect: Used, present: true, certain: true},
		{
			name:       "encrypted",
			input:      actual(mixed),
			key:        "encrypted",
			effect:     Omitted,
			present:    true,
			unreadable: []string{"encrypted"},
		},
		{
			name:       "optional encrypted",
			input:      actual(mixed),
			key:        "encrypted",
			optional:   true,
			effect:     Omitted,
			present:    true,
			unreadable: []string{"encrypted"},
		},
		{
			name:    "missing key",
			input:   actual(readable),
			key:     "missing",
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{
			name:    "permitted missing key",
			input:   actual(readable),
			key:     "missing",
			allowed: true,
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{
			name:     "optional missing key",
			input:    actual(readable),
			key:      "missing",
			optional: true,
			effect:   Omitted,
			present:  true,
		},
		{
			name:    "empty object missing key",
			input:   actual(empty),
			key:     "missing",
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{name: "opaque encrypted keys", input: actual(opaque), key: "plain", effect: Failed, present: true},
		{
			name:    "permitted opaque encrypted keys",
			input:   actual(opaque),
			key:     "plain",
			allowed: true,
			effect:  Omitted,
			present: true,
		},
		{
			name:     "optional opaque encrypted keys",
			input:    actual(opaque),
			key:      "plain",
			optional: true,
			effect:   Omitted,
			present:  true,
		},
		{
			name:    "opaque producer output stays strict",
			input:   store.ReferenceFacts{Declared: true, Data: opaque, Original: opaque, Generated: true},
			key:     "plain",
			effect:  Failed,
			present: true,
		},
		{name: "opaque substitution", input: actual(opaque), demand: SubstitutionDemand, effect: Used, present: true},
		{
			name:       "known produced key",
			input:      produced(manifest.KeySet{Names: []string{"plain"}, Complete: true}),
			key:        "plain",
			effect:     Omitted,
			present:    true,
			unreadable: []string{"plain"},
		},
		{
			name:    "complete produced empty",
			input:   produced(manifest.KeySet{Complete: true}),
			key:     "missing",
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{
			name:    "permitted complete produced empty",
			input:   produced(manifest.KeySet{Complete: true}),
			key:     "missing",
			allowed: true,
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{
			name:    "complete nonempty producer excludes key",
			input:   produced(manifest.KeySet{Names: []string{"plain"}, Complete: true}),
			key:     "missing",
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{
			name:    "permitted complete nonempty producer excludes key",
			input:   produced(manifest.KeySet{Names: []string{"plain"}, Complete: true}),
			key:     "missing",
			allowed: true,
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{
			name:    "unknown produced key",
			input:   produced(manifest.KeySet{}),
			key:     "missing",
			effect:  Omitted,
			present: true,
		},
		{
			name:    "incomplete nonempty producer",
			input:   produced(manifest.KeySet{Names: []string{"plain"}}),
			key:     "missing",
			effect:  Omitted,
			present: true,
		},
		{
			name:    "readable original excludes producer key",
			input:   store.ReferenceFacts{Declared: true, Generated: true, Data: readable, Original: readable},
			key:     "missing",
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{
			name:    "resolved supply",
			input:   store.ReferenceFacts{Generated: true, Data: readable},
			key:     "plain",
			value:   "repository",
			effect:  Used,
			present: true,
			certain: true,
		},
		{
			name:    "partial supplied unknown substitution",
			input:   store.ReferenceFacts{Generated: true, Data: readable},
			demand:  SubstitutionDemand,
			effect:  Used,
			present: true,
		},
		{
			name:    "partial supplied production",
			input:   store.ReferenceFacts{Generated: true, Data: readable},
			key:     "missing",
			effect:  Omitted,
			present: true,
		},
		{
			name:      "malformed consumed",
			input:     actual(malformed),
			key:       "broken",
			effect:    Failed,
			present:   true,
			malformed: true,
		},
		{
			name:      "optional malformed consumed",
			input:     actual(malformed),
			key:       "broken",
			optional:  true,
			effect:    Failed,
			present:   true,
			malformed: true,
		},
		{
			name:    "malformed sibling lazy",
			input:   actual(malformed),
			key:     "missing",
			effect:  Failed,
			present: true,
			cause:   `key "missing" not found`,
		},
		{
			name:      "malformed substitution",
			input:     actual(malformed),
			demand:    SubstitutionDemand,
			effect:    Failed,
			present:   true,
			malformed: true,
		},
		{
			name:    "readable source",
			input:   actual(readable),
			demand:  SourceDemand,
			key:     "plain",
			value:   "repository",
			effect:  Used,
			present: true,
			certain: true,
		},
		{
			name:       "encrypted source",
			input:      actual(mixed),
			demand:     SourceDemand,
			key:        "encrypted",
			effect:     Failed,
			present:    true,
			unreadable: []string{"encrypted"},
		},
		{
			name:    "produced source",
			input:   produced(manifest.KeySet{}),
			demand:  SourceDemand,
			key:     "plain",
			effect:  Omitted,
			present: true,
		},
		{name: "permitted source", demand: SourceDemand, allowed: true, key: "plain", effect: Omitted},
	}
	for i := range cases {
		tc := &cases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ref := manifest.Reference{
				NamedResource: id,
				Use:           "valuesFrom",
				Position:      3,
				Key:           tc.key,
				Optional:      tc.optional,
			}
			permission := testPermissions(nil)
			if tc.allowed {
				permission = testPermissions{kind}
			}
			demand := tc.demand
			if demand == 0 {
				demand = HelmDemand
			}
			result := Resolve(
				&ResolveRequest{Reference: ref, Facts: tc.input, Permission: permission, Demand: demand},
			)
			tc.assertResult(t, id, &result)
		})
	}
}

func (tc *selectedEvidenceCase) assertResult(t *testing.T, id manifest.NamedResource, result *Result) {
	t.Helper()
	assert.Equal(t, result.Effect(), tc.effect)
	assert.Equal(t, result.Value(), tc.value)
	assert.Equal(t, result.Present(), tc.present)
	assert.Equal(t, result.ID(), id)
	assert.Equal(t, result.Position(), 3)
	assert.Equal(t, result.Admitted(), tc.present || tc.optional || tc.allowed)
	assert.Equal(t, result.StaticCertain(), tc.certain)
	assert.Diff(t, result.UnreadableKeys(), tc.unreadable)
	if tc.cause != "" {
		assert.Equal(t, result.Cause(), tc.cause)
	}
	assert.Equal(t, result.MaterialError() != nil, tc.malformed)
	if tc.effect == Failed {
		assert.Equal(t, errors.Is(result.Err(), manifest.ErrFlux), true)
		if !tc.present {
			assert.Equal(t, strings.Contains(result.Err().Error(), "--supply"), true)
		}
	} else {
		assert.NoError(t, result.Err())
	}
	if tc.effect == Omitted || tc.effect == Used {
		assert.Equal(t, len(result.warnings), 0)
	}
}

func TestResolve_PrivateEncryptedAuthority(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	for _, tc := range []struct {
		name               string
		accepted, prepared bool
		effect             Effect
		present            bool
	}{
		{name: "private prepared unreadable", prepared: true, effect: Failed},
		{name: "stored unreadable", effect: Used, present: true},
		{name: "accepted unreadable", prepared: true, accepted: true, effect: Used, present: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := manifest.NewSecret(
				id,
				nil,
				map[string]any{"encrypted": "ENC[AES256_GCM,data:opaque,iv:x,tag:y,type:str]", "plain": "value"},
			)
			input := store.ReferenceFacts{
				Data:     data,
				Original: data,
				Private:  true,
				Offline:  true,
				Prepared: tc.prepared,
				Accepted: tc.accepted,
			}
			result := Resolve(
				&ResolveRequest{Reference: manifest.Reference{NamedResource: id, Use: substitutionUse}, Facts: input},
			)
			assert.Equal(t, result.Effect(), tc.effect)
			assert.Equal(t, result.Present(), tc.present)
			if tc.present {
				assert.Diff(t, result.UnreadableKeys(), []string{"encrypted"})
			} else {
				assert.Equal(t, result.Data() == nil, true)
			}
			st := store.New()
			owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			st.AddObject(owner)
			var reads store.ReadSet
			reads.Init(st, owner.Named(), nil)
			defer reads.Retire()
			result.ReportWarnings(&reads)
			result.ReportWarnings(&reads)
			assert.Equal(t, len(st.Warnings()), 0)
			reads.Accept(store.AssessmentReconciled)
			assert.Equal(t, len(st.Warnings()), 0)
		})
	}
}

func TestResolve_CapturedFailuresAndSettlement(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	producer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "producer"}
	failure := manifest.NewError(manifest.ErrInput, errors.New("captured failure"), "")
	for _, tc := range []struct {
		name                                             string
		facts                                            store.ReferenceFacts
		demand                                           Demand
		optional, allowed, ambient, hard, cycle, pending bool
		effect                                           Effect
		captured                                         bool
	}{
		{name: "hard failure ignores permission", hard: true, allowed: true, effect: Failed, captured: true},
		{name: "object hard failure", demand: ObjectDemand, hard: true, effect: Failed, captured: true},
		{name: "object terminal failure", demand: ObjectDemand, facts: store.ReferenceFacts{Generated: true}, effect: Failed, captured: true},
		{name: "pending materializer", facts: store.ReferenceFacts{Generated: true}, pending: true, effect: Deferred},
		{name: "terminal failure", facts: store.ReferenceFacts{Generated: true}, effect: Failed, captured: true},
		{name: "optional terminal failure", optional: true, effect: Omitted},
		{name: "permitted terminal failure", allowed: true, effect: Omitted},
		{name: "ambient attempt", ambient: true, effect: Deferred},
		{name: "readable bypass", facts: store.ReferenceFacts{Data: manifest.NewSecret(id, nil, map[string]any{"plain": "value"})}, effect: Used},
		{name: "cycle retains failure", facts: store.ReferenceFacts{Data: manifest.NewSecret(id, nil, map[string]any{"plain": "value"})}, cycle: true, effect: Failed, captured: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := ResolveRequest{
				Reference: manifest.Reference{NamedResource: id, Use: "secretRef", Key: "plain", Optional: tc.optional},
				Facts:     tc.facts,
				Demand:    tc.demand,
				Ambient:   tc.ambient,
			}
			if tc.allowed {
				request.Permission = testPermissions{id.Kind}
			}
			switch {
			case tc.hard:
				request.Failure = failure
			case tc.pending:
				request.Settlement.Pending = []manifest.NamedResource{producer}
			case !tc.ambient:
				request.Settlement.Failure = failure
				request.Settlement.BlocksConsumer = tc.cycle
			}
			result := Resolve(&request)
			assert.Equal(t, result.Effect(), tc.effect)
			if tc.captured {
				assert.Equal(t, result.Err(), error(failure))
			}
			if tc.pending {
				assert.Diff(t, result.Pending(), []manifest.NamedResource{producer})
			}
		})
	}
	t.Run("object derived absence", func(t *testing.T) {
		t.Parallel()
		request := ResolveRequest{
			Reference: manifest.Reference{NamedResource: id, Use: substitutionUse},
			Demand:    ObjectDemand,
		}
		result := Resolve(&request)
		copied := result
		request.Reference.Name = "replacement"
		request.Failure = errors.New("replacement failure")
		assert.Equal(t, result.Effect(), Failed)
		assert.Equal(t, result.MaterialError(), error(nil))
		first, repeated, copyError := result.Err(), result.Err(), copied.Err()
		const want = "substituteFrom Secret apps/input not found; supply it with --supply"
		assert.Equal(t, first.Error(), want)
		assert.Equal(t, repeated.Error(), want)
		assert.Equal(t, copyError.Error(), want)
		for _, err := range []error{first, repeated, copyError} {
			assert.Equal(t, errors.Is(err, manifest.ErrObjectNotFound), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		}
	})
}

func TestResolve_UnreadableKeysUseCapturedData(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	data := &capturedKeyData{
		Data: manifest.NewSecret(
			id,
			nil,
			map[string]any{
				"alpha": "ENC[AES256_GCM,data:alpha,iv:x,tag:y,type:str]",
				"zeta":  "ENC[AES256_GCM,data:zeta,iv:x,tag:y,type:str]",
			},
		),
	}
	request := ResolveRequest{
		Reference: manifest.Reference{NamedResource: id, Use: substitutionUse},
		Facts:     store.ReferenceFacts{Data: data, Original: data},
	}
	result := Resolve(&request)
	assert.NoError(t, result.Err())
	data.err = errors.New("data cannot be read after assessment")
	assert.Diff(t, result.UnreadableKeys(), []string{"alpha", "zeta"})
	assert.Equal(t, result.variables["alpha"], "")
	next := Resolve(&request)
	assert.Equal(t, errors.Is(next.Err(), data.err), true)
}

type capturedKeyData struct {
	manifest.Data

	err error
}

func (d *capturedKeyData) Value(key string) (manifest.DataValue, error) {
	if d.err != nil {
		return manifest.DataValue{}, d.err
	}
	return d.Data.Value(key)
}

func TestResolve_SuppliedMaterialAndStaticCertainty(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	data := manifest.NewConfigMap(id, map[string]any{"VALUE": "stable"})
	result := Resolve(
		&ResolveRequest{
			Reference: manifest.Reference{NamedResource: id, Use: substitutionUse},
			Facts: store.ReferenceFacts{
				Data:      data,
				Generated: true,
				Keys:      manifest.KeySet{Names: []string{"VALUE"}},
			},
		},
	)
	assert.Equal(t, result.Effect(), Used)
	assert.Equal(t, result.Materialized(), true)
	assert.Equal(t, result.StaticCertain(), false)
	assert.Equal(t, result.CanPrepare(), false)
	assert.Diff(t, result.variables, map[string]string{"VALUE": "stable"})
}

func TestResolve_SourceAcquisition(t *testing.T) {
	t.Parallel()
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "auth"}
	unavailable := manifest.NewError(manifest.ErrMissingSecret, nil, "secret apps/auth unavailable")
	pending := []manifest.NamedResource{{Kind: manifest.KindKustomization, Namespace: "apps", Name: "producer"}}
	for _, tc := range []struct {
		name                                 string
		facts                                store.ReferenceFacts
		permission                           testPermissions
		ambient, provider, denied, anonymous bool
		pending                              []manifest.NamedResource
		failure                              error
		effect                               Effect
	}{
		{name: "strict", effect: Failed},
		{name: "matching permission", permission: testPermissions{manifest.KindSecret}, effect: Omitted},
		{name: "other permission", permission: testPermissions{manifest.KindConfigMap}, effect: Failed},
		{name: "unknown generated", facts: store.ReferenceFacts{Generated: true}, ambient: true, effect: Omitted},
		{name: "private preparation", facts: store.ReferenceFacts{Generated: true, Private: true, Prepared: true}, ambient: true, effect: Deferred},
		{name: "declared encrypted", facts: store.ReferenceFacts{Declared: true}, effect: Failed},
		{name: "ambient before permission", ambient: true, permission: testPermissions{manifest.KindSecret}, effect: Deferred},
		{name: "wait before ambient", ambient: true, pending: pending, effect: Deferred},
		{name: "provider denial", provider: true, denied: true, anonymous: true, effect: Omitted},
		{name: "source denial", denied: true, anonymous: true, effect: Failed},
		{name: "authored denial", provider: true, denied: true, effect: Failed},
		{name: "unproven provider denial", provider: true, anonymous: true, effect: Failed},
		{name: "hard consumed failure", facts: store.ReferenceFacts{Generated: true}, permission: testPermissions{manifest.KindSecret}, failure: manifest.ErrInput, effect: Failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := secret
			if tc.anonymous {
				id.Name = ""
			}
			result := Resolve(&ResolveRequest{
				Reference: manifest.Reference{NamedResource: id, Key: "token", Position: 2, Use: "secretRef"},
				Facts:     tc.facts, Permission: tc.permission, Demand: SourceDemand,
				Unavailable: unavailable, Ambient: tc.ambient, ProviderOnly: tc.provider, AnonymousDenied: tc.denied,
				Settlement: Settlement{Pending: tc.pending}, Failure: tc.failure,
			})
			assert.Equal(t, result.Effect(), tc.effect)
			assert.Equal(t, result.ID(), id)
			assert.Equal(t, result.Position(), 2)
			assert.Equal(t, result.Value(), "")
			assert.Diff(t, result.Pending(), tc.pending)
			if tc.effect == Failed {
				cause := error(unavailable)
				if tc.failure != nil {
					cause = tc.failure
				}
				assert.Equal(t, result.Err(), cause)
			} else {
				assert.NoError(t, result.Err())
			}
		})
	}
}
