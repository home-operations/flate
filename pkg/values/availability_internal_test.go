package values

import (
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestClassify_SelectedEvidence(t *testing.T) {
	t.Parallel()
	zero := Result{}
	assert.Equal(t, zero.Disposition(testPermissions{""}), Reject)
	assert.Equal(t, zero.AdmitMissing(testPermissions{""}), false)
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "input"}
			var readable manifest.Data = manifest.NewConfigMap(id, map[string]any{"plain": "repository", "empty": ""})
			var empty manifest.Data = manifest.NewConfigMap(id, nil)
			var malformed manifest.Data = manifest.NewConfigMap(id, map[string]any{"broken": []any{"invalid"}})
			if kind == manifest.KindSecret {
				readable = manifest.NewSecret(id, nil, map[string]any{"plain": "repository", "empty": ""})
				empty = manifest.NewSecret(id, nil, nil)
				malformed = manifest.NewSecret(id, map[string]any{"broken": "!"}, nil)
			}
			absentSubstitution := Reject
			if kind == manifest.KindSecret {
				absentSubstitution = Unresolved
			}
			warnings := []manifest.Warning{
				{Resource: id, Category: manifest.WarnSOPSUnreadable, Detail: []string{"encrypted"}},
			}
			for _, tc := range []classificationCase{
				{name: "absent whole object", state: Absent, cause: "not found", substitution: absentSubstitution, permittedSubstitution: Unresolved, admission: [3]bool{false, true, false}},
				{name: "encrypted whole object", input: store.ReferenceFacts{Data: empty, Original: empty, Warnings: warnings}, state: Encrypted, cause: `key "encrypted" is SOPS-encrypted and unreadable; supply it with --supply`, substitution: Unresolved, permittedSubstitution: Unresolved, admission: [3]bool{true, true, true}},
				{name: "produced whole object", input: store.ReferenceFacts{Generated: true}, state: Produced, cause: "is produced in the cluster with unknown data; supply it with --supply", substitution: Unresolved, permittedSubstitution: Unresolved, admission: [3]bool{true, true, true}},
				{name: "absent", key: "plain", state: Absent, cause: "not found", substitution: absentSubstitution, permittedSubstitution: Unresolved, admission: [3]bool{false, true, false}},
				{name: "readable", key: "plain", input: store.ReferenceFacts{Data: readable, Original: readable, Warnings: warnings}, state: Readable, value: "repository", substitution: UseData, permittedSubstitution: UseData},
				{name: "empty string wins", key: "empty", input: store.ReferenceFacts{Data: readable, Original: readable, Warnings: []manifest.Warning{{Category: manifest.WarnSOPSUnreadable, Detail: []string{"empty"}}}}, state: Readable, substitution: UseData, permittedSubstitution: UseData},
				{name: "encrypted", key: "encrypted", input: store.ReferenceFacts{Data: readable, Original: readable, Warnings: warnings}, state: Encrypted, cause: `key "encrypted" is SOPS-encrypted and unreadable; supply it with --supply`, substitution: UseData, permittedSubstitution: Unresolved, admission: [3]bool{false, true, false}},
				{name: "missing key", key: "missing", input: store.ReferenceFacts{Data: readable, Original: readable, Generated: true}, state: MissingKey, cause: `key "missing" not found`, substitution: UseData, permittedSubstitution: UseData},
				{name: "empty object missing key", key: "missing", input: store.ReferenceFacts{Data: empty, Original: empty}, state: MissingKey, cause: `key "missing" not found`, substitution: Reject, permittedSubstitution: Reject},
				{name: "empty whole object", input: store.ReferenceFacts{Data: empty, Original: empty}, state: Readable, substitution: UseData, permittedSubstitution: UseData},
				{name: "mixed whole object", input: store.ReferenceFacts{Data: readable, Original: readable, Warnings: warnings}, state: Encrypted, cause: `key "encrypted" is SOPS-encrypted and unreadable; supply it with --supply`, substitution: UseData, permittedSubstitution: Unresolved, admission: [3]bool{false, true, false}},
				{name: "produced", key: "missing", input: store.ReferenceFacts{Generated: true}, state: Produced, cause: "is produced in the cluster with unknown data; supply it with --supply", substitution: Unresolved, permittedSubstitution: Unresolved, admission: [3]bool{true, true, true}},
				{name: "partial supplied offline input", key: "missing", input: store.ReferenceFacts{Data: readable, Offline: true}, state: Absent, cause: "not found", substitution: UseData, permittedSubstitution: Unresolved, admission: [3]bool{false, true, false}},
				{name: "partial mixed offline production", key: "missing", input: store.ReferenceFacts{Data: readable, Generated: true, Offline: true}, state: Produced, cause: "is produced in the cluster with unknown data; supply it with --supply", substitution: UseData, permittedSubstitution: Unresolved, admission: [3]bool{false, true, false}},
				{name: "partial supplied production", key: "missing", input: store.ReferenceFacts{Data: readable, Generated: true}, state: Produced, cause: "is produced in the cluster with unknown data; supply it with --supply", substitution: UseData, permittedSubstitution: Unresolved, admission: [3]bool{false, true, false}},
				{name: "resolved supplied production", key: "plain", input: store.ReferenceFacts{Data: readable, Generated: true}, state: Readable, value: "repository", substitution: UseData, permittedSubstitution: UseData},
				{name: "malformed consumed", key: "broken", input: store.ReferenceFacts{Data: malformed, Original: malformed}, wantError: true},
				{name: "malformed sibling lazy", key: "missing", input: store.ReferenceFacts{Data: malformed, Original: malformed}, state: MissingKey, cause: `key "missing" not found`, substitution: UseData, permittedSubstitution: UseData},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					testClassifySelectedEvidence(t, kind, &tc)
				})
			}
		})
	}
}

type classificationCase struct {
	name, key, value, cause             string
	input                               store.ReferenceFacts
	state                               State
	wantError                           bool
	substitution, permittedSubstitution Disposition
	admission                           [3]bool
}

func testClassifySelectedEvidence(t *testing.T, kind string, tc *classificationCase) {
	t.Helper()
	ref := manifest.Reference{Kind: kind, Namespace: "apps", Name: "input", Key: tc.key, Use: "valuesFrom"}
	result, err := Classify(new(ref), new(tc.input))
	if tc.wantError {
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, result.State(), tc.state)
	assert.Equal(t, result.Value(), tc.value)
	assert.Equal(t, result.Data(), tc.input.Data)
	if tc.state != Readable {
		assert.Equal(t, result.Cause(), tc.cause)
		assert.Equal(t, errors.Is(result.Failure(), manifest.ErrFlux), true)
		if tc.state != MissingKey {
			assert.Equal(t, strings.Contains(result.Failure().Error(), "--supply"), true)
		}
	}
	wrongKind := manifest.KindConfigMap
	if kind == manifest.KindConfigMap {
		wrongKind = manifest.KindSecret
	}
	for _, allowed := range []string{"", kind, wrongKind} {
		want := Reject
		if tc.state == Readable {
			want = UseData
		} else if tc.state == Produced || allowed == kind && tc.state != MissingKey {
			want = Omit
		}
		assert.Equal(t, result.Disposition(testPermissions{allowed}), want)
	}
	ref.Use = substitutionUse
	result, err = Classify(new(ref), new(tc.input))
	assert.NoError(t, err)
	for i, policy := range []struct {
		name, allowed string
		want          Disposition
	}{
		{name: "no permission", want: tc.substitution},
		{name: "matching permission", allowed: kind, want: tc.permittedSubstitution},
		{name: "wrong kind permission", allowed: wrongKind, want: tc.substitution},
	} {
		t.Run(policy.name, func(t *testing.T) {
			t.Parallel()
			permission := testPermissions{policy.allowed}
			assert.Equal(t, result.Disposition(permission), policy.want)
			assert.Equal(t, result.AdmitMissing(permission), tc.admission[i])
			assert.Equal(t, result.State(), tc.state)
			assert.Equal(t, result.Data(), tc.input.Data)
			if tc.state != Readable {
				assert.Equal(t, result.Cause(), tc.cause)
				assert.Equal(t, errors.Is(result.Failure(), manifest.ErrFlux), true)
			}
			optionalRef := ref
			optionalRef.Optional = true
			optional, err := Classify(new(optionalRef), new(tc.input))
			assert.NoError(t, err)
			want := Omit
			if tc.state == Readable {
				want = UseData
			}
			assert.Equal(t, optional.Disposition(permission), want)
			assert.Equal(t, optional.AdmitMissing(permission), tc.state != Readable)
		})
	}
}
