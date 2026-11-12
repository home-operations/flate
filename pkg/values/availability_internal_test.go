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
			warnings := []manifest.Warning{
				{Resource: id, Category: manifest.WarnSOPSUnreadable, Detail: []string{"encrypted"}},
			}
			for _, tc := range []classificationCase{
				{name: "absent", key: "plain", state: Absent, cause: "not found"},
				{name: "readable", key: "plain", input: store.ReferenceFacts{Data: readable, Original: readable, Warnings: warnings}, state: Readable, value: "repository"},
				{name: "empty string wins", key: "empty", input: store.ReferenceFacts{Data: readable, Original: readable, Warnings: []manifest.Warning{{Category: manifest.WarnSOPSUnreadable, Detail: []string{"empty"}}}}, state: Readable},
				{name: "encrypted", key: "encrypted", input: store.ReferenceFacts{Data: readable, Original: readable, Warnings: warnings}, state: Encrypted, cause: `key "encrypted" is SOPS-encrypted and unreadable; supply it with --supply`},
				{name: "missing key", key: "missing", input: store.ReferenceFacts{Data: readable, Original: readable, Generated: true}, state: MissingKey, cause: `key "missing" not found`},
				{name: "empty object missing key", key: "missing", input: store.ReferenceFacts{Data: empty, Original: empty}, state: MissingKey, cause: `key "missing" not found`},
				{name: "empty whole object", input: store.ReferenceFacts{Data: empty, Original: empty}, state: Readable},
				{name: "mixed whole object", input: store.ReferenceFacts{Data: readable, Original: readable, Warnings: warnings}, state: Encrypted, cause: `key "encrypted" is SOPS-encrypted and unreadable; supply it with --supply`},
				{name: "produced", key: "missing", input: store.ReferenceFacts{Generated: true}, state: Produced, cause: "is produced in the cluster with unknown data; supply it with --supply"},
				{name: "partial supplied offline input", key: "missing", input: store.ReferenceFacts{Data: readable, Offline: true}, state: Absent, cause: "not found"},
				{name: "partial mixed offline production", key: "missing", input: store.ReferenceFacts{Data: readable, Generated: true, Offline: true}, state: Produced, cause: "is produced in the cluster with unknown data; supply it with --supply"},
				{name: "partial supplied production", key: "missing", input: store.ReferenceFacts{Data: readable, Generated: true}, state: Produced, cause: "is produced in the cluster with unknown data; supply it with --supply"},
				{name: "resolved supplied production", key: "plain", input: store.ReferenceFacts{Data: readable, Generated: true}, state: Readable, value: "repository"},
				{name: "malformed consumed", key: "broken", input: store.ReferenceFacts{Data: malformed, Original: malformed}, wantError: true},
				{name: "malformed sibling lazy", key: "missing", input: store.ReferenceFacts{Data: malformed, Original: malformed}, state: MissingKey, cause: `key "missing" not found`},
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
	name, key, value, cause string
	input                   store.ReferenceFacts
	state                   State
	wantError               bool
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
	for _, allowed := range []string{"", kind, "other"} {
		want := Reject
		if tc.state == Readable {
			want = UseData
		} else if tc.state == Produced || allowed == kind && tc.state != MissingKey {
			want = Omit
		}
		assert.Equal(t, result.Disposition(testPermissions{allowed}), want)
	}
}
