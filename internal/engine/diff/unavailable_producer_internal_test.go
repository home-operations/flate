package diff

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCompare_UnavailableProducerPolicy(t *testing.T) {
	t.Parallel()
	failedOrig := store.StatusInfo{Status: store.StatusFailed, Message: "orig build failed"}
	failedCurrent := store.StatusInfo{Status: store.StatusFailed, Message: "current build failed"}
	skipped := store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "missing source"}
	suspended := store.StatusInfo{
		Status:  store.StatusReady,
		Reason:  store.ReasonSuspended,
		Message: "operator stopped reconciliation",
	}
	unchanged := store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"}
	st := store.New()
	st.UpdateFailure(
		helmReleaseParent().NamedResource,
		&manifest.DependencyFailedError{
			Parent: helmReleaseParent().NamedResource,
			Failed: []manifest.NamedResource{{Name: "source"}},
		},
	)
	blocked, _ := st.GetStatus(helmReleaseParent().NamedResource)
	selectedErr := errors.New("scoped render failure")
	cases := []struct {
		name          string
		left, right   store.StatusInfo
		leftN, rightN int
		withheld      bool
		side          side
		count         int
		reason        string
		keepError     bool
	}{
		{
			name:      "current failure with stale output",
			right:     failedCurrent,
			leftN:     3,
			rightN:    1,
			withheld:  true,
			side:      sideCurrent,
			count:     3,
			reason:    failedCurrent.String(),
			keepError: true,
		},
		{
			name:      "orig failure",
			left:      failedOrig,
			leftN:     1,
			rightN:    2,
			withheld:  true,
			side:      sideOrig,
			count:     2,
			reason:    failedOrig.String(),
			keepError: true,
		},
		{
			name:     "out-of-scope failure stays nonfatal",
			right:    failedCurrent,
			leftN:    2,
			withheld: true,
			side:     sideCurrent,
			count:    2,
			reason:   failedCurrent.String(),
		},
		{
			name:     "current soft skip",
			right:    skipped,
			leftN:    2,
			rightN:   1,
			withheld: true,
			side:     sideCurrent,
			count:    2,
			reason:   "missing source",
		},
		{
			name:      "both failed",
			left:      failedOrig,
			right:     failedCurrent,
			leftN:     2,
			rightN:    1,
			withheld:  true,
			side:      sideBoth,
			count:     3,
			reason:    "failed on both sides: orig: orig build failed; current: current build failed",
			keepError: true,
		},
		{
			name:      "both failed without documents",
			left:      failedOrig,
			right:     failedCurrent,
			withheld:  true,
			side:      sideBoth,
			reason:    "failed on both sides: orig: orig build failed; current: current build failed",
			keepError: true,
		},
		{
			name:     "current suspended",
			right:    suspended,
			leftN:    2,
			rightN:   1,
			withheld: true,
			side:     sideCurrent,
			count:    2,
			reason:   "suspended on current",
		},
		{
			name:     "orig suspended",
			left:     suspended,
			leftN:    1,
			rightN:   2,
			withheld: true,
			side:     sideOrig,
			count:    2,
			reason:   "suspended on orig",
		},
		{
			name:     "suspension without healthy documents",
			right:    suspended,
			rightN:   1,
			withheld: true,
			side:     sideCurrent,
			reason:   "suspended on current",
		},
		{
			name:     "both suspended",
			left:     suspended,
			right:    suspended,
			leftN:    2,
			rightN:   1,
			withheld: true,
			side:     sideBoth,
			count:    3,
			reason:   "suspended on orig; suspended on current",
		},
		{
			name:     "both suspended without documents",
			left:     suspended,
			right:    suspended,
			withheld: true,
			side:     sideBoth,
			reason:   "suspended on orig; suspended on current",
		},
		{
			name:      "failure and suspension",
			left:      failedOrig,
			right:     suspended,
			leftN:     2,
			rightN:    1,
			withheld:  true,
			side:      sideBoth,
			count:     3,
			reason:    "orig: orig build failed; suspended on current",
			keepError: true,
		},
		{
			name:      "suspension and failure",
			left:      suspended,
			right:     failedCurrent,
			leftN:     1,
			rightN:    2,
			withheld:  true,
			side:      sideBoth,
			count:     3,
			reason:    "suspended on orig; current: current build failed",
			keepError: true,
		},
		{
			name:     "skip and suspension",
			left:     skipped,
			right:    suspended,
			leftN:    1,
			rightN:   2,
			withheld: true,
			side:     sideBoth,
			count:    3,
			reason:   "orig: missing source; suspended on current",
		},
		{name: "both skipped control", left: skipped, right: skipped, leftN: 1, rightN: 1},
		{name: "failure and skip control", left: failedOrig, right: skipped, leftN: 1, rightN: 1, keepError: true},
		{name: "skip and failure control", left: skipped, right: failedCurrent, leftN: 1, rightN: 1, keepError: true},
		{name: "unchanged is available", left: unchanged, right: unchanged, leftN: 1, rightN: 1},
		{
			name:      "blocked is failed",
			right:     blocked,
			left:      suspended,
			leftN:     1,
			rightN:    2,
			withheld:  true,
			side:      sideBoth,
			count:     3,
			reason:    "suspended on orig; current: " + blocked.String(),
			keepError: true,
		},
		{
			name:     "empty typed skip",
			right:    store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped},
			leftN:    1,
			withheld: true,
			side:     sideCurrent,
			count:    1,
		},
		{
			name: "nested skip",
			right: store.StatusInfo{
				Status:  store.StatusReady,
				Reason:  store.ReasonSkipped,
				Message: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped: skipped: missing auth",
			},
			leftN:    1,
			withheld: true,
			side:     sideCurrent,
			count:    1,
			reason:   "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped: skipped: missing auth",
		},
		{
			name: "boundary whitespace",
			right: store.StatusInfo{
				Status:  store.StatusReady,
				Reason:  store.ReasonSkipped,
				Message: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:   missing auth",
			},
			leftN:    1,
			withheld: true,
			side:     sideCurrent,
			count:    1,
			reason:   "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:   missing auth",
		},
		{
			name:   "misleading skip",
			right:  store.StatusInfo{Status: store.StatusReady, Message: "skipped: missing source"},
			leftN:  1,
			rightN: 1,
		},
		{
			name:   "misleading suspension",
			right:  store.StatusInfo{Status: store.StatusReady, Message: "suspended"},
			leftN:  1,
			rightN: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCompareUnavailableProducerPolicyCase(
				t,
				&testCompareUnavailableProducerPolicyCaseInputs{selectedErr: &selectedErr, tc: &tc},
			)
		},
		)
	}
	for _, side := range []side{sideOrig, sideCurrent} {
		t.Run("generated "+string(side), func(t *testing.T) {
			t.Parallel()
			parent, sibling := helmReleaseParent(), kustomizationParent()
			old := withGeneratorHash(t, new(cmDoc(&parent, manifest.NamedResource{Name: "values"}, "before", "")))
			current := withGeneratorHash(t, new(cmDoc(&parent, manifest.NamedResource{Name: "values"}, "after", "")))
			extra := withGeneratorHash(t, new(cmDoc(&parent, manifest.NamedResource{Name: "extra"}, "extra", "")))
			healthyOld := withGeneratorHash(
				t,
				new(cmDoc(&sibling, manifest.NamedResource{Name: "healthy"}, "before", "")),
			)
			healthyNew := withGeneratorHash(
				t,
				new(cmDoc(&sibling, manifest.NamedResource{Name: "healthy"}, "after", "")),
			)
			left := selectedSnapshot(&parent, []Doc{old, extra}, store.StatusInfo{})
			right := selectedSnapshot(&parent, []Doc{current}, failedCurrent)
			if side == sideOrig {
				left = selectedSnapshot(&parent, []Doc{old}, failedOrig)
				right = selectedSnapshot(&parent, []Doc{current, extra}, store.StatusInfo{})
			}
			left.Manifests = append(
				left.Manifests,
				selectedSnapshot(&sibling, []Doc{healthyOld}, store.StatusInfo{}).Manifests...)
			right.Manifests = append(
				right.Manifests,
				selectedSnapshot(&sibling, []Doc{healthyNew}, store.StatusInfo{}).Manifests...)
			comparison, err := Compare(
				[]Group{{Orig: left, Current: right, SelectedErr: selectedErr}},
				Options{Format: FormatJSON},
			)
			assert.Equal(t, errors.Is(err, selectedErr), true)
			assert.Equal(t, len(comparison.Groups[0].Warnings), 0)
			assert.Equal(t, len(comparison.Groups[0].Suppressed), 1)
			note := comparison.Groups[0].Suppressed[0]
			assert.Equal(t, note.Side, side)
			assert.Equal(t, note.Count, 2)
			assert.Equal(t, len(comparison.output[0].changes), 1)
			change := comparison.output[0].changes[0]
			assert.Equal(t, change.Status, ChangeStatus("changed"))
			assert.Equal(t, change.Name, manifest.DocIdentity(healthyNew.Manifest).Name)
			var first, second bytes.Buffer
			assert.NoError(t, comparison.Write(&first, nil))
			assert.NoError(t, comparison.Write(&second, nil))
			assert.Diff(t, second.Bytes(), first.Bytes())
		})
	}
}

type testCompareUnavailableProducerPolicyCaseInputs struct {
	selectedErr *error
	tc          *struct {
		name      string
		left      store.StatusInfo
		right     store.StatusInfo
		leftN     int
		rightN    int
		withheld  bool
		side      side
		count     int
		reason    string
		keepError bool
	}
}

func testCompareUnavailableProducerPolicyCase(
	t *testing.T,
	testCaseInput *testCompareUnavailableProducerPolicyCaseInputs,
) {
	t.Helper()

	left, right := hrDocs(testCaseInput.tc.leftN), hrDocs(testCaseInput.tc.rightN)
	for _, doc := range left {
		doc.Manifest["spec"] = map[string]any{
			"containers": []any{map[string]any{"image": "ghcr.io/demo/orig:v1"}},
		}
	}
	for _, doc := range right {
		doc.Manifest["spec"] = map[string]any{
			"containers": []any{map[string]any{"image": "ghcr.io/demo/current:v2"}},
		}
	}
	selected := []Snapshot{
		selectedSnapshot(new(helmReleaseParent()), left, testCaseInput.tc.left),
		selectedSnapshot(new(helmReleaseParent()), right, testCaseInput.tc.right),
	}
	var runErr error
	if testCaseInput.tc.keepError {
		runErr = (*testCaseInput.selectedErr)
	}
	comparison1, preparationErr1 := Compare(
		[]Group{{Orig: selected[0], Current: selected[1], SelectedErr: runErr}},
		Options{Format: FormatJSON},
	)
	var comparisonOutput1 bytes.Buffer
	writeErr1 := comparison1.Write(&comparisonOutput1, nil)
	out, warnings, suppressed, err := comparisonOutput1.Bytes(), comparison1.Groups[0].Warnings, comparison1.Groups[0].Suppressed, errors.Join(
		preparationErr1,
		writeErr1,
	)
	if errors.Is(err, (*testCaseInput.selectedErr)) != testCaseInput.tc.keepError {
		t.Errorf("selected error = %v; want preserved=%t", err, testCaseInput.tc.keepError)
	}
	var document struct {
		Changes    []Change      `json:"changes"`
		Suppressed []Suppression `json:"suppressed"`
	}
	{
		unmarshalErr2 := json.Unmarshal(out, &document)
		assert.NoError(t, unmarshalErr2)
	}
	if testCaseInput.tc.withheld {
		want := Suppression{
			Parent: helmReleaseParent(),
			Side:   testCaseInput.tc.side,
			Count:  testCaseInput.tc.count,
			Reason: testCaseInput.tc.reason,
		}
		assert.FatalIf(
			t,
			len(document.Changes) != 0 || len(document.Suppressed) != 1 || document.Suppressed[0] != want,
			"withheld output = %+v; want note %+v and no changes",
			document,
			want,
		)
		assert.ErrorIf(
			t,
			!slices.Equal(suppressed, []Suppression{want}) || len(warnings) != 0,
			"structured suppressions = %+v; warnings = %+v",
			suppressed,
			warnings,
		)
	} else {
		assert.FatalIf(
			t,
			len(document.Changes) == 0 || len(document.Suppressed) != 0 || len(warnings) != 0 ||
				len(suppressed) != 0,
			"control must remain comparable: %+v",
			document,
		)
	}
	images, notes, err := CompareImages(selected[0], selected[1], runErr, true)
	assert.ErrorIf(
		t,
		errors.Is(err, (*testCaseInput.selectedErr)) != testCaseInput.tc.keepError ||
			!slices.Equal(notes, suppressed),
		"image disclosure/error differs: %+v / %v",
		notes,
		err,
	)
	if testCaseInput.tc.withheld != (len(images) == 0) {
		t.Errorf("image withholding=%t; images=%v", testCaseInput.tc.withheld, images)
	}
}
