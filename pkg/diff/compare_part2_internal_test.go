package diff

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

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
		selectedSnapshot(helmReleaseParent(), left, testCaseInput.tc.left),
		selectedSnapshot(helmReleaseParent(), right, testCaseInput.tc.right),
	}
	var runErr error
	if testCaseInput.tc.keepError {
		runErr = (*testCaseInput.selectedErr)
	}
	out, warnings, suppressed, err := Compare(selected[0], selected[1], runErr, Options{Format: FormatJSON})
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
		{
			assert.FatalIf(
				t,
				len(document.Changes) != 0 || len(document.Suppressed) != 1 || document.Suppressed[0] != want,
				"withheld output = %+v; want note %+v and no changes",
				document,
				want,
			)
		}
		{
			assert.ErrorIf(
				t,
				!slices.Equal(suppressed, []Suppression{want}) || len(warnings) != 0,
				"structured suppressions = %+v; warnings = %+v",
				suppressed,
				warnings,
			)
		}
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
	{
		assert.ErrorIf(
			t,
			errors.Is(err, (*testCaseInput.selectedErr)) != testCaseInput.tc.keepError ||
				!slices.Equal(notes, suppressed),
			"image disclosure/error differs: %+v / %v",
			notes,
			err,
		)
	}
	if testCaseInput.tc.withheld != (len(images) == 0) {
		t.Errorf("image withholding=%t; images=%v", testCaseInput.tc.withheld, images)
	}
}
