package cli

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

func TestFinishSelected_SuppliedAdvisoryUnion(t *testing.T) {
	t.Parallel()
	ordinary := manifest.Warning{
		Resource: testutil.HelmReleaseID("apps", "demo"),
		Category: manifest.WarnStaleValues,
		Message:  "ordinary",
		Detail:   []string{"kept"},
		Count:    3,
	}
	other := ordinary
	other.Message, other.Detail = "another ordinary", []string{"another", "kept"}
	inv := &selectedInvocation{}
	for _, cluster := range []string{"alpha", "omega"} {
		advisories := []manifest.Warning{
			{
				Category: manifest.WarnSuppliedPrecedence,
				Message:  "repository wins",
				Detail:   []string{cluster, "common", "first", "last"},
				Count:    1,
			},
		}
		inv.groups = append(inv.groups, selectedGroup{cluster: cluster, sides: []*selection.Result{
			{Warnings: []manifest.Warning{ordinary, other}},
			{Warnings: []manifest.Warning{ordinary, other}, ClusterWarnings: advisories},
		}})
	}
	var first string
	for run := range 2 {
		var stderr bytes.Buffer
		assert.NoError(
			t,
			finishSelected(&selectedRequest{pair: true, stdout: io.Discard, stderr: &stderr}, inv, &selectedPayload{}),
		)
		text := stderr.String()
		for _, token := range []string{"warnings (10)"} {
			assert.Equal(t, strings.Count(text, token), 1)
		}
		assert.Equal(t, strings.Count(text, "[SuppliedPrecedence]"), 2)
		assert.Equal(t, strings.Count(text, "[StaleValues]"), 8)
		finishSelectedSuppliedAdvisoryUnionLoop(t, struct {
			text     *string
			ordinary *struct {
				Resource manifest.NamedResource
				Subject  manifest.DiagnosticIdentity
				Related  []manifest.NamedResource
				Category string
				Message  string
				Detail   []string
				Count    int
			}
			other *struct {
				Resource manifest.NamedResource
				Subject  manifest.DiagnosticIdentity
				Related  []manifest.NamedResource
				Category string
				Message  string
				Detail   []string
				Count    int
			}
		}{text: &text, ordinary: (*struct {
			Resource manifest.NamedResource
			Subject  manifest.DiagnosticIdentity
			Related  []manifest.NamedResource
			Category string
			Message  string
			Detail   []string
			Count    int
		})(&ordinary), other: (*struct {
			Resource manifest.NamedResource
			Subject  manifest.DiagnosticIdentity
			Related  []manifest.NamedResource
			Category string
			Message  string
			Detail   []string
			Count    int
		})(&other)})
		assert.FatalIf(
			t,
			strings.Contains(text, "Cluster:") || strings.Contains(text, output.GlyphFail),
			"advisories changed report framing or verdict: %s",
			text,
		)
		if run == 1 {
			assert.Equal(t, text, first)
		}
		first = text
		for _, group := range inv.groups {
			for _, side := range group.sides {
				assert.Diff(t, side.Warnings, []manifest.Warning{ordinary, other})
			}
		}
	}
	fatal := errors.New("fatal render failure")
	var stderr bytes.Buffer
	err := finishSelected(
		&selectedRequest{pair: true, stdout: io.Discard, stderr: &stderr},
		inv,
		&selectedPayload{err: fatal},
	)
	assert.FatalIf(
		t,
		!errors.Is(err, fatal) || !strings.Contains(stderr.String(), "another ordinary (×3)"),
		"warnings changed fatal result or lost messages: %v\n%s",
		err,
		stderr.String(),
	)
}

func finishSelectedSuppliedAdvisoryUnionLoop(t *testing.T, loopInputs struct {
	text     *string
	ordinary *struct {
		Resource manifest.NamedResource
		Subject  manifest.DiagnosticIdentity
		Related  []manifest.NamedResource
		Category string
		Message  string
		Detail   []string
		Count    int
	}
	other *struct {
		Resource manifest.NamedResource
		Subject  manifest.DiagnosticIdentity
		Related  []manifest.NamedResource
		Category string
		Message  string
		Detail   []string
		Count    int
	}
},
) {
	t.Helper()

	for _, cluster := range []string{"alpha", "omega"} {
		for _, row := range []string{
			"[SuppliedPrecedence] " + cluster + ": repository wins\n      " + cluster + ", common, first, last\n",
		} {
			assert.Equal(t, strings.Count((*loopInputs.text), row), 1)
		}
		for _, context := range []string{"orig snapshot", "current snapshot"} {
			for warningIndex := range []manifest.Warning{(*loopInputs.ordinary), (*loopInputs.other)} {
				warning := []manifest.Warning{(*loopInputs.ordinary), (*loopInputs.other)}[warningIndex]
				row := "[StaleValues] " + context + ": HelmRelease " + cluster + ": apps/demo: " + warning.Message + " (×3)\n      " + strings.Join(
					warning.Detail,
					", ",
				) + "\n"
				assert.Equal(t, strings.Count((*loopInputs.text), row), 1)
			}
		}
	}
}
