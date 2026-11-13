package cli

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // Native diff output changes process-wide color settings.
func TestDiffPayload_GroupedSDKEquivalence(t *testing.T) {
	for _, format := range diff.Formats() {
		t.Run(string(format), func(t *testing.T) {
			selectedErr := errors.New("selected fixture failure")
			inv := groupedComparisonFixture(selectedErr)
			before := groupedComparisonFixture(selectedErr)
			attrs := []string{"example.test/ignored", "untouched attribute"}
			fields := []string{"spec.noise", "untouched field"}
			beforeAttrs, beforeFields := slices.Clone(attrs), slices.Clone(fields)
			flags := diffFlags{stripAttrs: attrs[:1], stripFields: fields[:1]}
			var cliOutput, diagnostics bytes.Buffer
			payload := flags.payload(&inv, output.Encoder{Writer: &cliOutput, Format: output.Format(format)})
			assert.NoError(t, payload.err)

			groups := make([]diff.Group, len(inv.groups))
			for i, group := range inv.groups {
				groups[i] = diff.Group{
					Cluster: group.cluster,
					Orig:    clusterSnapshot(group.sides[0]),
					Current: clusterSnapshot(group.sides[1]),
				}
			}
			groups[0].SelectedErr = selectedErr
			beforeGroups := slices.Clone(groups)
			calls := make(map[string]int)
			opts := diff.Options{
				Format:      format,
				StripAttrs:  attrs[:1],
				StripFields: fields[:1],
				Normalize: func(doc map[string]any) {
					metadata := doc["metadata"].(map[string]any)
					data := doc["data"].(map[string]any)
					calls[metadata["name"].(string)+"/"+data["value"].(string)]++
				},
			}
			comparison, err := diff.Compare(groups, opts)
			assert.FatalIf(t, !errors.Is(err, selectedErr), "SDK selected error lost: %v", err)
			assert.Equal(t, strings.Count(err.Error(), selectedErr.Error()), 1)
			wantCalls := map[string]int{"steady/stable": 2, "config/old": 1, "config/new": 1}
			assert.Diff(t, calls, wantCalls)
			assert.Equal(t, len(comparison.Groups), 2)
			assert.Equal(t, len(payload.warnings), 2)
			assert.Equal(t, len(payload.subjects), 2)
			failed := inv.groups[0].sides[1].Manifests[0].Parent.Named()
			subject := manifest.DiagnosticIdentity{ID: failed, Resource: failed, Path: "clusters/alpha"}
			note := diff.Suppression{
				Parent:  diff.Parent{NamedResource: failed},
				Subject: subject,
				Side:    diff.SideCurrent,
				Count:   1,
				Reason:  "secret apps/auth not found",
			}
			wantNotes := [][]diff.Suppression{{note}, nil}
			for i, result := range comparison.Groups {
				assert.Diff(t, result.Warnings, []manifest.Warning(nil))
				assert.Diff(t, result.Suppressed, wantNotes[i])
				assert.Diff(t, payload.warnings[i], diff.MergeWarnings(result.Warnings, result.Suppressed))
				assert.Diff(t, payload.subjects[i], result.Subjects)
			}
			assert.Diff(t, payload.warnings[0], []manifest.Warning{{
				Resource: failed, Subject: subject, Category: manifest.WarnDiffSuppressed,
				Message: "1 object not rendered on the current side; diff suppressed (secret apps/auth not found)",
			}})
			assert.Diff(t, payload.warnings[1], []manifest.Warning(nil))

			var sdkFirst, sdkSecond bytes.Buffer
			assert.NoError(t, comparison.Write(&sdkFirst, nil))
			assert.NoError(t, comparison.Write(&sdkSecond, nil))
			assert.FatalIf(t, sdkFirst.Len() == 0, "changed documents produced empty %s output", format)
			assert.Diff(t, sdkSecond.Bytes(), sdkFirst.Bytes())
			req := selectedRequest{pair: true, stdout: &cliOutput, stderr: &diagnostics}
			firstErr := finishSelected(&req, &inv, &payload)
			assert.FatalIf(t, !errors.Is(firstErr, selectedErr), "CLI selected error lost: %v", firstErr)
			assert.Equal(t, strings.Count(firstErr.Error(), selectedErr.Error()), 1)
			assert.Equal(t, strings.Count(diagnostics.String(), selectedErr.Error()), 1)
			assert.Diff(t, cliOutput.Bytes(), sdkFirst.Bytes())
			firstDiagnostics := diagnostics.String()
			cliOutput.Reset()
			diagnostics.Reset()
			secondErr := finishSelected(&req, &inv, &payload)
			assert.FatalIf(
				t,
				!errors.Is(secondErr, selectedErr),
				"repeated CLI write lost selected error: %v",
				secondErr,
			)
			assert.Diff(t, cliOutput.Bytes(), sdkFirst.Bytes())
			assert.Diff(t, diagnostics.String(), firstDiagnostics)
			assert.Diff(t, calls, wantCalls)
			inputEquality := cmp.Options{
				cmp.AllowUnexported(
					selectedInvocation{},
					selectedGroup{},
					manifest.Kustomization{},
					manifest.HelmRelease{},
					store.StatusInfo{},
				),
				cmp.Comparer(func(a, b error) bool { return errors.Is(a, b) && errors.Is(b, a) }),
			}
			assert.ErrorIf(
				t,
				!cmp.Equal(inv, before, inputEquality),
				"comparison mutated selected snapshots or invocation",
			)
			assert.ErrorIf(t, !cmp.Equal(groups, beforeGroups, inputEquality), "comparison mutated group input")
			assert.Diff(t, attrs, beforeAttrs)
			assert.Diff(t, fields, beforeFields)
			assert.Diff(t, opts.StripAttrs, beforeAttrs[:1])
			assert.Diff(t, opts.StripFields, beforeFields[:1])
			assert.Diff(t, opts.Suppressed, []diff.Suppression(nil))
			assert.Equal(t, opts.Format, format)
		})
	}
}

func groupedComparisonFixture(selectedErr error) selectedInvocation {
	failed := &manifest.HelmRelease{Name: "hidden", Namespace: "apps"}
	healthy := &manifest.Kustomization{Name: "root", Namespace: "apps"}
	healthy.Path = "./apps"
	failedID := failed.Named()
	subject := manifest.DiagnosticIdentity{ID: failedID, Resource: failedID, Path: "clusters/alpha"}
	return selectedInvocation{
		err: selectedErr, remainder: selectedErr,
		groups: []selectedGroup{
			{cluster: "alpha", sides: []*selection.Result{
				{
					Subjects: map[manifest.NamedResource]manifest.DiagnosticIdentity{failedID: subject},
					Manifests: []manifest.DocumentBatch{
						{
							Parent:    failed,
							Documents: []map[string]any{groupedComparisonDocument("hidden", "withheld", "old")},
						},
						{
							Parent:    healthy,
							Documents: []map[string]any{groupedComparisonDocument("steady", "stable", "old")},
						},
					},
				},
				{
					Subjects: map[manifest.NamedResource]manifest.DiagnosticIdentity{failedID: subject},
					Manifests: []manifest.DocumentBatch{
						{Parent: failed},
						{
							Parent:    healthy,
							Documents: []map[string]any{groupedComparisonDocument("steady", "stable", "new")},
						},
					},
					Unavailable: map[manifest.NamedResource]store.StatusInfo{
						failedID: {Status: store.StatusFailed, Message: "secret apps/auth not found"},
					},
				},
			}},
			{cluster: "beta", sides: []*selection.Result{
				{
					Manifests: []manifest.DocumentBatch{
						{
							Parent:    healthy,
							Documents: []map[string]any{groupedComparisonDocument("config", "old", "old")},
						},
					},
				},
				{
					Manifests: []manifest.DocumentBatch{
						{
							Parent:    healthy,
							Documents: []map[string]any{groupedComparisonDocument("config", "new", "new")},
						},
					},
				},
			}},
		},
	}
}

func groupedComparisonDocument(name, value, noise string) map[string]any {
	return map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{
			"namespace": "apps", "name": name,
			"annotations": map[string]any{"example.test/ignored": noise},
		},
		"data": map[string]any{"value": value},
		"spec": map[string]any{"noise": noise},
	}
}
