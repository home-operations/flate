package diff

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/gonvenience/ytbx"
	"github.com/homeport/dyff/pkg/dyff"
)

// dyffReport compares two inputs and renders the report in the given style
// for the native path (renderNative). An empty report yields no output:
// dyff would otherwise emit a stray newline the caller would mistake for a
// non-empty diff.
func dyffReport(from, to ytbx.InputFile, style Format) ([]byte, error) {
	report, err := dyff.CompareInputFiles(from, to)
	if err != nil {
		return nil, fmt.Errorf("dyff compare: %w", err)
	}
	report.Diffs = slices.DeleteFunc(report.Diffs, isFileLevelOrderChange)
	if len(report.Diffs) == 0 {
		return nil, nil
	}
	writer, err := dyffWriter(report, style)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := writer.WriteReport(&buf); err != nil {
		return nil, fmt.Errorf("dyff render: %w", err)
	}
	return buf.Bytes(), nil
}

// isFileLevelOrderChange reports whether d is dyff's document-SET "order
// changed" note: a nil path (dyff renders it as "(file level)") whose details
// are all ORDERCHANGE. A nil path uniquely marks that top-level
// document-sequence diff — every document add/remove/change and every
// in-resource diff carries a non-nil path.
//
// flate emits documents in a deterministic sorted order, so the document
// sequence never reorders on its own. dyff raises this note only when an
// add/remove/rename keeps the document COUNT equal but shifts identities in the
// list: its order check excludes modified documents but NOT added/removed ones
// (see homeport/dyff core.go), so a single rename — already reported as the
// remove + add — explodes into a wall of "order changed" spanning every
// untouched resource. Dropping it removes that noise. In-resource list reorders
// (container/env order) carry a non-nil path and are kept — that K8s-aware
// signal is exactly why dyff's order detection stays enabled.
func isFileLevelOrderChange(d dyff.Diff) bool {
	if d.Path != nil || len(d.Details) == 0 {
		return false
	}
	for _, detail := range d.Details {
		if detail.Kind != dyff.ORDERCHANGE {
			return false
		}
	}
	return true
}

// dyffWriter builds the dyff ReportWriter for a style. The diff-syntax
// styles differ only in their path/root/change prefixes; human and
// brief use their own report types. Configs mirror dyff's CLI so flate
// output matches `dyff between --output <style>`.
func dyffWriter(report dyff.Report, style Format) (dyff.ReportWriter, error) {
	switch style {
	case FormatGitHub:
		return diffSyntaxReport(report, "@@", "#", "!"), nil
	case FormatGitLab:
		return diffSyntaxReport(report, "=", "=", "#"), nil
	case FormatGitea:
		return diffSyntaxReport(report, "@@", "=", "!"), nil
	case FormatHuman:
		return &dyff.HumanReport{
			Report:                report,
			Indent:                2,
			UseIndentLines:        true,
			OmitHeader:            true,
			MultilineContextLines: 4,
			MinorChangeThreshold:  0.1,
		}, nil
	case FormatBrief:
		return &dyff.BriefReport{Report: report}, nil
	}
	return nil, fmt.Errorf("unsupported dyff style %q", style)
}

// diffSyntaxReport assembles a dyff DiffSyntaxReport with the given
// marker prefixes — the shape shared by the github/gitlab/gitea styles.
func diffSyntaxReport(report dyff.Report, pathPrefix, rootPrefix, changePrefix string) *dyff.DiffSyntaxReport {
	return &dyff.DiffSyntaxReport{
		PathPrefix:            pathPrefix,
		RootDescriptionPrefix: rootPrefix,
		ChangeTypePrefix:      changePrefix,
		HumanReport: dyff.HumanReport{
			Report:                report,
			Indent:                0,
			UseIndentLines:        true,
			NoTableStyle:          true,
			OmitHeader:            true,
			PrefixMultiline:       true,
			MultilineContextLines: 4,
			MinorChangeThreshold:  0.1,
		},
	}
}
