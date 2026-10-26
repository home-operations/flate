package output

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"

	"github.com/home-operations/flate/internal/assert"
)

func TestEncoderDiffGroups_Bytes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		format Format
		groups []DiffGroup
		want   string
	}{
		{name: "singleton", format: FormatDiff, groups: []DiffGroup{{Cluster: "a", Body: []byte("existing\n")}}, want: "existing\n"},
		{name: "human", format: FormatHuman, groups: []DiffGroup{{Cluster: "a", Body: []byte("existing\n")}, {Cluster: "b"}}, want: "Cluster: a\nexisting\n\nCluster: b\n\n"},
		{name: "brief", format: FormatBrief, groups: []DiffGroup{{Cluster: "a", Removed: true}, {Cluster: "b", Added: true}}, want: "Cluster: a (removed)\n\nCluster: b (added)\n\n"},
		{name: "diff", format: FormatDiff, groups: []DiffGroup{{Cluster: "a", Body: []byte("--- old\n+++ new\n")}, {Cluster: "b", Added: true}}, want: "Cluster: a\n--- old\n+++ new\n\nCluster: b (added)\n\n"},
		{name: "github", format: FormatGitHub, groups: []DiffGroup{{Cluster: "a", Body: []byte("existing\n")}, {Cluster: "b", Removed: true}}, want: "## Cluster: a\n\nexisting\n\n## Cluster: b (removed)\n\n\n"},
		{name: "gitlab", format: FormatGitLab, groups: []DiffGroup{{Cluster: "a", Added: true}}, want: "## Cluster: a (added)\n\n\n"},
		{name: "gitea", format: FormatGitea, groups: []DiffGroup{{Cluster: "a", Removed: true}}, want: "## Cluster: a (removed)\n\n\n"},
		{name: "json", format: FormatJSON, groups: []DiffGroup{{Cluster: "a", Removed: true, Body: []byte(`{"changes":[],"suppressed":[{"count":1}]}`)}, {Cluster: "b", Added: true, Body: []byte(`{"changes":[{"status":"added"}],"suppressed":[]}`)}}, want: "[\n  {\n    \"changes\": [],\n    \"cluster\": \"a\",\n    \"removed\": true,\n    \"suppressed\": [\n      {\n        \"count\": 1\n      }\n    ]\n  },\n  {\n    \"added\": true,\n    \"changes\": [\n      {\n        \"status\": \"added\"\n      }\n    ],\n    \"cluster\": \"b\",\n    \"suppressed\": []\n  }\n]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).DiffGroups(tc.groups); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoderDiffGroups_ReturnsErrors(t *testing.T) {
	t.Parallel()
	want := errors.New("write failed")
	for _, format := range []Format{FormatHuman, FormatJSON, FormatHTML} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			groups := []DiffGroup{{Cluster: "a", Body: []byte(`{"changes":[],"suppressed":[]}`)}, {Cluster: "b"}}
			if err := (Encoder{Writer: errorWriter{want}, Format: format}).DiffGroups(groups); !errors.Is(err, want) {
				t.Fatalf("DiffGroups = %v, want %v", err, want)
			}
		})
	}
	var b bytes.Buffer
	if err := (Encoder{Writer: &b, Format: FormatJSON}).DiffGroups([]DiffGroup{{Cluster: "a", Body: []byte("invalid")}, {Cluster: "b"}}); err == nil || b.Len() != 0 {
		t.Fatalf("invalid JSON body = %q, %v", b.String(), err)
	}
}

func TestEncoderLines_ReturnsErrors(t *testing.T) {
	t.Parallel()
	prepareErr := errors.New("preparation failed")
	for _, format := range []Format{FormatDiff, FormatHTML} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			starts, consumed := 0, 0
			changes := func(yield func(LineChange, error) bool) {
				starts++
				for range 2 {
					consumed++
					if !yield(LineChange{}, prepareErr) {
						return
					}
				}
			}
			var buf bytes.Buffer
			err := (Encoder{Writer: &buf, Format: format}).Lines(changes)
			if !errors.Is(err, prepareErr) || starts != 1 || consumed != 1 {
				t.Fatalf("Lines = %v, starts %d, consumed %d; want preparation error after one value", err, starts, consumed)
			}
		})
	}
	t.Run("writer stops changes", func(t *testing.T) {
		t.Parallel()
		writeErr := errors.New("write failed")
		consumed := 0
		changes := func(yield func(LineChange, error) bool) {
			for range 2 {
				consumed++
				if !yield(LineChange{From: "key: old\n", To: "key: new\n", Status: "changed"}, nil) {
					return
				}
			}
		}
		err := (Encoder{Writer: errorWriter{writeErr}, Format: FormatDiff}).Lines(changes)
		if !errors.Is(err, writeErr) || consumed != 1 {
			t.Fatalf("Lines = %v, consumed %d changes; want writer error after one change", err, consumed)
		}
	})
}

func TestEncoderNative_ReturnsWriterError(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("write failed")
	t.Run("body", func(t *testing.T) {
		t.Parallel()
		report := dyff.Report{Diffs: []dyff.Diff{{}}}
		err := (Encoder{Writer: errorWriter{writeErr}, Format: FormatBrief}).Native(report)
		if !errors.Is(err, writeErr) || !strings.HasPrefix(err.Error(), "dyff render: ") {
			t.Fatalf("Native = %v; want wrapped dyff render writer error", err)
		}
	})
}
