package output

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diffprep"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEncoderDiffGroups_Bytes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		groups []DiffGroup
		want   string
	}{
		{name: "singleton", groups: []DiffGroup{{Cluster: "a", Body: []byte("existing\n")}}, want: "existing\n"},
		{name: "json", groups: []DiffGroup{{Cluster: "a", Removed: true, Body: []byte(`{"changes":[],"suppressed":[{"count":1}]}`)}, {Cluster: "b", Added: true, Body: []byte(`{"changes":[{"status":"added"}],"suppressed":[]}`)}}, want: "[\n  {\n    \"changes\": [],\n    \"cluster\": \"a\",\n    \"removed\": true,\n    \"suppressed\": [\n      {\n        \"count\": 1\n      }\n    ]\n  },\n  {\n    \"added\": true,\n    \"changes\": [\n      {\n        \"status\": \"added\"\n      }\n    ],\n    \"cluster\": \"b\",\n    \"suppressed\": []\n  }\n]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			assert.NoError(t, (Encoder{Writer: &b, Format: FormatJSON}).DiffGroups(tc.groups))
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoderDiffGroups_ReturnsErrors(t *testing.T) {
	t.Parallel()
	want := errors.New("write failed")
	groups := []DiffGroup{{Cluster: "a", Body: []byte(`{"changes":[],"suppressed":[]}`)}, {Cluster: "b"}}
	{
		err := (Encoder{Writer: errorWriter{want}, Format: FormatJSON}).DiffGroups(groups)
		assert.FatalIf(t, !errors.Is(err, want), "DiffGroups = %v, want %v", err, want)
	}
	var b bytes.Buffer
	{
		err := (Encoder{Writer: &b, Format: FormatJSON}).DiffGroups(
			[]DiffGroup{{Cluster: "a", Body: []byte("invalid")}, {Cluster: "b"}},
		)
		assert.FatalIf(t, err == nil ||
			b.Len() != 0, "invalid JSON body = %q, %v", b.String(), err)
	}
}

func TestEncoderLines_ReturnsErrors(t *testing.T) {
	t.Parallel()
	bad := diffprep.Prepared{Pairs: []diffprep.Pair{{New: map[string]any{"bad": make(chan int)}}}}
	{
		lines, err := PrepareLines(bad)
		assert.FatalIf(
			t,
			err == nil || len(lines) != 0,
			"PrepareLines = %+v, %v; want serialization failure without partial output",
			lines,
			err,
		)
	}
	t.Run("writer returns error after preparation", func(t *testing.T) {
		t.Parallel()
		writeErr := errors.New("write failed")
		prepared := diffprep.Prepared{Pairs: []diffprep.Pair{
			{Old: map[string]any{"key": "old"}, New: map[string]any{"key": "new"}},
			{Old: map[string]any{"key": "before"}, New: map[string]any{"key": "after"}},
		}}
		lines, err := PrepareLines(prepared)
		assert.NoError(t, err)
		assert.Equal(t, len(lines), 2)
		err = (Encoder{Writer: errorWriter{writeErr}, Format: FormatDiff}).Lines(lines)
		assert.FatalIf(t, !errors.Is(err, writeErr), "Lines = %v; want writer error after preparing both changes", err)
	})
}

func TestEncoderNative_ReturnsWriterError(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("write failed")
	t.Run("body", func(t *testing.T) {
		t.Parallel()
		report := dyff.Report{Diffs: []dyff.Diff{{}}}
		err := (Encoder{Writer: errorWriter{writeErr}, Format: FormatBrief}).Native(&report)
		assert.FatalIf(
			t,
			!errors.Is(err, writeErr) || !strings.HasPrefix(err.Error(), "dyff render: "),
			"Native = %v; want wrapped dyff render writer error",
			err,
		)
	})
}

func TestPrepareLines_NormalizeOnce(t *testing.T) {
	t.Parallel()
	left := []diffprep.Doc{
		{Manifest: map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"name": "x"}, "data": "old"}},
	}
	right := []diffprep.Doc{
		{Manifest: map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"name": "x"}, "data": "new"}},
	}
	before := []map[string]any{manifest.DeepCopyMap(left[0].Manifest), manifest.DeepCopyMap(right[0].Manifest)}
	calls := 0
	prepared := diffprep.Prepare(left, right, &diffprep.Options{Normalize: func(m map[string]any) {
		calls++
		m["metadata"].(map[string]any)["name"] = "x-normalized"
	}})
	first, err := PrepareLines(prepared)
	assert.NoError(t, err)
	assert.Equal(t, calls, 2)
	assert.Equal(t, len(first), 1)
	assert.Equal(t, first[0].Resource.ID.Name, "x-normalized")
	calls = 0
	second, err := PrepareLines(prepared)
	assert.NoError(t, err)
	assert.Equal(t, calls, 0)
	assert.Diff(t, second, first)
	assert.Diff(t, []map[string]any{left[0].Manifest, right[0].Manifest}, before)
}
