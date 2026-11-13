package diff

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEncoderDiffGroups_Bytes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		groups []diffGroup
		want   string
	}{
		{name: "empty", want: "[]\n"},
		{name: "singleton", groups: []diffGroup{{Cluster: "a", Body: []byte("existing\n")}}, want: "existing\n"},
		{name: "added singleton", groups: []diffGroup{{Cluster: "a", Added: true, Body: []byte(`{"changes":[],"suppressed":[]}`)}}, want: "[\n  {\n    \"added\": true,\n    \"changes\": [],\n    \"cluster\": \"a\",\n    \"suppressed\": []\n  }\n]\n"},
		{name: "removed singleton", groups: []diffGroup{{Cluster: "a", Removed: true, Body: []byte(`{"changes":[],"suppressed":[]}`)}}, want: "[\n  {\n    \"changes\": [],\n    \"cluster\": \"a\",\n    \"removed\": true,\n    \"suppressed\": []\n  }\n]\n"},
		{name: "json", groups: []diffGroup{{Cluster: "a", Removed: true, Body: []byte(`{"changes":[],"suppressed":[{"count":1}]}`)}, {Cluster: "b", Added: true, Body: []byte(`{"changes":[{"status":"added"}],"suppressed":[]}`)}}, want: "[\n  {\n    \"changes\": [],\n    \"cluster\": \"a\",\n    \"removed\": true,\n    \"suppressed\": [\n      {\n        \"count\": 1\n      }\n    ]\n  },\n  {\n    \"added\": true,\n    \"changes\": [\n      {\n        \"status\": \"added\"\n      }\n    ],\n    \"cluster\": \"b\",\n    \"suppressed\": []\n  }\n]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			assert.NoError(t, (encoder{Writer: &b, Format: FormatJSON}).diffGroups(tc.groups))
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoderDiffGroups_ReturnsErrors(t *testing.T) {
	t.Parallel()
	want := errors.New("write failed")
	groups := []diffGroup{{Cluster: "a", Body: []byte(`{"changes":[],"suppressed":[]}`)}, {Cluster: "b"}}
	{
		err := (encoder{Writer: writerError{want}, Format: FormatJSON}).diffGroups(groups)
		assert.FatalIf(t, !errors.Is(err, want), "DiffGroups = %v, want %v", err, want)
	}
	var b bytes.Buffer
	{
		err := (encoder{Writer: &b, Format: FormatJSON}).diffGroups(
			[]diffGroup{{Cluster: "a", Body: []byte("invalid")}, {Cluster: "b"}},
		)
		assert.FatalIf(t, err == nil ||
			b.Len() != 0, "invalid JSON body = %q, %v", b.String(), err)
	}
}

func TestEncoderLines_ReturnsErrors(t *testing.T) {
	t.Parallel()
	bad := prepared{Pairs: []pair{{New: map[string]any{"bad": make(chan int)}}}}
	{
		lines, err := prepareLines(bad)
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
		result := prepared{Pairs: []pair{
			{Old: map[string]any{"key": "old"}, New: map[string]any{"key": "new"}},
			{Old: map[string]any{"key": "before"}, New: map[string]any{"key": "after"}},
		}}
		lines, err := prepareLines(result)
		assert.NoError(t, err)
		assert.Equal(t, len(lines), 2)
		err = (encoder{Writer: writerError{writeErr}, Format: FormatDiff}).lines(lines)
		assert.FatalIf(t, !errors.Is(err, writeErr), "Lines = %v; want writer error after preparing both changes", err)
	})
}

func TestEncoderNative_ReturnsWriterError(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("write failed")
	t.Run("body", func(t *testing.T) {
		t.Parallel()
		report := dyff.Report{Diffs: []dyff.Diff{{}}}
		err := (encoder{Writer: writerError{writeErr}, Format: FormatBrief}).native(&report)
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
	left := []Doc{
		{Manifest: map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"name": "x"}, "data": "old"}},
	}
	right := []Doc{
		{Manifest: map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"name": "x"}, "data": "new"}},
	}
	before := []map[string]any{manifest.DeepCopyMap(left[0].Manifest), manifest.DeepCopyMap(right[0].Manifest)}
	calls := 0
	result := prepare(left, right, &Options{Normalize: func(m map[string]any) {
		calls++
		m["metadata"].(map[string]any)["name"] = "x-normalized"
	}})
	first, err := prepareLines(result)
	assert.NoError(t, err)
	assert.Equal(t, calls, 2)
	assert.Equal(t, len(first), 1)
	assert.Equal(t, first[0].Resource.ID.Name, "x-normalized")
	calls = 0
	second, err := prepareLines(result)
	assert.NoError(t, err)
	assert.Equal(t, calls, 0)
	assert.Diff(t, second, first)
	assert.Diff(t, []map[string]any{left[0].Manifest, right[0].Manifest}, before)
}

func TestEncoderNative_ColorFollowsEnvironment(t *testing.T) {
	cm := func(data string) Doc {
		return Doc{Manifest: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "x", "namespace": "default"}, "data": map[string]any{"k": data},
		}}
	}
	report, err := prepare([]Doc{cm("old")}, []Doc{cm("new")}, &Options{}).native()
	assert.NoError(t, err)
	for _, tc := range []struct {
		name  string
		env   map[string]string
		color bool
	}{
		{name: "pipe", color: false},
		{name: "forced", env: map[string]string{"CLICOLOR_FORCE": "1"}, color: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLICOLOR_FORCE", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			var out bytes.Buffer
			assert.NoError(t, (encoder{Writer: &out, Format: FormatHuman}).native(&report))
			assert.Equal(t, strings.Contains(out.String(), "\x1b["), tc.color)
		})
	}
}

type writerError struct{ err error }

func (w writerError) Write([]byte) (int, error) { return 0, w.err }
