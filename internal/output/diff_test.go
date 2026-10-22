package output

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"
)

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
