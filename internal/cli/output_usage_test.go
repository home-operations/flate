package cli

import (
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/format"
)

func TestOutputUsage(t *testing.T) {
	cases := []struct {
		name    string
		outputs []format.Output
		want    string
	}{
		{
			name:    "build set",
			outputs: []format.Output{format.OutputYAML, format.OutputJSON, format.OutputMarkdown},
			want:    "output format: table, yaml, json, markdown",
		},
		{
			name:    "table is implicit and not duplicated",
			outputs: []format.Output{format.OutputTable, format.OutputName},
			want:    "output format: table, name",
		},
		{
			name:    "no extra formats",
			outputs: nil,
			want:    "output format: table",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := outputUsage(tc.outputs); got != tc.want {
				t.Errorf("outputUsage = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestOutputFlagAdvertisesNameOnlyWhereSupported pins that each
// subcommand's -o help advertises `name` only where the command honors
// it (get ks/hr/images and diff images) — and never for build, test, or
// the diff/get summaries that reject it. Regression guard for the shared
// help that used to claim `name` everywhere ("--output=name is not a
// thing").
func TestOutputFlagAdvertisesNameOnlyWhereSupported(t *testing.T) {
	wantName := map[string]bool{
		"build ks":    false,
		"build hr":    false,
		"build all":   false,
		"diff ks":     false,
		"diff hr":     false,
		"diff all":    false,
		"diff images": true,
		"get ks":      true,
		"get hr":      true,
		"get images":  true,
		"get all":     false,
		"test ks":     false,
		"test hr":     false,
		"test all":    false,
	}

	got := map[string]bool{}
	for _, root := range []*cobra.Command{newBuildCmd(), newDiffCmd(), newGetCmd(), newTestCmd()} {
		for _, sub := range root.Commands() {
			f := sub.Flags().Lookup("output")
			if f == nil {
				continue
			}
			got[root.Name()+" "+sub.Name()] = slices.Contains(advertisedFormats(f.Usage), "name")
		}
	}

	for path, want := range wantName {
		gotName, ok := got[path]
		if !ok {
			t.Errorf("subcommand %q not found (renamed?)", path)
			continue
		}
		if gotName != want {
			t.Errorf("%q: -o usage advertises name=%v, want %v", path, gotName, want)
		}
	}
}

// advertisedFormats parses the comma-separated format list out of an -o
// flag usage string ("output format: table, yaml, json").
func advertisedFormats(usage string) []string {
	_, list, ok := strings.Cut(usage, ": ")
	if !ok {
		return nil
	}
	parts := strings.Split(list, ", ")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
