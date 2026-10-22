package cli

import (
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/output"
)

func TestBindOutput_Usage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		outputs []output.Format
		want    string
	}{
		{
			name:    "diff help order",
			outputs: output.DiffFormats,
			want:    "output format: human, github, diff, html, brief, gitlab, gitea, json",
		},
		{
			name:    "build set",
			outputs: []output.Format{output.FormatYAML, output.FormatJSON},
			want:    "output format: yaml, json",
		},
		{
			name:    "listed verbatim in order, default first",
			outputs: []output.Format{output.FormatTable, output.FormatYAML, output.FormatJSON, output.FormatName},
			want:    "output format: table, yaml, json, name",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var target string
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			bindOutput(fs, &target, tc.outputs...)
			if target != string(tc.outputs[0]) || fs.Lookup("output").DefValue != target || fs.Lookup("output").Shorthand != "o" {
				t.Fatalf("incorrect output binding: %s %#v", target, fs.Lookup("output"))
			}
			if got := fs.Lookup("output").Usage; got != tc.want {
				t.Errorf("output usage = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOutputFlags_CommandFormats(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		command, formats string
	}{
		{"build", "yaml, json"},
		{"build ks", "yaml, json"},
		{"build hr", "yaml, json"},
		{"build all", "yaml, json"},
		{"get", "yaml, json"},
		{"get all", "yaml, json"},
		{"get ks", "table, yaml, json, name"},
		{"get hr", "table, yaml, json, name"},
		{"get images", "name, yaml, json"},
		{"diff", "human, github, diff, html, brief, gitlab, gitea, json"},
		{"diff ks", "human, github, diff, html, brief, gitlab, gitea, json"},
		{"diff hr", "human, github, diff, html, brief, gitlab, gitea, json"},
		{"diff all", "human, github, diff, html, brief, gitlab, gitea, json"},
		{"diff images", "name, yaml, json"},
		{"test", ""},
		{"test ks", ""},
		{"test hr", ""},
		{"test all", ""},
	} {
		t.Run(tc.command, func(t *testing.T) {
			t.Parallel()
			cmd, _, err := New("test").Find(strings.Fields(tc.command))
			if err != nil {
				t.Fatal(err)
			}
			flag := cmd.Flags().Lookup("output")
			if tc.formats == "" {
				if flag != nil {
					t.Fatal("test has an output flag")
				}
				return
			}
			if flag == nil {
				t.Fatal("missing output binding")
			}
			formats := strings.Split(tc.formats, ", ")
			usage, _, _ := strings.Cut(flag.Usage, " [env: ")
			if usage != "output format: "+tc.formats || !strings.Contains(flag.Usage, envKey("output")) || flag.DefValue != formats[0] || flag.Shorthand != "o" {
				t.Fatalf("incorrect output binding: %#v", flag)
			}
			for _, format := range formats {
				if err := cmd.ParseFlags([]string{"-o", format}); err != nil {
					t.Errorf("rejects advertised format %s: %v", format, err)
				}
			}
			if err := cmd.ParseFlags([]string{"-o", "unsupported"}); err == nil || !strings.Contains(err.Error(), "must be one of: "+tc.formats) {
				t.Errorf("invalid output accepted: %v", err)
			}
		})
	}
}

func TestOutputFlags_TestRejectsBeforeRender(t *testing.T) {
	for _, command := range []string{"test", "test all", "test ks", "test hr"} {
		t.Run(command, func(t *testing.T) {
			args := append(strings.Fields(command), "--path", t.TempDir()+"/missing", "-o", "yaml")
			out, diagnostic, code := runCLI(t, args...)
			if code != 1 || out != "" || !strings.Contains(diagnostic, "unknown shorthand flag: 'o'") || strings.Contains(diagnostic, "does not exist") {
				t.Fatalf("output rejection happened after render: exit=%d stdout=%s stderr=%s", code, out, diagnostic)
			}
		})
	}
}
