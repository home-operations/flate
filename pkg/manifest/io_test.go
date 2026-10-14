package manifest

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
)

func TestDecodeDocs_ScalarValues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      string
		want    map[string]any
		wantErr bool
	}{
		{
			name: "implicit date",
			in:   "date: 2001-01-01\n",
			want: map[string]any{"date": "2001-01-01"},
		},
		{
			name: "explicit timestamp",
			in:   "date: !!timestamp 2001-01-01\n",
			want: map[string]any{"date": time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)},
		},
		{
			name: "anchored date",
			in:   "date: &date 2001-01-01\ncopy: *date\n",
			want: map[string]any{"date": "2001-01-01", "copy": "2001-01-01"},
		},
		{
			name: "alias bomb",
			in: "a: &a [x,x,x,x,x,x,x,x,x,x]\n" +
				"b: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a,*a]\n" +
				"c: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b,*b]\n" +
				"d: [*c,*c,*c,*c,*c,*c,*c,*c,*c,*c]\n",
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docs, err := DecodeDocs(strings.NewReader(tc.in))
			if tc.wantErr {
				if !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) {
					t.Fatalf("want input/Flux error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, docs, []map[string]any{tc.want})
		})
	}
}

// TestDecodeDocs_SkipsNonMappingDocs pins the fix for non-manifest YAML noise:
// a valid YAML document whose root is a sequence or scalar (e.g. an ansible
// task file) is not a Kubernetes manifest, so it is skipped — no error, no
// failed-to-parse WARN. Only a genuine syntax error aborts.
func TestDecodeDocs_SkipsNonMappingDocs(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    int // number of mapping docs returned
		wantErr bool
	}{
		{"top-level sequence (ansible)", "- name: a\n- name: b\n", 0, false},
		{"top-level scalar string", "just a string\n", 0, false},
		{"top-level scalar number", "42\n", 0, false},
		{"null doc", "null\n", 0, false},
		{"empty input", "", 0, false},
		{"single mapping", "apiVersion: v1\nkind: ConfigMap\n", 1, false},
		{"mappings interleaved with non-mappings", "- a\n---\nkind: A\n---\nfoo\n---\nkind: B\n", 2, false},
		{"malformed YAML aborts", "{ unterminated\n", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			docs, err := SplitDocs([]byte(tc.in))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got docs=%v", docs)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(docs) != tc.want {
				t.Fatalf("got %d docs, want %d: %v", len(docs), tc.want, docs)
			}
		})
	}
}

// TestDecodeDocs_AdvancesPastSkippedSeq confirms skipping a non-mapping doc
// advances the decoder so neighbouring manifests in the SAME file survive —
// previously a single top-level sequence failed the entire file, dropping the
// real manifests with it.
func TestDecodeDocs_AdvancesPastSkippedSeq(t *testing.T) {
	in := "kind: First\n---\n- not\n- a\n- manifest\n---\nkind: Second\n"
	docs, err := SplitDocs([]byte(in))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("got %d docs, want 2: %v", len(docs), docs)
	}
	if docs[0]["kind"] != "First" || docs[1]["kind"] != "Second" {
		t.Errorf("wrong docs survived: %v", docs)
	}
}
