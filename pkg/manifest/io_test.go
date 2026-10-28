package manifest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/internal/assert"
)

func TestReleaseDoc_WithoutAllocations(t *testing.T) {
	docs, err := decodeDocMaps(strings.NewReader(strings.Repeat("kind: ConfigMap\n---\n", 101)))
	if err != nil {
		t.Fatal(err)
	}
	next := 0
	if got := testing.AllocsPerRun(100, func() {
		ReleaseDoc(docs[next])
		next++
	}); got != 0 {
		t.Fatalf("document release allocated %v times, want zero", got)
	}
}

func TestDecodeDocs_ScalarValues(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			docs, err := decodeDocMaps(strings.NewReader(tc.in))
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

func TestDecodeDocs_SkipsNonMappingDocs(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			docs, err := splitDocMaps([]byte(tc.in))
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

func TestDecodeDocs_AdvancesPastSkippedSeq(t *testing.T) {
	t.Parallel()
	in := "kind: First\n---\n- not\n- a\n- manifest\n---\nkind: Second\n"
	docs, err := splitDocMaps([]byte(in))
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

func TestDecodeDocs_CumulativeSkippedAliasBudget(t *testing.T) {
	t.Parallel()
	mapping := "a: &a [" + strings.Repeat("x,", 99) + "x]\nb: [" + strings.Repeat("*a,", 8) + "*a]\n"
	skipped := mapping + "? [bad, key]\n: ignored\n"
	prefix := strings.Repeat(skipped+"---\n", 650)
	tail := strings.Repeat(mapping+"---\n", 150)
	for _, tc := range []struct {
		name, input    string
		want, failures int
	}{
		{"recoverable budget", prefix, 650, 650}, {"independent tail", tail, 150, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			docs, err := DecodeDocs(strings.NewReader(tc.input), IncidentalDocuments)
			assert.NoError(t, err)
			assert.Equal(t, len(docs), tc.want)
			failures := 0
			for _, doc := range docs {
				if doc.Err != nil {
					failures++
				}
			}
			assert.Equal(t, failures, tc.failures)
		})
	}
	docs, err := DecodeDocs(strings.NewReader(prefix+tail), IncidentalDocuments)
	assert.NoError(t, err)
	if len(docs) >= 800 || len(docs) == 0 || !strings.Contains(docs[len(docs)-1].Err.Error(), "alias") {
		t.Fatalf("skipped documents reset stream budget: %d documents, %v", len(docs), DocumentFailure(docs))
	}
}

func TestDecodeDocs_RetainedOutcomes(t *testing.T) {
	t.Parallel()
	const input = "null\n---\ntext\n---\nkey: first\nkey: duplicate\n---\nkind: Sibling\n---\nbroken: [\n---\nkind: Unreachable\n"
	for _, role := range []DocumentRole{IncidentalDocuments, RequiredDocuments} {
		name := "incidental"
		if role == RequiredDocuments {
			name = "required"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			docs, err := DecodeDocs(strings.NewReader(input), role)
			assert.NoError(t, err)
			want := []int{2, 3, 4}
			if role == RequiredDocuments {
				want = []int{1, 2, 3, 4}
			}
			var ordinals []int
			for _, doc := range docs {
				ordinals = append(ordinals, doc.Ordinal)
				if doc.Ordinal == 3 {
					assert.Equal(t, doc.Document["kind"], any("Sibling"))
					continue
				}
				if doc.Document != nil || !errors.Is(doc.Err, ErrInput) || !errors.Is(doc.Err, ErrFlux) {
					t.Fatalf("invalid outcome: %+v", doc)
				}
				if doc.Ordinal == 2 {
					if _, ok := errors.AsType[*yaml.LoadErrors](doc.Err); !ok {
						t.Fatalf("constructor cause lost: %v", doc.Err)
					}
				}
			}
			assert.Diff(t, ordinals, want)
			again, err := DecodeDocs(strings.NewReader(input), role)
			assert.NoError(t, err)
			assert.Equal(t, DocumentFailure(again).Error(), DocumentFailure(docs).Error())
		})
	}
	for _, tc := range []struct {
		name, input string
		constructor bool
		terminal    bool
		shape       bool
	}{
		{name: "duplicate keys", input: "key: first\nkey: duplicate\n", constructor: true},
		{name: "complex key", input: "? [bad, key]\n: ignored\n", constructor: true},
		{name: "explicit tag", input: "value: !!int invalid\n", terminal: true},
		{name: "merge operand", input: "<<: 42\n", terminal: true},
		{name: "undefined alias", input: "value: *missing\n", terminal: true},
		{name: "syntax", input: "broken: [\n", terminal: true},
		{name: "scalar", input: "text\n", shape: true},
		{name: "sequence", input: "- name: value\n", shape: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, role := range []DocumentRole{IncidentalDocuments, RequiredDocuments} {
				name := "incidental"
				if role == RequiredDocuments {
					name = "required"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					input := "null\n---\n" + tc.input + "---\nkind: Sibling\n"
					docs, err := DecodeDocs(strings.NewReader(input), role)
					assert.NoError(t, err)
					var ordinals []int
					for _, doc := range docs {
						ordinals = append(ordinals, doc.Ordinal)
						if doc.Ordinal == 2 {
							assert.Equal(t, doc.Document["kind"], any("Sibling"))
							assert.NoError(t, doc.Err)
							continue
						}
						if doc.Ordinal != 1 || doc.Document != nil || !errors.Is(doc.Err, ErrInput) || !errors.Is(doc.Err, ErrFlux) {
							t.Fatalf("invalid outcome: %+v", doc)
						}
						_, constructor := errors.AsType[*yaml.LoadErrors](doc.Err)
						assert.Equal(t, constructor, tc.constructor)
					}
					want := []int{1, 2}
					if tc.terminal {
						want = []int{1}
					} else if tc.shape && role == IncidentalDocuments {
						want = []int{2}
					}
					assert.Diff(t, ordinals, want)
					again, err := DecodeDocs(strings.NewReader(input), role)
					assert.NoError(t, err)
					failure := DocumentFailure(docs)
					if failure == nil {
						assert.NoError(t, DocumentFailure(again))
					} else {
						assert.Equal(t, DocumentFailure(again).Error(), failure.Error())
					}
				})
			}
		})
	}
}

func TestRenderCodec_OutcomeOrdinals(t *testing.T) {
	t.Parallel()
	docs := []DocOccurrence{{Document: map[string]any{"kind": "ConfigMap"}, Ordinal: 7}}
	data, err := EncodeRender(docs)
	assert.NoError(t, err)
	restored, err := DecodeRender(data)
	assert.NoError(t, err)
	assert.Diff(t, restored, docs)
	failure := fmt.Errorf("%w: invalid document", ErrInput)
	docs = append(docs, DocOccurrence{Ordinal: 9, Err: failure})
	data, err = EncodeRender(docs)
	if data != nil || !errors.Is(err, failure) {
		t.Fatalf("failed subset cached: %v %v", data, err)
	}
}

func TestDecodeDocs_ReadFailures(t *testing.T) {
	t.Parallel()
	cause := errors.New("reader failed")
	for _, tc := range []struct {
		name, input     string
		together        bool
		ordinal, prefix int
	}{
		{name: "empty"},
		{name: "skipped and empty prefix", input: "kind: Prefix\n---\nnull\n---\ntext\n---\n\n---\npartial: [" + strings.Repeat(" ", 1024), ordinal: 4, prefix: 1},
		{name: "bytes with error", input: "kind: Prefix\n", together: true},
		{name: "trailing bytes with error", input: "kind: Prefix\n---\npartial: [" + strings.Repeat(" ", 32768), together: true, ordinal: 1, prefix: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			docs, err := DecodeDocs(&failingDocReader{input: tc.input, err: cause, together: tc.together}, IncidentalDocuments)
			assert.NoError(t, err)
			assert.Equal(t, len(docs), tc.prefix+1)
			if tc.prefix != 0 {
				assert.Equal(t, docs[0].Document["kind"], any("Prefix"))
				assert.Equal(t, docs[0].Ordinal, 0)
			}
			failure := docs[len(docs)-1]
			if failure.Ordinal != tc.ordinal || failure.Document != nil || !errors.Is(failure.Err, cause) || !errors.Is(failure.Err, ErrInput) || !errors.Is(failure.Err, ErrFlux) {
				t.Fatalf("retained read failure = %+v", failure)
			}
		})
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, fmt.Errorf("wrapped: %w", context.Canceled), fmt.Errorf("wrapped: %w", context.DeadlineExceeded)} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			docs, err := DecodeDocs(&failingDocReader{err: cause}, IncidentalDocuments)
			if !errors.Is(err, cause) || !errors.Is(err, ErrInput) || DocumentFailure(docs) != nil {
				t.Fatalf("cancellation retained: docs=%v err=%v", docs, err)
			}
		})
	}
}

type failingDocReader struct {
	input    string
	err      error
	together bool
}

func (r *failingDocReader) Read(p []byte) (int, error) {
	n := copy(p, r.input)
	r.input = r.input[n:]
	if n != 0 && (!r.together || r.input != "") {
		return n, nil
	}
	return n, r.err
}
