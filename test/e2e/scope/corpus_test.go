package scope_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func testScopeCase(t *testing.T, scenario, name string) {
	t.Helper()
	fixture := harness.Open(t, scenario, name)
	want := fixture.Want(t)
	first, second := fixture.Run(t), fixture.Run(t)
	assertScopeResults(t, want, first)
	assertScopeResults(t, want, second)
	if !slices.Equal(first, second) {
		t.Fatal("complete result sequences differ")
	}
}

func assertScopeResults(t *testing.T, want, got []harness.Result) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("result count: want %d, got %d", len(want), len(got))
	}
	for i, result := range got {
		if result.Step != want[i].Step || result.Code != want[i].Code {
			t.Fatalf("result %d: want step %q exit %d, got step %q exit %d\nstdout %q\nstderr %q",
				i, want[i].Step, want[i].Code, result.Step, result.Code, result.Stdout, result.Stderr)
		}
		assertScopeStream(t, result.Step+" stdout", want[i].Stdout, result.Stdout)
		assertScopeStream(t, result.Step+" stderr", want[i].Stderr, result.Stderr)
	}
}

func assertScopeStream(t *testing.T, stream, want, got string) {
	t.Helper()
	if want == got {
		return
	}
	offset := 0
	for offset < min(len(want), len(got)) && want[offset] == got[offset] {
		offset++
	}
	start := max(0, offset-80)
	t.Fatalf("%s differs at byte %d (lengths %d/%d):\nwant %q\ngot  %q",
		stream, offset, len(want), len(got),
		want[start:min(len(want), offset+400)], got[start:min(len(got), offset+400)])
}
