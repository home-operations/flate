package sourceref_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI invocations change process logging, environment, directories and transports.
func TestE2E_SourceRef_BootstrapCredentials(t *testing.T) {
	fixture := harness.Open(t, "bootstrap-credentials", "build-and-diff")
	want := fixture.Want(t)
	first, second := fixture.Run(t), fixture.Run(t)
	if !slices.Equal(first, want) {
		t.Errorf("first sequence:\nwant %#v\ngot %#v", want, first)
	}
	if !slices.Equal(second, want) {
		t.Errorf("second sequence:\nwant %#v\ngot %#v", want, second)
	}
	if !slices.Equal(first, second) {
		t.Errorf("replay differs:\nfirst %#v\nsecond %#v", first, second)
	}
}
