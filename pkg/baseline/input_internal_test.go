package baseline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func TestAdmitLocation_InputErrors(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"containment", "absent content root", "absent scan child"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testAdmitLocationInputErrorsCase(t, &name)
		},
		)
	}
}

func TestAdmitLocation_Cancellation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "canceled", cause: context.Canceled},
		{name: "deadline", cause: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			wrapped := fmt.Errorf("admission: %w", tc.cause)
			_, err := admitLocation(
				admissionContextError{wrapped},
				&gitrepo.Location{ContentRoot: root, ScanPath: root},
			)
			assert.Equal(t, errors.Is(err, wrapped), true)
			assert.Equal(t, errors.Is(err, tc.cause), true)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), false)
		})
	}
}

type admissionContextError struct{ error }

func (admissionContextError) Deadline() (time.Time, bool) { return time.Time{}, false }
func (admissionContextError) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (c admissionContextError) Err() error  { return c.error }
func (admissionContextError) Value(any) any { return nil }

func testAdmitLocationInputErrorsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	scan := filepath.Join(root, "missing")
	switch *testCaseInput {
	case "containment":
		scan = filepath.Join(t.TempDir(), "outside")
	case "absent content root":
		root = scan
	}
	_, err := admitLocation(t.Context(), &gitrepo.Location{ContentRoot: root, ScanPath: scan})
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	{
		assert.FatalIf(t, err == nil, "%v", "expected admission error")
	}
	assert.Equal(t, strings.Contains(err.Error(), scan), true)
	if (*testCaseInput) == "containment" {
		assert.Equal(t, strings.Contains(err.Error(), root), true)
	} else {
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		pathErr, ok := errors.AsType[*fs.PathError](err)
		assert.Equal(t, ok, true)
		if ok {
			assert.Equal(t, pathErr.Path, scan)
		}
	}
}
