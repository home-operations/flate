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
			root := t.TempDir()
			scan := filepath.Join(root, "missing")
			switch name {
			case "containment":
				scan = filepath.Join(t.TempDir(), "outside")
			case "absent content root":
				root = scan
			}
			_, err := admitLocation(t.Context(), &gitrepo.Location{ContentRoot: root, ScanPath: scan})
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			if err == nil {
				t.Fatal("expected admission error")
			}
			assert.Equal(t, strings.Contains(err.Error(), scan), true)
			if name == "containment" {
				assert.Equal(t, strings.Contains(err.Error(), root), true)
			} else {
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
				pathErr, ok := errors.AsType[*fs.PathError](err)
				assert.Equal(t, ok, true)
				if ok {
					assert.Equal(t, pathErr.Path, scan)
				}
			}
		})
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
			_, err := admitLocation(admissionErrorContext{wrapped}, &gitrepo.Location{ContentRoot: root, ScanPath: root})
			assert.Equal(t, errors.Is(err, wrapped), true)
			assert.Equal(t, errors.Is(err, tc.cause), true)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), false)
		})
	}
}

type admissionErrorContext struct{ error }

func (admissionErrorContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (admissionErrorContext) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
func (c admissionErrorContext) Err() error  { return c.error }
func (admissionErrorContext) Value(any) any { return nil }
