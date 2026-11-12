package baseline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func TestAdmitLocation_InputErrors(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"containment", "absent content root", "absent scan child", "selected ancestor open"} {
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
			location := &gitrepo.Location{ContentRoot: root, ScanPath: scan}
			if name == "selected ancestor open" {
				testutil.WriteFile(t, root, ".git", "gitdir: "+filepath.Join(root, "absent-metadata")+"\n")
				assert.NoError(t, os.Mkdir(scan, 0o750))
				var err error
				location, err = gitrepo.Locate(t.Context(), scan, "")
				assert.NoError(t, err)
			}
			_, err := admitLocation(
				t.Context(),
				location,
				manifest.WorkScope{},
			)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			if err == nil {
				t.Fatal("expected admission error")
			}
			assert.Equal(t, strings.Contains(err.Error(), scan), true)
			switch name {
			case "containment":
				assert.Equal(t, strings.Contains(err.Error(), root), true)
			case "selected ancestor open":
				assert.Equal(t, errors.Is(err, location.LookupErr), true)
				assert.Equal(t, errors.Is(err, git.ErrRepositoryNotExists), true)
				assert.Equal(t, strings.Contains(err.Error(), root), true)
			default:
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
		t.Run(

			tc.name,

			func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				wrapped := fmt.Errorf("admission: %w", tc.cause)
				_, err := admitLocation(
					admissionContextError{
						wrapped,
					},
					&gitrepo.Location{
						ContentRoot: root,
						ScanPath:    root,
					},
					manifest.WorkScope{},
				)
				assert.Equal(t, errors.Is(err, wrapped), true)
				assert.Equal(t, errors.Is(err, tc.cause), true)
				assert.Equal(t, errors.Is(err, manifest.ErrInput), false)
			},
		)
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
