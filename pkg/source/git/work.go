package git

import (
	"errors"

	"github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/pkg/manifest"
)

func recordTransfer(accounting *manifest.AcquisitionWork, protocol string, err error) {
	work := accounting.Physical()
	switch {
	case protocol == "file":
		work.Add(&manifest.WorkCounts{LocalAcquisitions: 1})
	case errors.Is(err, git.NoErrAlreadyUpToDate):
		accounting.Validate().Add(&manifest.WorkCounts{Attempts: 1, Completed: 1})
	case err == nil:
		work.Add(&manifest.WorkCounts{RemoteTransfers: 1})
	}
}
