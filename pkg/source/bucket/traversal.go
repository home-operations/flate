package bucket

import (
	"github.com/home-operations/flate/pkg/source/safepath"
)

// safeJoinUnderSlot validates and joins a bucket-relative key into the
// cache slot. A malicious bucket — either deliberately or via stale
// curation — can produce a key whose filepath.FromSlash form contains
// `..` enough times to climb past slot; filepath.Join happily cleans
// the climb-out without complaint. Without this guard such a key would
// write arbitrary files on the host.
//
// Delegates to safepath.SafeJoin with rejectAbsolute=false: bucket keys
// aren't tar headers, so an absolute-looking key (e.g. "/etc/passwd")
// stays under slot by virtue of filepath.Join's component-boundary
// handling, which traversal_test asserts. See also the OCI caller in
// pkg/source/oci/layer.go, which uses rejectAbsolute=true.
func safeJoinUnderSlot(slot, rel string) (string, error) {
	return safepath.SafeJoin(slot, rel, false)
}
