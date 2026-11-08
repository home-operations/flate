package cache

import "time"

const (
	extractPolicy = "extract"
	payloadName   = "content"
	openOperation = "open"
	fixedPolicy   = "fixed"
	wrapOutcome   = "wrap"
	joinOutcome   = "join"
)

const (
	privateFileMode          = 0o600
	cacheDirectoryMode       = 0o750
	immutableFileMode        = 0o444
	unixPermissionMask       = 0o777
	tarBlockSize             = 512
	storeLockInterval        = 100 * time.Millisecond
	defaultMaximumBackoff    = 15 * time.Minute
	defaultMinimumBackoff    = 750 * time.Millisecond
	manifestSchemaVersion    = 2
	inventoryAnnotationCount = 2
)
