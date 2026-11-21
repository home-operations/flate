package orchestrator

import (
	"time"

	engine "github.com/home-operations/flate/internal/engine/orchestrator"
	"github.com/home-operations/flate/pkg/manifest"
)

// Run MUST remain open until all consumers finish with its borrowed results.
type Run struct {
	Results    []Rendered
	invocation *engine.Invocation
}

// Tree identities MUST distinguish cluster and comparison side within one invocation.
type Tree struct {
	ID       manifest.TreeID
	RepoRoot string
	Path     string
	SelfURLs []string
}

// Config collections MUST remain immutable until the returned run closes.
type Config struct {
	RecordWork          bool
	Selection           SelectionRequest
	HelmOptions         HelmOptions
	AllowMissingKinds   []string
	Supply              []SuppliedDocument
	StrictSubstitutions bool
	RestrictEgress      bool
	RegistryConfig      string
	CacheDir            string
	StatusListener      func(manifest.TreeID, manifest.DiagnosticIdentity, StatusInfo)
	Concurrency         int
	HeadOnly            bool
	SourceRetry         RetryConfig
	GitDepth            int
}

// SelectionRequest applies roots and labels within the owned cluster scope.
type SelectionRequest struct {
	Roots   []manifest.NamedResource
	Changed bool
	Labels  map[string]string
}

// HelmOptions apply to every chart rendered by the invocation.
type HelmOptions struct {
	DisableChartDigestTracking bool
	SkipCRDs                   bool
	SkipTests                  bool
	SkipKinds                  []string
	KubeVersion                string
	APIVersions                string
	EnableDNS                  bool
}

// RetryConfig bounds source retries without changing source identity.
type RetryConfig struct {
	Attempts int
	MinWait  time.Duration
	MaxWait  time.Duration
	Jitter   float64
}

// SuppliedDocument assigns supplied authority only after parsing and input isolation.
type SuppliedDocument struct {
	Doc       map[string]any
	InputPath string
	Ordinal   int
}

// Rendered can be complete even when its result contains resource failures.
type Rendered struct {
	ID       manifest.TreeID
	Absent   bool
	Complete bool
	Result   *Result
	Err      error
}

// Result borrows manifest payloads and warning details until Run.Close.
type Result struct {
	Work            manifest.WorkSummary
	Manifests       []manifest.DocumentBatch
	Failed          map[manifest.NamedResource]StatusInfo
	Orphans         map[manifest.NamedResource]string
	Blocked         map[manifest.NamedResource][]manifest.NamedResource
	Warnings        []manifest.Warning
	ClusterWarnings []manifest.Warning
}

// Status MUST preserve the projected store status without reclassification.
type Status string

// StatusPending, StatusReady, and StatusFailed preserve the store status spellings.
const (
	StatusPending Status = "Pending"
	StatusReady   Status = "Ready"
	StatusFailed  Status = "Failed"
)

// StatusReason MUST preserve the projected store reason without interpreting message text.
type StatusReason string

// ReasonSkipped, ReasonUnchanged, and ReasonSuspended preserve the store reason spellings.
const (
	ReasonSkipped   StatusReason = "Skipped"
	ReasonUnchanged StatusReason = "Unchanged"
	ReasonSuspended StatusReason = "Suspended"
)

// Outcome MUST preserve dependency-failure provenance from the store.
type Outcome string

// OutcomePending and the terminal outcomes preserve the store outcome spellings.
const (
	OutcomePending   Outcome = "Pending"
	OutcomePassed    Outcome = "Passed"
	OutcomeFailed    Outcome = "Failed"
	OutcomeBlocked   Outcome = "Blocked"
	OutcomeSkipped   Outcome = "Skipped"
	OutcomeUnchanged Outcome = "Unchanged"
	OutcomeSuspended Outcome = "Suspended"
)

// StatusInfo retains the formatted message and semantic outcome from one store snapshot.
type StatusInfo struct {
	Status  Status
	Reason  StatusReason
	Message string
	Outcome Outcome
}

// Selection borrows completed facts until Run.Close and never owns invocation resources.
type Selection struct {
	Subjects        map[manifest.NamedResource]manifest.DiagnosticIdentity
	Manifests       []manifest.DocumentBatch
	Failed          map[manifest.NamedResource]StatusInfo
	Blocked         map[manifest.NamedResource][]manifest.NamedResource
	Warnings        []manifest.Warning
	ClusterWarnings []manifest.Warning
	Objects         []manifest.BaseManifest
	Counts          map[string]int
	Statuses        map[manifest.NamedResource]StatusInfo
	ChartVersions   map[manifest.NamedResource]string
	Causes          map[manifest.NamedResource][]manifest.NamedResource
	Unavailable     map[manifest.NamedResource]StatusInfo
	Unrelated       []manifest.Warning
	External        []manifest.Warning
	BlockedCount    int
}

// ProjectionErrors keeps selected failures separate from the remaining invocation errors.
type ProjectionErrors struct {
	Selected  error
	Remaining error
}
