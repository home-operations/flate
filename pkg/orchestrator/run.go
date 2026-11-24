package orchestrator

import (
	"context"

	"github.com/home-operations/flate/internal/engine/helm"
	engine "github.com/home-operations/flate/internal/engine/orchestrator"
	"github.com/home-operations/flate/internal/engine/selection"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/ssrfguard"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// RenderTrees requires Close for every non-nil run, including returns with an error.
func RenderTrees(
	ctx context.Context,
	trees []Tree,
	cfg Config, //nolint:gocritic // The public call owns its configuration value.
) (*Run, error) {
	inputs := make([]engine.Tree, len(trees))
	for i, tree := range trees {
		inputs[i] = engine.Tree{ID: tree.ID, RepoRoot: tree.RepoRoot, Path: tree.Path, SelfURLs: tree.SelfURLs}
	}
	if err := engine.ValidateTrees(inputs); err != nil {
		return nil, err
	}
	supply := make([]store.Declaration, len(cfg.Supply))
	for i, doc := range cfg.Supply {
		supply[i] = store.Declaration{Doc: doc.Doc, InputPath: doc.InputPath, Ordinal: doc.Ordinal}
	}
	internal := engine.Config{
		RecordWork: cfg.RecordWork,
		Selection: selection.Request{
			Roots:   cfg.Selection.Roots,
			Changed: cfg.Selection.Changed,
			Labels:  cfg.Selection.Labels,
		},
		HelmOptions:         helm.Options(cfg.HelmOptions),
		AllowMissingKinds:   cfg.AllowMissingKinds,
		Supply:              supply,
		StrictSubstitutions: cfg.StrictSubstitutions,
		RestrictEgress:      cfg.RestrictEgress,
		RegistryConfig:      cfg.RegistryConfig,
		CacheDir:            cfg.CacheDir,
		Concurrency:         cfg.Concurrency,
		HeadOnly:            cfg.HeadOnly,
		SourceRetry:         sourcecache.RetryConfig(cfg.SourceRetry),
		GitDepth:            cfg.GitDepth,
	}
	if listener := cfg.StatusListener; listener != nil {
		internal.StatusListener = func(tree manifest.TreeID, subject manifest.DiagnosticIdentity, info store.StatusInfo) {
			listener(tree, subject, statusInfo(info))
		}
	}
	ssrfguard.Restrict(cfg.RestrictEgress)
	invocation, err := engine.RenderTrees(ctx, inputs, internal)
	if invocation == nil {
		return nil, err
	}
	run := &Run{invocation: invocation, Results: make([]Rendered, len(invocation.Results))}
	for i, result := range invocation.Results {
		var report *Result
		if output := result.Result; output != nil {
			report = &Result{
				Work:            output.Work,
				Manifests:       output.Manifests,
				Failed:          statuses(output.Failed),
				Orphans:         output.Orphans,
				Blocked:         output.Blocked,
				Warnings:        output.Warnings,
				ClusterWarnings: output.ClusterWarnings,
			}
		}
		run.Results[i] = Rendered{
			ID: result.ID, Absent: result.Absent,
			Complete: result.Orchestrator != nil && result.Result != nil,
			Result:   report, Err: result.Err,
		}
	}
	return run, err
}

// Select MUST receive one owned head or an ordered base/head pair from one cluster.
func (r *Run) Select(
	ids []manifest.TreeID,
	roots []manifest.NamedResource,
	labels map[string]string,
) ([]*Selection, ProjectionErrors) {
	results, projection := r.invocation.Select(ids, roots, labels)
	if results == nil {
		return nil, ProjectionErrors{Selected: projection.Selected, Remaining: projection.Remaining}
	}
	selected := make([]*Selection, len(results))
	for i, result := range results {
		selected[i] = &Selection{
			Subjects: result.Subjects, Manifests: result.Manifests,
			Failed: statuses(result.Failed), Blocked: result.Blocked,
			Warnings: result.Warnings, ClusterWarnings: result.ClusterWarnings,
			Objects: result.Objects, Counts: result.Counts, Statuses: statuses(result.Statuses),
			ChartVersions: result.ChartVersions, Causes: result.Causes,
			Unavailable: statuses(result.Unavailable),
			Unrelated:   result.Unrelated, External: result.External, BlockedCount: result.BlockedCount(),
		}
	}
	return selected, ProjectionErrors{Selected: projection.Selected, Remaining: projection.Remaining}
}

// Close MUST follow all consumers. The caller serializes closure with rendering and selection.
func (r *Run) Close() error {
	if r == nil {
		return nil
	}
	return r.invocation.Close()
}

func statuses(values map[manifest.NamedResource]store.StatusInfo) map[manifest.NamedResource]StatusInfo {
	if values == nil {
		return nil
	}
	converted := make(map[manifest.NamedResource]StatusInfo, len(values))
	for id, info := range values {
		converted[id] = statusInfo(info)
	}
	return converted
}

func statusInfo(info store.StatusInfo) StatusInfo {
	return StatusInfo{
		Status: Status(
			info.Status,
		),
		Reason:  StatusReason(info.Reason),
		Message: info.String(),
		Outcome: Outcome(info.Outcome()),
	}
}
