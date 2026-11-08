package loader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:revive // The fixture binds a resource, physical capture and optional logical roots in one call.
func promoteForTest(
	ctx context.Context,
	tb testing.TB,
	st *store.Store,
	id manifest.NamedResource,
	options PromoteOptions, //nolint:gocritic // The fixture owns an independent acquisition snapshot.
	roots ...manifest.NamedResource,
) (bool, error) {
	tb.Helper()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(roots) == 0 {
		if existing, policy := st.Lookup(id); existing != nil && policy.Root {
			return true, nil
		}
	}
	reads := promotionReadsForTest(tb, st, &options)
	children, err := Promote(ctx, st, id, options)
	if err != nil {
		return false, err
	}
	if len(roots) != 0 {
		l := New(st)
		configureSelection(tb, l, selection.Request{Roots: roots})
		proposal, err := prepareSelection(
			ctx,
			tb,
			l,
			selection.PrepareRequest{Generation: children, Reads: reads, Invocation: true},
		)
		if err != nil {
			return false, err
		}
		_, committed, err := l.commitSelection(ctx, tb, proposal, selection.Execution)
		return committed && l.Options.Session.Snapshot().Reconcile(id) && st.GetObject(id) != nil, err
	}
	for i := range children {
		child := children[i]
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !reads.Valid() {
			return false, nil
		}
		for j := range child.Warnings {
			warning := child.Warnings[j]
			warning.Resource = child.Obj.Named()
			reads.AddWarning(warning)
		}
		reads.Admit(child, child.SourceSupport, options.Record.Owner)
	}
	return reads.Valid() && st.GetObject(id) != nil, nil
}

func promotionReadsForTest(tb testing.TB, st *store.Store, options *PromoteOptions) *store.ReadSet {
	tb.Helper()
	if options.Reads == nil && options.Record.Source == (manifest.NamedResource{}) &&
		st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) == nil {
		input := &store.SourceArtifact{LocalPath: tb.TempDir(), FS: fstest.MapFS{}}
		if options.Record.Path != "" {
			input.LocalPath = filepath.Dir(options.Record.Path)
			input.FS = os.DirFS(input.LocalPath)
		}
		st.SetArtifact(manifest.BootstrapSourceID, input)
	}
	reads := options.Reads
	if reads == nil {
		reads = new(store.ReadSet)
		reads.Init(st, manifest.NamedResource{}, nil)
		tb.Cleanup(reads.Retire)
		options.Reads = reads
	}
	return reads
}
