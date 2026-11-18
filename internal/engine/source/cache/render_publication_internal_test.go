package cache

import (
	"context"
	"errors"
	"io/fs"
	"iter"
	"maps"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

type publicationReceipt struct {
	projection RenderPublication
	calls      *int
	err        error
}

//nolint:gocritic // The fixture implements the concrete value-receipt contract.
func (p publicationReceipt) RenderPublication(context.Context, *RenderAcceptance) (RenderPublication, error) {
	if p.calls != nil {
		*p.calls++
	}
	return p.projection, p.err
}

func newRenderPublication(t *testing.T) (*Cache, RenderAcceptance, publicationReceipt) {
	t.Helper()
	c := cachetest.New(t, t.TempDir(), New)
	if !c.CanPersistRenders() {
		t.Skip("accepted render proof requires a clean stamped build: " + RenderPersistenceDisabledReason())
	}
	owner := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "owner"}
	child := manifest.NamedResource{Kind: "ConfigMap", Name: "child"}
	sources := []manifest.SourceEvidence{{Source: manifest.SourceIdentity{Root: "/source"}, Path: "old.yaml"}}
	p := publicationReceipt{projection: RenderPublication{
		Frame:  RenderFrame{Key: "render", Binding: "binding", Output: []byte("output")},
		Domain: "test-render", Metadata: "metadata", Content: "final", Sources: sources,
	}}
	a := RenderAcceptance{
		Owner: owner, Receipt: manifest.NewRenderReceipt(p, nil), Sources: sources, Content: "final",
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{child: {Path: "old.yaml"}}),
	}
	return c, a, p
}

func assertRenderPublication(t *testing.T, c *Cache, acceptance *RenderAcceptance) []byte {
	t.Helper()
	generation, _, _, err := c.Blob(
		t.Context(),
		BlobKey{Content: "render", Render: RenderAccepted},
		&Policy{MaxAge: Forever},
		nil,
	)
	assert.NoError(t, err)
	var metadata string
	frame, err := OpenRenderFrame(t.Context(), "test-render", generation, &metadata)
	assert.NoError(t, err)
	records, complete := frame.Stored.History(acceptance.Owner, acceptance.Content)
	assert.Equal(t, complete, true)
	assert.Diff(t, maps.Collect(records), maps.Collect(acceptance.Records))
	owners, err := c.ProducersForPaths(t.Context(), acceptance.Sources)
	assert.NoError(t, err)
	assert.Diff(t, owners[acceptance.Owner], acceptance.Sources)
	data, err := fs.ReadFile(generation, "content")
	assert.NoError(t, err)
	return data
}

func TestRecordRender_Completeness(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"receipt", "records", "sources"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			c, initial, p := newRenderPublication(t)
			assert.NoError(t, c.RecordRender(t.Context(), initial))
			before := assertRenderPublication(t, c, &initial)
			candidate := initial
			calls := 0
			p.calls = &calls
			candidate.Receipt = manifest.NewRenderReceipt(p, nil)
			switch field {
			case "receipt":
				candidate.Receipt = nil
			case "records":
				candidate.Records = nil
			case "sources":
				candidate.Sources = nil
			}
			assert.NoError(t, c.RecordRender(t.Context(), candidate))
			assert.Equal(t, calls, 0)
			after := assertRenderPublication(t, c, &initial)
			assert.Diff(t, after, before)
		})
	}
	t.Run("explicit empty", func(t *testing.T) {
		t.Parallel()
		c, initial, p := newRenderPublication(t)
		assert.NoError(t, c.RecordRender(t.Context(), initial))
		p.projection.Sources = []manifest.SourceEvidence{}
		p.projection.Content = "empty"
		initial.Records = maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{})
		initial.Sources = []manifest.SourceEvidence{}
		initial.Receipt = manifest.NewRenderReceipt(p, nil)
		assert.NoError(t, c.RecordRender(t.Context(), initial))
		generation, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "render", Render: RenderAccepted},
			&Policy{MaxAge: Forever},
			nil,
		)
		assert.NoError(t, err)
		var metadata string
		frame, err := OpenRenderFrame(t.Context(), "test-render", generation, &metadata)
		assert.NoError(t, err)
		records, complete := frame.Stored.History(initial.Owner, "empty")
		assert.Equal(t, complete, true)
		assert.Equal(t, len(maps.Collect(records)), 0)
		owners, err := c.ProducersForPaths(
			t.Context(),
			[]manifest.SourceEvidence{{Source: manifest.SourceIdentity{Root: "/source"}, Path: "old.yaml"}},
		)
		assert.NoError(t, err)
		assert.Equal(t, len(owners), 0)
	})
}

func TestRecordRender_ReadInvalidation(t *testing.T) {
	t.Parallel()
	for _, capture := range []string{"reads", "output reads"} {
		for _, phase := range []string{"before", "during traversal"} {
			t.Run(capture+"/"+phase, func(t *testing.T) {
				t.Parallel()
				c, initial, p := newRenderPublication(t)
				assert.NoError(t, c.RecordRender(t.Context(), initial))
				before := assertRenderPublication(t, c, &initial)
				st := store.New()
				input := &manifest.ConfigMap{Name: "input", Data: map[string]any{"value": "old"}}
				st.AddObject(input)
				var reads store.ReadSet
				reads.Init(st, initial.Owner, nil)
				t.Cleanup(reads.Retire)
				reads.GetObject(input.Named())
				candidate := initial
				if capture == "reads" {
					candidate.Reads = &reads
				} else {
					candidate.OutputReads = &reads
				}
				p.projection.Content = "changed"
				p.projection.Sources = []manifest.SourceEvidence{{Source: initial.Sources[0].Source, Path: "new.yaml"}}
				candidate.Sources = p.projection.Sources
				calls, traversals := 0, 0
				p.calls = &calls
				candidate.Receipt = manifest.NewRenderReceipt(p, nil)
				invalidate := func() {
					st.AddObject(&manifest.ConfigMap{Name: "input", Data: map[string]any{"value": "new"}})
				}
				candidate.Records = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					traversals++
					requested, done := make(chan struct{}), make(chan struct{})
					go func() {
						<-requested
						invalidate()
						close(done)
					}()
					close(requested)
					<-done
					yield(
						manifest.NamedResource{Kind: "ConfigMap", Name: "new-child"},
						manifest.DiscoveryRecord{Path: "new.yaml"},
					)
				}
				if phase == "before" {
					invalidate()
				}
				assert.NoError(t, c.RecordRender(t.Context(), candidate))
				want := 1
				if phase == "before" {
					want = 0
				}
				assert.Equal(t, calls, want)
				assert.Equal(t, traversals, want)
				assert.Equal(t, reads.Valid(), false)
				after := assertRenderPublication(t, c, &initial)
				assert.Diff(t, after, before)
				owners, err := c.ProducersForPaths(t.Context(), candidate.Sources)
				assert.NoError(t, err)
				assert.Equal(t, len(owners), 0)
			})
		}
	}
}

func TestRecordRender_Publication(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"replacement", "unsupported receipt", "nil content", "nil projected sources", "unrepresentable content", "oversized metadata", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			c, initial, p := newRenderPublication(t)
			assert.NoError(t, c.RecordRender(t.Context(), initial))
			before := assertRenderPublication(t, c, &initial)
			candidate := initial
			candidate.Content = "changed"
			p.projection.Content = "changed"
			candidate.Records = maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{})
			ctx := t.Context()
			switch kind {
			case "nil content":
				p.projection.Content = nil
			case "nil projected sources":
				p.projection.Sources = nil
			case "unrepresentable content":
				p.projection.Content = make(chan int)
			case "oversized metadata":
				p.projection.Metadata = strings.Repeat("x", frameHeaderLimit)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			candidate.Receipt = manifest.NewRenderReceipt(p, nil)
			if kind == "unsupported receipt" {
				candidate.Receipt = manifest.NewRenderReceipt("unsupported", nil)
			}
			err := c.RecordRender(ctx, candidate)
			switch kind {
			case "unsupported receipt":
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			case "canceled":
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			default:
				assert.NoError(t, err)
			}
			if kind == "replacement" {
				after := assertRenderPublication(t, c, &candidate)
				assert.Equal(t, string(after) == string(before), false)
			} else {
				after := assertRenderPublication(t, c, &initial)
				assert.Diff(t, after, before)
			}
		})
	}
	t.Run("disabled persistence", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, "", New)
		assert.Equal(t, c.CanPersistRenders(), false)
		calls, traversals := 0, 0
		p := publicationReceipt{calls: &calls, projection: RenderPublication{
			Frame: RenderFrame{
				Key: "render",
			},
			Domain:   "domain",
			Metadata: make(chan int),
			Content:  "final",
			Sources:  []manifest.SourceEvidence{},
		}}
		candidate := RenderAcceptance{
			Receipt: manifest.NewRenderReceipt(p, nil),
			Sources: []manifest.SourceEvidence{},
			Records: iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord](
				func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) { traversals++ },
			),
		}
		assert.NoError(t, c.RecordRender(t.Context(), candidate))
		assert.Equal(t, calls, 1)
		assert.Equal(t, traversals, 0)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.Equal(t, errors.Is(c.RecordRender(ctx, candidate), context.Canceled), true)
		candidate.Receipt = manifest.NewRenderReceipt("unsupported", nil)
		assert.Equal(t, errors.Is(c.RecordRender(t.Context(), candidate), manifest.ErrFlux), true)
		p.err = manifest.NewError(manifest.ErrFlux, nil, "malformed engine frame")
		candidate.Receipt = manifest.NewRenderReceipt(p, nil)
		assert.Equal(t, errors.Is(c.RecordRender(t.Context(), candidate), manifest.ErrFlux), true)
	})
}
