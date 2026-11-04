package cache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOutcome_ReadBoundsAndIntegrity(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"exact bounds", "config one over", "manifest one over", "negative size", "missing config", "missing manifest", "config digest", "manifest digest", "config short", "config trailing", "missing layer"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testOutcomeReadBoundsAndIntegrityCase(t, &scenario)
		},
		)
	}
}

func TestOutcome_WriteBounds(t *testing.T) {
	t.Parallel()
	for _, size := range []int{outcomeConfigLimit, outcomeConfigLimit + 1} {
		t.Run(fmt.Sprintf("record/%d", size), func(t *testing.T) {
			t.Parallel()
			testOutcomeWriteBoundsCase(t, &size)
		},
		)
	}
}

func testOutcomeReadBoundsAndIntegrityCase(t *testing.T, scenario *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &ocispec.Descriptor{})
	config, data, _ := o.encode()
	var m ocispec.Manifest
	{
		err := json.Unmarshal(data, &m)
		assert.NoError(t, err)
	}

	config = append(config, bytes.Repeat([]byte(" "), outcomeConfigLimit-len(config))...)
	if (*scenario) == "config one over" {
		config = append(config, ' ')
	}
	m.Config = content.NewDescriptorFromBytes(outcomeConfig, config)
	if (*scenario) == "missing layer" {
		m.Layers = []ocispec.Descriptor{
			content.NewDescriptorFromBytes("application/octet-stream", []byte("absent")),
		}
	}
	data, err := json.Marshal(m)
	assert.NoError(t, err)

	data = append(data, bytes.Repeat([]byte(" "), outcomeManifestLimit-len(data))...)
	if (*scenario) == "manifest one over" {
		data = append(data, ' ')
	}
	desc := pushOutcomeFixture(t, c, config, data)
	path := blobPath(m.Config.Digest)
	{
		chmodErr := c.root.Chmod(path, 0o600)
		assert.NoError(t, chmodErr)
	}
	{
		chmodErr2 := c.root.Chmod(blobPath(desc.Digest), 0o600)
		assert.NoError(t, chmodErr2)
	}

	switch *scenario {
	case "negative size":
		desc.Size = -1
	case "missing manifest":
		path = blobPath(desc.Digest)
		fallthrough
	case "missing config":
		{
			removeErr := c.root.Remove(path)
			assert.NoError(t, removeErr)
		}

	case "config digest":
		{
			writeFileErr := c.root.WriteFile(
				path,
				bytes.Repeat([]byte("x"), len(config)),
				0o600,
			)
			assert.NoError(t, writeFileErr)
		}

	case "manifest digest":
		{
			writeFileErr2 := c.root.WriteFile(
				blobPath(desc.Digest),
				bytes.Repeat([]byte("x"), len(data)),
				0o600,
			)
			assert.NoError(t, writeFileErr2)
		}

	case "config short":
		{
			writeFileErr3 := c.root.WriteFile(path, config[:len(config)-1], 0o600)
			assert.NoError(t, writeFileErr3)
		}

	case "config trailing":
		{
			writeFileErr4 := c.root.WriteFile(path, append(config, 'x'), 0o600)
			assert.NoError(t, writeFileErr4)
		}
	}
	got, err := c.readOutcome(t.Context(), &desc)
	switch *scenario {
	case "exact bounds":
		assert.FatalIf(t, err != nil || got == nil, "exact caps rejected: %+v, %v", got, err)
	case "config one over", "manifest one over", "negative size":
		assert.FatalIf(t, err != nil || got != nil, "out-of-bounds read: %+v, %v", got, err)
	default:
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(
				t,
				!ok || got != nil || !errors.Is(err, manifest.ErrFlux),
				"integrity failure lost: %+v, %v",
				got,
				err,
			)
		}
	}
}

func testOutcomeWriteBoundsCase(t *testing.T, size *int,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	o := &outcome{
		record: outcomeRecord{
			Version:   1,
			Completed: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			Count:     1,
			Cause:     outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "join"}}},
		},
	}
	for range 6 {
		child := outcomeCause{Kind: "reset"}
		for range 12 {
			child = outcomeCause{Kind: "net", Op: "recvfrom", Network: "tcp6", Children: []outcomeCause{child}}
		}
		o.record.Cause.Children[0].Children = append(o.record.Cause.Children[0].Children, child)
	}
	encodedSize := func() int {
		data, err := json.Marshal(o.record)
		assert.NoError(t, err)

		return len(data)
	}
	var trim func(*outcomeCause)
	trim = func(n *outcomeCause) {
		testOutcomeWriteBoundsCaseCallback(
			n,
			testOutcomeWriteBoundsCaseCallbackInputs{size: &size, encodedSize: &encodedSize, trim: &trim},
		)
	}

	trim(&o.record.Cause)
	if encodedSize() != (*size) {
		t.Fatalf("fixture size = %d, want %d", encodedSize(), (*size))
	}
	before := sweepSnapshot(t, filepath.Join(c.root.Name(), blobDir()))
	desc, err := c.publishOutcome(t.Context(), "bounded", o, connReset())
	assert.NoError(t, err)

	if (*size) == outcomeConfigLimit {
		assert.FatalIf(t, desc.Digest == "", "%s", "exact config rejected")
	} else {
		assert.FatalIf(t, desc.Digest != "", "%s", "oversize config published")

		{
			diff := cmp.Diff(before, sweepSnapshot(t, filepath.Join(c.root.Name(), blobDir())))
			assert.FatalIf(
				t,
				diff != "",
				"partial outcome written before bounds checked (-want +got):\n%s",
				diff,
			)
		}
	}
}

type testOutcomeWriteBoundsCaseCallbackInputs struct {
	size        **int
	encodedSize *func() int
	trim        *func(*outcomeCause)
}

func testOutcomeWriteBoundsCaseCallback(n *outcomeCause, testCaseInput testOutcomeWriteBoundsCaseCallbackInputs) {
	if n.Kind == "net" && (*testCaseInput.encodedSize)()-(*(*testCaseInput.size)) >= len(`,"op":"recvfrom"`) {
		n.Op = ""
	}
	if n.Kind == "net" && (*testCaseInput.encodedSize)() > (*(*testCaseInput.size)) {
		n.Network = "tcp"
	}
	for i := range n.Children {
		(*testCaseInput.trim)(&n.Children[i])
	}
}
