package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

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

func testOutcomeInvalidRecordsCase(t *testing.T, invalidCauses *map[string]outcomeCause,
	scenario *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &ocispec.Descriptor{})
	_, data, _ := o.encode()
	var m ocispec.Manifest
	{
		err := json.Unmarshal(data, &m)
		assert.NoError(t, err)
	}

	if cause, ok := (*invalidCauses)[(*scenario)]; ok {
		o.record.Cause = outcomeCause{
			Kind:     "terminal",
			Children: []outcomeCause{{Kind: "join", Children: []outcomeCause{{Kind: "reset"}, cause}}},
		}
	}
	switch *scenario {
	case "version":
		o.record.Version++
	case "count":
		o.record.Count = 0
	case "completion":
		o.record.Completed = time.Time{}
	case "unknown cause":
		o.record.Cause.Kind = "fixture-secret"
	case "cause field":
		o.record.Cause.Op = "fixture-secret"
	case "too deep":
		for range outcomeDepthLimit {
			o.record.Cause = outcomeCause{Kind: "wrap", Children: []outcomeCause{o.record.Cause}}
		}
	case "bare EOF":
		o.record.Cause = outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "eof"}}}
	case "no terminal":
		o.record.Cause = outcomeCause{Kind: "reset"}
	case "no remote":
		o.record.Cause = outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "flux"}}}
	case "format":
		m.ArtifactType = "unknown"
	case "schema":
		m.SchemaVersion = 1
	case "media":
		m.MediaType = "unknown"
	case "two layers":
		m.Layers = []ocispec.Descriptor{{}, {}}
	case "subject":
		m.Subject = &ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Digest:    "sha256:abc",
			Size:      1,
		}
	case "layer is outcome":
		m.Layers = []ocispec.Descriptor{content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, data)}
	}
	config, err := json.Marshal(o.record)
	assert.NoError(t, err)

	if (*scenario) == "unknown field" {
		config = append([]byte(`{"unknown":"fixture-secret",`), config[1:]...)
	}
	if (*scenario) == "unknown cause field" {
		config = bytes.Replace(config, []byte(`"kind":`), []byte(`"unknown":"fixture-secret","kind":`), 1)
	}
	if (*scenario) == "trailing JSON" {
		config = append(config, []byte(`{}`)...)
	}
	m.Config = content.NewDescriptorFromBytes(outcomeConfig, config)
	if (*scenario) == "config media" {
		m.Config.MediaType = "unknown"
	}
	data, err = json.Marshal(m)
	assert.NoError(t, err)

	desc := pushOutcomeFixture(t, c, config, data)
	{
		got, readOutcomeErr := c.readOutcome(t.Context(), &desc)
		assert.FatalIf(
			t,
			got != nil || readOutcomeErr != nil,
			"invalid record accepted: %+v, %v",
			got,
			readOutcomeErr,
		)
	}
	id := manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"invalid"}})
	{
		tagErr := c.store.Tag(t.Context(), desc, id)
		assert.NoError(t, tagErr)
	}

	calls := 0
	_, _, _, err = c.Blob(
		t.Context(),
		BlobKey{Content: "invalid"},
		Policy{MaxAge: Forever},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			calls++
			return producer([]byte("replacement"), &ocispec.Descriptor{})(ctx, w)
		},
	)
	assert.FatalIf(
		t,
		err != nil || calls != 1,
		"invalid record suppressed acquisition: calls=%d, error=%v",
		calls,
		err,
	)
}

func testOutcomePublicationFailureCase(t *testing.T, scenario *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	live := outcomeFailure(connReset())
	o := newOutcome(live, time.Now().UTC(), 1, &ocispec.Descriptor{})
	switch *scenario {
	case "tag":
		index := filepath.Join(c.root.Name(), indexPath())
		{
			err := os.Remove(index)
			assert.NoError(t, err)
		}
		{
			err := os.Mkdir(index, 0o750)
			assert.NoError(t, err)
		}

	case "push":
		{
			err := c.root.RemoveAll("oci/blobs")
			assert.NoError(t, err)
		}
		{
			err := c.root.WriteFile("oci/blobs", []byte("blocked"), 0o600)
			assert.NoError(t, err)
		}

	case "existing config corrupt":
		config, data, _ := o.encode()
		pushOutcomeFixture(t, c, config, data)
		desc := content.NewDescriptorFromBytes(outcomeConfig, config)
		{
			err := c.root.Chmod(blobPath(desc.Digest), 0o600)
			assert.NoError(t, err)
		}
		{
			err := c.root.WriteFile(blobPath(desc.Digest), []byte("corrupt"), 0o600)
			assert.NoError(t, err)
		}
	}
	if _, err := c.publishOutcome(
		t.Context(),
		"failure",
		o,
		live,
	); err == nil || !errors.Is(err, syscall.ECONNRESET) ||
		!errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("publication lost live failure: %v", err)
	} else {
		_, ok := errors.AsType[*CacheError](
			err,
		)
		assert.FatalIf(t, !ok, "publication not CacheError: %v", err)
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

func testOutcomeAcquisitionValidityCase(t *testing.T, scenario *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testOutcomeAcquisitionValidityCaseCallback(
			t,
			testOutcomeAcquisitionValidityCaseCallbackInputs{scenario: &scenario},
		)
	},
	)
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

type testOutcomeAcquisitionValidityCaseCallbackInputs struct {
	scenario **string
}

func testOutcomeAcquisitionValidityCaseCallback(
	t *testing.T,
	testCaseInput testOutcomeAcquisitionValidityCaseCallbackInputs,
) {
	t.Helper()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &ocispec.Descriptor{})
	switch *(*testCaseInput.scenario) {
	case "future":
		o.record.Completed = time.Now().UTC().Add(time.Hour)
	case "rollback":
		o.observed = time.Now().UTC().Add(time.Second)
	case "saturation":
		o.record.Completed = time.Now().UTC().Add(-15 * time.Minute)
		o.record.Count = math.MaxUint32
	}
	id := manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"validity"}})
	desc, err := c.publishOutcome(t.Context(), id, o, o.replay())
	assert.NoError(t, err)

	switch *(*testCaseInput.scenario) {
	case "rollback":
		c.entries.Store(id, &entry{stored: desc, outcome: o})
	case "missing":
		{
			removeErr2 := c.root.Remove(blobPath(desc.Digest))
			assert.NoError(t, removeErr2)
		}

	case "corrupt":
		{
			chmodErr3 := c.root.Chmod(blobPath(desc.Digest), 0o600)
			assert.NoError(t, chmodErr3)
		}
		{
			writeFileErr5 := c.root.WriteFile(
				blobPath(desc.Digest),
				[]byte("invalid"),
				0o600,
			)
			assert.NoError(t, writeFileErr5)
		}
	}
	if (*(*testCaseInput.scenario)) == "future" || (*(*testCaseInput.scenario)) == "saturation" {
		{
			closeErr4 := c.Close()
			assert.NoError(t, closeErr4)
		}

		c = cachetest.New(t, root, New)
	}
	calls := 0
	_, _, _, err = c.Blob(
		t.Context(),
		BlobKey{Content: "validity"},
		Policy{MaxAge: Forever, Retry: fastRetry(2)},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			calls++
			return ProducerResult{}, connReset()
		},
	)
	if (*(*testCaseInput.scenario)) == "missing" || (*(*testCaseInput.scenario)) == "corrupt" {
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(
				t,
				!ok || calls != 0,
				"broken persistence concealed: calls=%d, error=%v",
				calls,
				err,
			)
		}
		return
	}
	wantCalls, wantCount := 2, uint32(1)
	if (*(*testCaseInput.scenario)) == "saturation" {
		wantCalls, wantCount = 1, math.MaxUint32
	}
	assert.FatalIf(
		t,
		calls != wantCalls || !errors.Is(err, syscall.ECONNRESET),
		"validity calls=%d, want=%d, error=%v",
		calls,
		wantCalls,
		err,
	)
	desc, err = c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	next, err := c.readOutcome(t.Context(), &desc)
	assert.FatalIf(t, err != nil || next == nil || next.record.Count != wantCount ||
		!next.record.Completed.Equal(time.Now()), "validity outcome=%+v, error=%v", next, err)
}
