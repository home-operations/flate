package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOutcome_InvalidRecords(t *testing.T) {
	t.Parallel()
	code := outcomeCause{Kind: "code", Code: errcode.ErrorCodeBlobUnknown}
	codes := outcomeCause{Kind: "codes", Children: []outcomeCause{code}}
	invalidCauses := map[string]outcomeCause{
		"status client":   {Kind: "status", Status: 404},
		"status terminal": {Kind: "status", Status: 600},
		"status children": {
			Kind:     "status",
			Status:   503,
			Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}},
		},
		"status unknown child":     {Kind: "status", Status: 503, Children: []outcomeCause{{Kind: "unknown"}}},
		"status unknown operation": {Kind: "status", Status: 503, Op: "fixture-secret"},
		"raw response":             {Kind: "response", Status: 503},
		"raw codes":                codes,
		"leaf children":            {Kind: "reset", Children: []outcomeCause{{Kind: "flux"}}},
		"leaf network":             {Kind: "reset", Network: "tcp"},
		"leaf status":              {Kind: "reset", Status: 503},
		"leaf code":                {Kind: "reset", Code: errcode.ErrorCodeBlobUnknown},
		"leaf temporary":           {Kind: "reset", Temporary: true},
		"leaf not found":           {Kind: "reset", NotFound: true},
		"machine missing child":    {Kind: "machine"},
		"machine extra child":      {Kind: "machine", Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"machine operation":        {Kind: "machine", Op: "read", Children: []outcomeCause{{Kind: "reset"}}},
		"machine network":          {Kind: "machine", Network: "tcp", Children: []outcomeCause{{Kind: "reset"}}},
		"machine status":           {Kind: "machine", Status: 503, Children: []outcomeCause{{Kind: "reset"}}},
		"machine code": {
			Kind:     "machine",
			Code:     errcode.ErrorCodeBlobUnknown,
			Children: []outcomeCause{{Kind: "reset"}},
		},
		"machine temporary":     {Kind: "machine", Temporary: true, Children: []outcomeCause{{Kind: "reset"}}},
		"machine not found":     {Kind: "machine", NotFound: true, Children: []outcomeCause{{Kind: "reset"}}},
		"empty join":            {Kind: "join"},
		"wrap children":         {Kind: "wrap", Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"DNS children":          {Kind: "dns", Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"net unknown network":   {Kind: "net", Network: "fixture-secret", Children: []outcomeCause{{Kind: "reset"}}},
		"URL unknown operation": {Kind: "url", Op: "fixture-secret", Children: []outcomeCause{{Kind: "reset"}}},
		"sibling transport EOF": {
			Kind:     "join",
			Children: []outcomeCause{{Kind: "url", Children: []outcomeCause{{Kind: "reset"}}}, {Kind: "eof"}},
		},
	}
	for name, cause := range map[string]outcomeCause{
		"response not codes":     {Kind: "response", Status: 503, Children: []outcomeCause{code}},
		"response wrapped codes": {Kind: "response", Status: 503, Children: []outcomeCause{{Kind: "wrap", Children: []outcomeCause{codes}}}},
		"response children":      {Kind: "response", Status: 503, Children: []outcomeCause{codes, codes}},
		"codes pointer":          {Kind: "codes", Children: []outcomeCause{{Kind: "code-pointer", Code: errcode.ErrorCodeBlobUnknown}}},
		"codes wrapped value":    {Kind: "codes", Children: []outcomeCause{{Kind: "wrap", Children: []outcomeCause{code}}}},
		"codes noncode":          {Kind: "codes", Children: []outcomeCause{{Kind: "reset"}}},
		"unknown code":           {Kind: "code", Code: "fixture-secret"},
		"code children":          {Kind: "code", Code: errcode.ErrorCodeBlobUnknown, Children: []outcomeCause{{Kind: "flux"}}},
	} {
		invalidCauses[name] = outcomeCause{Kind: "status", Status: 503, Children: []outcomeCause{cause}}
	}
	scenarios := []string{
		"version",
		"count",
		"completion",
		"unknown field",
		"unknown cause field",
		"trailing JSON",
		"unknown cause",
		"cause field",
		"too deep",
		"bare EOF",
		"no terminal",
		"no remote",
		"format",
		"schema",
		"media",
		"two layers",
		"subject",
		"config media",
		"layer is outcome",
	}
	for _, scenario := range append(scenarios, slices.Sorted(maps.Keys(invalidCauses))...) {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testOutcomeInvalidRecordsCase(
				t,
				invalidCauses, &scenario,
			)
		},
		)
	}
}

func testOutcomeInvalidRecordsCase(t *testing.T, invalidCauses map[string]outcomeCause,
	scenario *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &ocispec.Descriptor{})
	blobs, _ := o.encode()

	var m ocispec.Manifest
	{
		err := json.Unmarshal(blobs.manifest, &m)
		assert.NoError(t, err)
	}

	if cause, ok := invalidCauses[(*scenario)]; ok {
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
		m.Layers = []ocispec.Descriptor{content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, blobs.manifest)}
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
	blobs.manifest, err = json.Marshal(m)
	assert.NoError(t, err)

	desc := pushOutcomeFixture(t, c, config, blobs.manifest)
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
		&Policy{MaxAge: Forever},
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
