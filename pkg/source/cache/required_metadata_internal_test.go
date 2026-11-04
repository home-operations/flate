package cache

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testDecodeRequiredMetadataCase(t *testing.T, family *struct {
	name     string
	required []string
	fields   map[string]string
},
) {
	t.Helper()

	base := map[string]string{
		"flate.resolved-at":         time.Now().UTC().Format(time.RFC3339Nano),
		"flate.upstream-size":       "12",
		"flate.upstream-media-type": "test/type",
	}
	maps.Copy(base, family.fields)
	checks := []struct{ name, key, value string }{
		{"valid", "", ""},
		{"bad stored digest", "", ""},
		{"bad resolved time", "flate.resolved-at", "invalid"},
		{"bad upstream size", "flate.upstream-size", "-1"},
		{"bad upstream digest", "flate.upstream-digest", "invalid"},
	}
	for _, field := range family.required {
		checks = append(checks, struct{ name, key, value string }{"missing " + field, prefix + field, ""})
	}
	if family.name == "bucket" {
		checks = append(checks, struct{ name, key, value string }{"negative count", "flate.object-count", "-1"},
			struct{ name, key, value string }{"malformed count", "flate.object-count", "invalid"})
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			t.Parallel()
			testDecodeRequiredMetadataCaseCase(
				t,
				testDecodeRequiredMetadataCaseCaseInputs{family: &family, base: &base, check: &check},
			)
		},
		)
	}
	for _, field := range family.required {
		t.Run("disk missing "+field, func(t *testing.T) {
			t.Parallel()
			testDecodeRequiredMetadataCaseCase2(
				t,
				testDecodeRequiredMetadataCaseCase2Inputs{family: &family, field: &field},
			)
		},
		)
	}
}

type testDecodeRequiredMetadataCaseCaseInputs struct {
	family **struct {
		name     string
		required []string
		fields   map[string]string
	}
	base  *map[string]string
	check *struct {
		name string
		key  string

		value string
	}
}

func testDecodeRequiredMetadataCaseCase(t *testing.T, testCaseInput testDecodeRequiredMetadataCaseCaseInputs) {
	t.Helper()

	a := maps.Clone((*testCaseInput.base))
	if testCaseInput.check.key != "" {
		a[testCaseInput.check.key] = testCaseInput.check.value
	}
	stored := ocispec.Descriptor{Digest: digest.FromString("stored"), Annotations: a}
	if testCaseInput.check.name == "bad stored digest" {
		stored.Digest = "invalid"
	}
	_, _, valid := decode(&stored, (*testCaseInput.family).required)
	assert.FatalIf(t, valid != (testCaseInput.check.name == "valid"), "valid=%v", valid)
}

type testDecodeRequiredMetadataCaseCase2Inputs struct {
	family **struct {
		name     string
		required []string
		fields   map[string]string
	}
	field *string
}

func testDecodeRequiredMetadataCaseCase2(t *testing.T, testCaseInput testDecodeRequiredMetadataCaseCase2Inputs) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{
		Content:  struct{ URL string }{"https://example.test/" + (*testCaseInput.family).name},
		Required: (*testCaseInput.family).required,
	}
	upstream := ocispec.Descriptor{
		Digest:      digest.Digest((*testCaseInput.family).fields[prefix+"upstream-digest"]),
		Annotations: (*testCaseInput.family).fields,
	}
	_, _, id, err := c.Blob(
		t.Context(),
		key,
		&Policy{MaxAge: Forever},
		producer([]byte("payload"), &upstream),
	)
	assert.NoError(t, err)

	stored, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	delete(stored.Annotations, prefix+(*testCaseInput.field))
	{
		tagErr := c.store.Tag(t.Context(), stored, id)
		assert.NoError(t, tagErr)
	}
	{
		closeErr4 := c.Close()
		assert.NoError(t, closeErr4)
	}

	c = cachetest.New(t, root, New)
	{
		r, _, failedID, blobErr3 := c.Blob(
			t.Context(),
			key,
			&Policy{MaxAge: Forever},
			nil,
		)
		assert.FatalIf(t, r != nil || failedID != "" ||
			!errors.Is(
				blobErr3,
				errdef.ErrNotFound,
			), "invalid restored metadata hit: id=%q, %v", failedID, blobErr3)
	}
	calls := 0
	r, _, restoredID, err := c.Blob(
		t.Context(),
		key,
		&Policy{MaxAge: Forever},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			calls++
			return producer([]byte("refilled"), &upstream)(ctx, w)
		},
	)
	assert.FatalIf(
		t,
		err != nil || calls != 1 || restoredID != id,
		"metadata refill: calls=%d id=%q, %v",
		calls,
		restoredID,
		err,
	)
	_, _ = fs.ReadFile(r, "content")
}
