package helmchart

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetchIndex_Revalidation(t *testing.T) {
	t.Parallel()
	for _, name := range helmValidatorCases() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newHelmRevalidationFixture(t)
			if name == "redirect" {
				reply := *fixture.alternate.reply.Load()
				reply.body = fixture.index.reply.Load().body
				fixture.alternate.reply.Store(&reply)
			}
			testHelmValidators(t, fixture, &fixture.index, func() ([]byte, error) { return fixture.fetchIndex(t) })
		})
	}
	t.Run("disk reopen", func(t *testing.T) {
		t.Parallel()
		fixture := newHelmRevalidationFixture(t)
		first, err := fixture.fetchIndex(t)
		assert.NoError(t, err)
		fixture.reopen(t)
		second, err := fixture.fetchIndex(t)
		assert.NoError(t, err)
		assert.Equal(t, string(second), string(first))
		assert.Equal(t, fixture.index.requests.Load(), int32(2))
		assert.Equal(t, fixture.index.bodies.Load(), int32(1))
		assert.Equal(t, fixture.index.unchanged.Load(), int32(1))
	})
	t.Run("invalid changed index", func(t *testing.T) {
		t.Parallel()
		fixture := newHelmRevalidationFixture(t)
		first, err := fixture.fetchIndex(t)
		assert.NoError(t, err)
		original := fixture.index.reply.Load()
		fixture.index.reply.Store(&revalidationReply{body: []byte("["), etag: `"invalid"`})
		_, err = fixture.fetchIndex(t)
		assert.FatalIf(t, err == nil, "invalid changed index published")
		fixture.index.reply.Store(original)
		fixture.reopen(t)
		retained, err := fixture.fetchIndex(t)
		assert.NoError(t, err)
		assert.Equal(t, string(retained), string(first))
		assert.Equal(t, fixture.index.last.Load().etag, original.etag)
		assert.Equal(t, fixture.index.bodies.Load(), int32(2))
		assert.Equal(t, fixture.index.unchanged.Load(), int32(1))
	})
	t.Run("changed version", func(t *testing.T) {
		t.Parallel()
		fixture := newHelmRevalidationFixture(t)
		_, err := fixture.fetchIndex(t)
		assert.NoError(t, err)
		reply := *fixture.index.reply.Load()
		reply.body = []byte(strings.ReplaceAll(string(reply.body), "1.0.0", "2.0.0"))
		reply.etag = `"new-index"`
		fixture.index.reply.Store(&reply)
		index, err := fixture.fetcher.fetchIndex(t.Context(), fixture.repository, &sourcecache.Policy{})
		assert.NoError(t, err)
		selected, err := index.Get("app-template", "*")
		assert.NoError(t, err)
		assert.Equal(t, selected.Version, "2.0.0")
		assert.Equal(t, fixture.index.bodies.Load(), int32(2))
		assert.Equal(t, fixture.index.validated.Load(), int32(1))
	})
}

func TestFetchHTTPChart_Revalidation(t *testing.T) {
	t.Parallel()
	for _, name := range helmValidatorCases() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newHelmRevalidationFixture(t)
			testHelmValidators(t, fixture, &fixture.chart, func() ([]byte, error) {
				artifact, err := fixture.fetchChart(t)
				if err != nil {
					return nil, err
				}
				assert.Equal(t, artifact.URL, fixture.repository.URL+"/app-template-1.0.0.tgz")
				assert.Equal(t, artifact.Revision, "1.0.0")
				body, err := fs.ReadFile(artifact.FS, "chart.tgz")
				assert.NoError(t, err)
				assert.Equal(t, artifact.Digest, chartDigest(body))
				assert.Equal(t, artifact.Size, int64(len(body)))
				assert.Equal(t, artifact.Content, sourcecache.AcquisitionIdentity(artifact.FS))
				return body, nil
			})
		})
	}
	for _, name := range []string{"mutable chart", "missing digest", "changed URL", "changed version", "changed expected digest", "digest mismatch"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testHelmChartSelectionRevalidation(t, name)
		})
	}
	t.Run("shared consumers", testHelmRevalidationWork)
}

func testHelmChartSelectionRevalidation(t *testing.T, name string) {
	t.Helper()
	fixture := newHelmRevalidationFixture(t)
	index := *fixture.index.reply.Load()
	if name == "mutable chart" || name == "missing digest" {
		index.body = []byte(helmRepoIndex(""))
		fixture.index.reply.Store(&index)
	}
	first, err := fixture.fetchChart(t)
	assert.NoError(t, err)
	original, err := fs.ReadFile(first.FS, "chart.tgz")
	assert.NoError(t, err)
	changed := buildChartTarGz(t, "app-template", "2.0.0")
	want, endpoint := original, &fixture.chart
	switch name {
	case "mutable chart", "digest mismatch":
		fixture.chart.reply.Store(&revalidationReply{body: changed, etag: `"new-chart"`})
		want = changed
	case "changed URL", "changed version":
		index.body = []byte(
			strings.ReplaceAll(helmRepoIndex(chartDigest(changed)), "app-template-1.0.0.tgz", "alternate.tgz"),
		)
		if name == "changed version" {
			index.body = []byte(strings.ReplaceAll(string(index.body), "version: 1.0.0", "version: 2.0.0"))
		}
		index.etag = `"new-index"`
		fixture.index.reply.Store(&index)
		fixture.alternate.reply.Store(&revalidationReply{body: changed, etag: `"chart"`})
		want, endpoint = changed, &fixture.alternate
	case "changed expected digest":
		index.body, index.etag = []byte(helmRepoIndex(chartDigest(changed))), `"new-index"`
		fixture.index.reply.Store(&index)
	}
	second, err := fixture.fetchChart(t)
	if name == "changed expected digest" || name == "digest mismatch" {
		assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), "digest mismatch"), "digest error = %v", err)
		if name == "changed expected digest" {
			assert.Equal(t, fixture.chart.bodies.Load(), int32(1))
			assert.Equal(t, fixture.chart.unchanged.Load(), int32(1))
		} else {
			assert.Equal(t, fixture.chart.bodies.Load(), int32(2))
			assert.Equal(t, fixture.index.unchanged.Load(), int32(1))
		}
		return
	}
	assert.NoError(t, err)
	body, err := fs.ReadFile(second.FS, "chart.tgz")
	assert.NoError(t, err)
	assert.Equal(t, string(body), string(want))
	assert.Equal(t, second.Key, first.Key)
	assert.Equal(t, second.Digest, chartDigest(want))
	assert.Equal(t, second.Content == first.Content, name == "missing digest")
	if endpoint == &fixture.alternate {
		assert.Equal(t, *endpoint.last.Load(), revalidationRequest{})
		assert.Equal(t, second.URL, fixture.repository.URL+"/alternate.tgz")
		assert.Equal(t, fixture.chart.requests.Load(), int32(1))
	} else {
		assert.Equal(t, fixture.index.unchanged.Load(), int32(1))
		assert.Equal(t, fixture.chart.validated.Load(), int32(1))
	}
	version := "1.0.0"
	if name == "changed version" {
		version = "2.0.0"
	}
	assert.Equal(t, second.Revision, version)
}
