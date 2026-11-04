package scope_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2ENamespaceOrphansScenario(t *testing.T, scenario *string) {
	t.Helper()

	namespaceOrphanFixtureResult1 := namespaceOrphanFixture(t, (*scenario))
	root, cache, namespaces := namespaceOrphanFixtureResult1.root, namespaceOrphanFixtureResult1.cache, namespaceOrphanFixtureResult1.namespaces
	flags := []string{
		"--path",
		filepath.Join(root, "flux"),
		"--cache-dir",
		cache,
		"--concurrency",
		"2",
		"--no-progress",
	}
	getArgs := append([]string{"get", "hr", "-o", "json"}, flags...)
	requireCLIOKResult1 := harness.RequireCLIOK(t, getArgs...)
	hr, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
	var releases []struct {
		Name      string `json:"Name"`
		Namespace string `json:"Namespace"`
		SourceRef struct {
			Name      string `json:"Name"`
			Namespace string `json:"Namespace"`
		} `json:"SourceRef"`
	}
	assert.NoError(t, json.Unmarshal([]byte(hr), &releases))
	var got []string
	for _, release := range releases {
		assert.ErrorIf(t, release.Name != "demo" || release.SourceRef.Name != release.Namespace+"-demo" ||
			release.SourceRef.Namespace != namespaceOrphanSourceNamespace(
				(*scenario),
				release.Namespace,
			), "unexpected release/source: %+v", release)
		if got == nil {
			got = make([]string, 0, len(releases))
		}
		got = append(got, release.Namespace)
	}
	assert.FatalIf(
		t,
		!slices.Equal(got, namespaces),
		"release namespaces = %v, want %v; full output: %s",
		got,
		namespaces,
		hr,
	)
	buildArgs := append([]string{"build", "all", "-o", "yaml"}, flags...)
	requireCLIOKResult2 := harness.RequireCLIOK(t, buildArgs...)
	build, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
	docs, err := testutil.SplitDocs([]byte(build))
	assert.NoError(t, err)
	var repos []string
	for _, doc := range docs {
		if doc["kind"] != manifest.KindHelmRepository {
			continue
		}
		metadata, _ := doc["metadata"].(map[string]any)
		assert.ErrorIf(t, metadata["name"] != "demo", "unexpected repository: %+v", metadata)
		ns, _ := metadata["namespace"].(string)
		repos = append(repos, ns)
	}
	slices.Sort(repos)
	wantRepos := namespaces
	if (*scenario) == "standalone" {
		counts := make(map[string]int)
		for _, doc := range docs {
			metadata2 := manifest.DocMetadata(doc)
			name, namespace := metadata2.Name, metadata2.Namespace
			kind := manifest.DocKind(doc)
			assert.FatalIf(
				t,
				namespace != "demo" || kind == manifest.KindConfigMap && name != "demo-chart",
				"standalone release lost its rendered namespace: %s",
				build,
			)
			counts[kind]++
		}
		assert.FatalIf(t, len(docs) != 3 || counts[manifest.KindHelmRelease] != 1 ||
			counts[manifest.KindHelmRepository] != 1 ||
			counts[manifest.KindConfigMap] != 1, "standalone root declarations or workload missing: %s", build)
	}
	assert.ErrorIf(
		t,
		(*scenario) != "shared" && !slices.Equal(repos, wantRepos),
		"repository namespaces = %v, want %v; full output: %s",
		repos,
		wantRepos,
		build,
	)
	requireCLIOKResult3 := harness.RequireCLIOK(t, append([]string{"test", "all"}, flags...)...)
	out, _ := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
	assert.ErrorIf(
		t,
		strings.Contains(out, "failed") || strings.Contains(out, "blocked"),
		"unexpected test result: %s",
		out,
	)
	wantPassed := 3 * len(namespaces)
	if (*scenario) == "shared" {
		wantPassed = 5
	}
	if (*scenario) == "standalone" {
		wantPassed = 2
	}
	assert.ErrorIf(
		t,
		!strings.Contains(out, fmt.Sprintf("%d passed", wantPassed)),
		"want %d real resources passed: %s",
		wantPassed,
		out,
	)
	if (*scenario) == "shared" {
		for _, namespace := range append(slices.Clone(namespaces), "absent") {
			e2ENamespaceOrphansScenarioLoop(t, struct {
				namespace *string
				flags     *[]string
			}{namespace: &namespace, flags: &flags})
		}
	}
}

func e2ENamespaceOrphansTerminalReplay(t *testing.T) {
	t.Helper()

	namespaceOrphanFixtureResult2 := namespaceOrphanFixture(t, "explicit")
	root, cache, _ := namespaceOrphanFixtureResult2.root, namespaceOrphanFixtureResult2.cache, namespaceOrphanFixtureResult2.namespaces
	flags := []string{
		"--path",
		filepath.Join(root, "flux"),
		"--cache-dir",
		cache,
		"--concurrency",
		"2",
		"--no-progress",
	}
	requireCLIOKResult4 := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	out, _ := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
	assert.FatalIf(t, !strings.Contains(out, "name: demo-chart"), "last-good chart did not render: %s", out)
	repository := &manifest.HelmRepository{URL: "https://charts.example.invalid"}
	chart := &manifest.HelmChartSource{Chart: "demo", Version: "0.1.0"}
	key := sourcecache.BlobKey{
		Content: struct {
			Repository any `json:"Repository"`
			Chart      any `json:"Chart"`
			Transport  any `json:"Transport"`
		}{repository.Content(), chart.Content(), nil},
		Required: []string{"chart-version", "chart-url"},
	}
	completed := time.Now()
	// CLI signal handlers MUST stay outside the bubble. Seed a capped
	// deadline at the real wall clock so CLI scheduling cannot expire it.
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(time.Until(completed.Add(-11 * 15 * time.Minute)))
		c, err := sourcecache.New(t.Context(), cache)
		assert.NoError(t, err)
		for attempt := range 12 {
			_, _, _, err = c.Blob(
				t.Context(),
				key,
				&sourcecache.Policy{},
				func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					return sourcecache.ProducerResult{}, &sourcecache.TerminalError{
						Err:       syscall.ECONNRESET,
						Attempted: true,
					}
				},
			)
			{
				terminal, ok := errors.AsType[*sourcecache.TerminalError](
					err,
				)
				assert.FatalIf(t, !ok || !terminal.Attempted ||
					!errors.Is(err, syscall.ECONNRESET), "seeded failure lost its classification: %v", err)
			}
			if attempt < 11 {
				time.Sleep(15 * time.Minute)
			}
		}
		assert.NoError(t, c.Close())
	})
	for _, command := range [][]string{{"build", "all"}, {"test", "all"}} {
		runCLIBuffersResult5 := harness.RunCLIBuffers(append(command, flags...)...)
		out, stderr, code := runCLIBuffersResult5.Stdout, runCLIBuffersResult5.Stderr, runCLIBuffersResult5.Code
		assert.FatalIf(t, code != 1 || !strings.Contains(out+stderr, syscall.ECONNRESET.Error()) ||
			strings.Contains(out, "name: demo-chart") ||
			strings.Contains(
				out+stderr,
				"[OrphanedResource]",
			), "terminal replay became success or warning: exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	c, err := sourcecache.New(t.Context(), cache)
	assert.NoError(t, err)
	_, _, _, err = c.Blob(
		t.Context(),
		key,
		&sourcecache.Policy{},
		func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			t.Fatal("fresh replay invoked producer")
			return sourcecache.ProducerResult{}, nil
		},
	)
	{
		terminal, ok := errors.AsType[*sourcecache.TerminalError](
			err,
		)
		assert.FatalIf(t, !ok || terminal.Attempted || !errors.Is(err, manifest.ErrFlux) ||
			!errors.Is(err, syscall.ECONNRESET), "fresh replay lost its non-attempt classification: %v", err)
	}
	assert.NoError(t, c.Close())
}

func e2ENamespaceOrphansScenarioLoop(t *testing.T, loopInputs struct {
	namespace *string
	flags     *[]string
},
) {
	t.Helper()

	for _, selection := range [][]string{{"all"}, {"hr"}, {"hr", "demo"}} {
		if (*loopInputs.namespace) == "absent" && len(selection) > 1 {
			continue
		}
		args := append(slices.Clone(selection), "--namespace", (*loopInputs.namespace))
		args = append(args, (*loopInputs.flags)...)
		requireCLIOKResult6 := harness.RequireCLIOK(t, append([]string{"build"}, args...)...)
		first, _ := requireCLIOKResult6.Stdout, requireCLIOKResult6.Stderr
		requireCLIOKResult7 := harness.RequireCLIOK(t, append([]string{"build"}, args...)...)
		second, _ := requireCLIOKResult7.Stdout, requireCLIOKResult7.Stderr
		assert.Equal(t, first, second)
		objects, err := testutil.SplitDocs([]byte(first))
		assert.NoError(t, err)
		if (*loopInputs.namespace) != "absent" {
			assert.Equal(t, len(objects) > 0, true)
		}
		for _, object := range objects {
			metadata3 := manifest.DocMetadata(object)
			ns := metadata3.Namespace
			assert.Equal(t, ns == "" || ns == (*loopInputs.namespace), true)
		}
		requireCLIOKResult8 := harness.RequireCLIOK(t, append([]string{"test"}, args...)...)
		result, _ := requireCLIOKResult8.Stdout, requireCLIOKResult8.Stderr
		assert.Equal(
			t,
			strings.Contains(result, "failed") || strings.Contains(result, "blocked"),
			false,
		)
	}
}
