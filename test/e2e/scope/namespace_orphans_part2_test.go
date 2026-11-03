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

	root, cache, namespaces := namespaceOrphanFixture(t, (*scenario))
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
	hr, _ := harness.RequireCLIOK(t, getArgs...)
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
	build, _ := harness.RequireCLIOK(t, buildArgs...)
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
			name, namespace := manifest.DocMetadata(doc)
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
	out, _ := harness.RequireCLIOK(t, append([]string{"test", "all"}, flags...)...)
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

	root, cache, _ := namespaceOrphanFixture(t, "explicit")
	flags := []string{
		"--path",
		filepath.Join(root, "flux"),
		"--cache-dir",
		cache,
		"--concurrency",
		"2",
		"--no-progress",
	}
	out, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
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
				sourcecache.Policy{},
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
		out, stderr, code := harness.RunCLIBuffers(append(command, flags...)...)
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
		sourcecache.Policy{},
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

func e2ERuntimeOwnerNamespaceChangedOnlyName(t *testing.T, transition *struct {
	name      string
	namespace string
	value     string
},
) {
	t.Helper()

	before := runtimeOwnerNamespaceFixture(t, "", "before")
	after := runtimeOwnerNamespaceFixture(t, transition.namespace, transition.value)
	for _, direction := range []struct{ name, current, original, old, new string }{
		{"forward", after, before, "before", transition.value},
		{"reverse", before, after, transition.value, "before"},
	} {
		t.Run(
			direction.name,
			func(t *testing.T) { e2ERuntimeOwnerNamespaceChangedOnlyNameName(t, &direction, &transition) },
		)
	}
}

func e2EChildKustomizationIdentityNameCase(t *testing.T, flags *[]string, tc **struct {
	name      string
	target    string
	authored  string
	namespace string
}, capturedInputs struct {
	root        *string
	rawBodies   *[]string
	order       *[2]int
	rawVersions *[]string
	removed     *int
},
) {
	t.Helper()

	renderRaw := func() map[string]string {
		t.Helper()
		out, stderr, code := harness.RunCLIBuffers(
			append([]string{"build", "all", "-o", "yaml"}, (*flags)...)...)
		assert.FatalIf(t, code != 0, "build exit=%d stderr=%s", code, stderr)
		raw := make(map[string]string)
		for body := range strings.SplitSeq(out, "---\n") {
			docs, err := testutil.SplitDocs([]byte(body))
			assert.NoError(t, err)
			for _, doc := range docs {
				version := manifest.DocAPIVersion(doc)
				name, ns := manifest.DocMetadata(doc)
				if manifest.DocKind(doc) != manifest.KindKustomization || name != "child" ||
					version == "kustomize.toolkit.fluxcd.io/v1" {
					continue
				}
				{
					_, duplicate := raw[version]
					assert.FatalIf(
						t,
						duplicate || ns != (*tc).namespace,
						"duplicate or wrong namespace for raw %s: %s",
						version,
						body,
					)
				}
				raw[version] = body
			}
		}
		return raw
	}
	testutil.WriteFile(
		t,
		(*capturedInputs.root),
		"apps/raw.yaml",
		(*capturedInputs.rawBodies)[capturedInputs.order[0]]+"---\n"+(*capturedInputs.rawBodies)[capturedInputs.order[1]],
	)
	before := renderRaw()
	assert.FatalIf(
		t,
		len(before) != 2 || before[(*capturedInputs.rawVersions)[0]] == "" ||
			before[(*capturedInputs.rawVersions)[1]] == "",
		"raw groups did not coexist: %v",
		before,
	)
	remaining := 1 - (*capturedInputs.removed)
	version := (*capturedInputs.rawVersions)[remaining]
	testutil.WriteFile(t, (*capturedInputs.root), "apps/raw.yaml", (*capturedInputs.rawBodies)[remaining])
	after := renderRaw()
	assert.FatalIf(
		t,
		len(after) != 1 || after[version] != before[version],
		"withdrawal changed surviving bytes or retained stale output: before=%v after=%v",
		before,
		after,
	)
	nextVersion := strings.TrimSuffix(version, "/v1") + "/v2"
	testutil.WriteFile(
		t,
		(*capturedInputs.root),
		"apps/raw.yaml",
		strings.Replace((*capturedInputs.rawBodies)[remaining], version, nextVersion, 1),
	)
	versioned := renderRaw()
	assert.FatalIf(t, len(versioned) != 1 ||
		versioned[nextVersion] != strings.Replace(
			before[version],
			version,
			nextVersion,
			1,
		), "version-only change split identity or changed content: before=%v after=%v", before, versioned)
}

func e2ERuntimeOwnerNamespaceChangedOnlyNameName(t *testing.T, direction *struct {
	name     string
	current  string
	original string
	old      string
	new      string
}, transition **struct {
	name      string
	namespace string
	value     string
},
) {
	t.Helper()

	flags := []string{
		"--path",
		direction.current,
		"--path-orig",
		direction.original,
		"--cache-dir",
		"",
		"--concurrency",
		"2",
		"--no-progress",
	}
	out := runtimeOwnerNamespaceOutput(t, append([]string{"build", "all"}, flags...)...)
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	counts := map[string]int{}
	for _, doc := range docs {
		kind := manifest.DocKind(doc)
		name, ns := manifest.DocMetadata(doc)
		counts[kind]++
		if kind == manifest.KindHelmRelease || kind == manifest.KindConfigMap {
			assert.Equal(t, ns, "apps")
			if kind == manifest.KindConfigMap {
				assert.Equal(t, name, "demo-chart")
				assert.Equal(t, doc["data"].(map[string]any)["value"], any(direction.new))
			}
		}
	}
	assert.Equal(t, counts[manifest.KindHelmRelease], 1)
	assert.Equal(t, counts[manifest.KindConfigMap], 1)
	out = runtimeOwnerNamespaceOutput(t, append([]string{"diff", "all", "-o", "json"}, flags...)...)
	var result struct {
		Changes []struct {
			Kind      string `json:"Kind"`
			Name      string `json:"Name"`
			Namespace string `json:"Namespace"`
			Status    string `json:"Status"`
			Parent    struct {
				Kind      string `json:"Kind"`
				Name      string `json:"Name"`
				Namespace string `json:"Namespace"`
			} `json:"Parent"`
			Old map[string]any `json:"Old"`
			New map[string]any `json:"New"`
		} `json:"Changes"`
		Suppressed []any `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.Equal(t, len(result.Suppressed), 0)
	if (*transition).namespace != "" {
		assert.Equal(t, len(result.Changes), 0)
		return
	}
	workloads := 0
	for changeIndex := range result.Changes {
		change := result.Changes[changeIndex]
		assert.ErrorIf(t, change.Parent.Namespace == "", "empty-namespace parent: %+v", change)
		if change.Kind == manifest.KindConfigMap {
			workloads++
			assert.Equal(t, change.Name, "demo-chart")
			assert.Equal(t, change.Namespace, "apps")
			assert.Equal(t, change.Status, "changed")
			assert.Equal(t, change.Parent.Kind, manifest.KindHelmRelease)
			assert.Equal(t, change.Parent.Name, "demo")
			assert.Equal(t, change.Parent.Namespace, "apps")
			assert.Equal(t, change.Old["data"].(map[string]any)["value"], any(direction.old))
			assert.Equal(t, change.New["data"].(map[string]any)["value"], any(direction.new))
		}
	}
	assert.Equal(t, workloads, 1)
}

func e2EChildKustomizationIdentityNameCase2(t *testing.T, flags *[]string, tc **struct {
	name      string
	target    string
	authored  string
	namespace string
}, capturedInputs struct {
	root        *string
	rawBodies   *[]string
	order       *[2]int
	rawVersions *[]string
	removed     *int
},
) {
	t.Helper()

	e2EChildKustomizationIdentityNameCase(t, flags, tc, struct {
		root        *string
		rawBodies   *[]string
		order       *[2]int
		rawVersions *[]string
		removed     *int
	}{root: capturedInputs.root, rawBodies: capturedInputs.rawBodies, order: capturedInputs.order, rawVersions: capturedInputs.rawVersions, removed: capturedInputs.removed})
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
		first, _ := harness.RequireCLIOK(t, append([]string{"build"}, args...)...)
		second, _ := harness.RequireCLIOK(t, append([]string{"build"}, args...)...)
		assert.Equal(t, first, second)
		objects, err := testutil.SplitDocs([]byte(first))
		assert.NoError(t, err)
		if (*loopInputs.namespace) != "absent" {
			assert.Equal(t, len(objects) > 0, true)
		}
		for _, object := range objects {
			_, ns := manifest.DocMetadata(object)
			assert.Equal(t, ns == "" || ns == (*loopInputs.namespace), true)
		}
		result, _ := harness.RequireCLIOK(t, append([]string{"test"}, args...)...)
		assert.Equal(
			t,
			strings.Contains(result, "failed") || strings.Contains(result, "blocked"),
			false,
		)
	}
}
