package cli

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

const (
	seedIdentityDefinition  = "clusters/cluster-b/definition"
	seedIdentitySortWarning = "Warning: Sorting order is set both in 'kustomization.yaml' ('sortOptions') " +
		"and in a CLI flag ('--reorder'). Using the kustomization file over the CLI flag."
)

type seedIdentityOutsideCase struct {
	name, resources, extra string
	ambiguous, fifo        bool
}

func testSeedIdentityOutsideLookups(t *testing.T) {
	t.Helper()
	t.Run("exploratory failure", testSeedIdentityExploratoryFailure)
	t.Run("renamed outside sibling", testSeedIdentityRenamedOutside)
	t.Run("paired outside sibling", testSeedIdentityPairedOutside)
	t.Run("namespace-only ambiguity", testSeedIdentityAmbiguousNamespace)
}

func testSeedIdentityExploratoryFailure(t *testing.T) {
	t.Helper()
	fixture := seedIdentityNamedFixture(t, "parent namespace")
	run := func(name string) cliResult {
		t.Helper()
		return runCLI(t, "build", "hr", name, "-n", fixture.namespace, "--concurrency", "2",
			"--path", fixture.entry, "--cache-dir", t.TempDir(), "--no-progress", "--log-level", "debug")
	}
	control := run(fixture.name)
	assert.FatalIf(t, control.code != 0 || control.stdout == "", "control: %+v", control)

	const absent = "absent"
	const miss = `no HelmRelease named "absent" in --path`
	const unresolved = "cannot establish membership"
	cleanMiss := run(absent)
	assert.Diff(t, cleanMiss.code, 1)
	assert.Diff(t, cleanMiss.stdout, "")
	assert.FatalIf(t, !strings.Contains(cleanMiss.stderr, miss) || strings.Contains(cleanMiss.stderr, unresolved),
		"clean miss: %+v", cleanMiss)

	const broken = "unrelated"
	testutil.WriteFile(t, fixture.root, broken+"/kustomization.yaml", "namespace: other\nresources: [missing.yaml]\n")
	found := run(fixture.name)
	logSeedRenders(t, "broken candidate", &found)
	assert.FatalIf(t, !strings.Contains(found.stderr, `msg="kustomize: build" path=`+broken+"\n"),
		"discovery did not try the broken package: %s", found.stderr)
	assert.FatalIf(t, found.code != 0, "found with broken candidate: %+v", found)
	assert.Diff(t, found.stdout, control.stdout)
	assert.FatalIf(t, strings.Contains(found.stderr, unresolved) || strings.Contains(found.stderr, "missing.yaml"),
		"found with broken candidate: %+v", found)

	unknown := run(absent)
	assert.Diff(t, unknown.code, 1)
	assert.Diff(t, unknown.stdout, "")
	for _, fragment := range []string{unresolved, absent, fixture.entry, broken, "missing.yaml"} {
		assert.FatalIf(t, !strings.Contains(unknown.stderr, fragment), "missing %q: %+v", fragment, unknown)
	}
	assert.FatalIf(t, strings.Contains(unknown.stderr, miss), "unresolved lookup reported as absent: %+v", unknown)

	t.Run("namespace-only failure", func(t *testing.T) {
		result := runCLI(t, "build", "hr", "-n", fixture.namespace, "--concurrency", "2",
			"--path", fixture.entry, "--cache-dir", t.TempDir(), "--no-progress")
		assert.Diff(t, result.code, 1)
		assert.Diff(t, result.stdout, "")
		for _, fragment := range []string{broken, "missing.yaml"} {
			assert.FatalIf(t, !strings.Contains(result.stderr, fragment), "missing %q: %+v", fragment, result)
		}
	})

	file := filepath.Join(fixture.entry, "release.yaml")
	body, err := os.ReadFile(file)
	assert.NoError(t, err)
	testutil.WriteFile(t, fixture.entry, "release.yaml",
		strings.ReplaceAll(string(body), "./charts/demo", "./charts/missing-prerequisite"))
	failed := run(fixture.name)
	assert.Diff(t, failed.code, 1)
	assert.Diff(t, failed.stdout, "")
	assert.FatalIf(t, !strings.Contains(failed.stderr, "charts/missing-prerequisite") ||
		strings.Contains(failed.stderr, unresolved) || strings.Contains(failed.stderr, "missing.yaml"),
		"real prerequisite failure: %+v", failed)
}

func testSeedIdentityAmbiguousNamespace(t *testing.T) {
	t.Helper()
	fixture := seedIdentityNamedFixture(t, "parent namespace")
	const owner = "in owning Kustomization.kustomize.toolkit.fluxcd.io/" + seedIdentityDefinition
	const recipe = "namespace: cluster-b\nresources: [leaf/release.yaml, outside/sibling.yaml]\n" +
		"sortOptions: {order: legacy}\n"
	release, err := os.ReadFile(filepath.Join(fixture.entry, "release.yaml"))
	assert.NoError(t, err)
	independent := strings.NewReplacer(
		"name: demo", "name: independent", "metadata:\n", "metadata:\n  namespace: cluster-b\n",
	).Replace(string(release))
	testutil.WriteFile(t, fixture.entry, "independent.yaml", independent)
	testutil.WriteFile(t, filepath.Dir(fixture.entry), "kustomization.yaml", recipe)
	flags := []string{
		"-n", "cluster-b", "--concurrency", "2", "--path", fixture.entry, "--no-progress", "--log-level", "debug",
	}
	inside := runCLI(t, append([]string{"build", "hr", "independent", "--cache-dir", t.TempDir()}, flags...)...)
	assert.FatalIf(t, inside.code != 0 || !strings.Contains(inside.stdout, "helm.toolkit.fluxcd.io/name: independent"),
		"independent release: %+v", inside)

	fullCache := t.TempDir()
	full := runCLI(t, "build", "all", "--only-kinds", "HelmRelease", "--concurrency", "2", "--no-progress",
		"--path", filepath.Dir(fixture.entry), "--cache-dir", fullCache)
	assert.FatalIf(t, full.code != 0 || !strings.Contains(full.stdout, "name: sibling"), "full build: %+v", full)

	for _, tc := range []struct{ name, cache string }{{"fresh cache", t.TempDir()}, {"full cache", fullCache}} {
		t.Run(tc.name, func(t *testing.T) {
			narrow := runCLI(t, append([]string{"build", "hr", "--cache-dir", tc.cache}, flags...)...)
			assert.Diff(t, narrow.code, 1)
			assert.Diff(t, narrow.stdout, "")
			for _, fragment := range []string{"cannot establish membership", fixture.entry, owner} {
				assert.FatalIf(t, !strings.Contains(narrow.stderr, fragment), "missing %q: %+v", fragment, narrow)
			}
		})
	}
}

func testSeedIdentityRenamedOutside(t *testing.T) {
	const rename = "patches:\n- target: {kind: HelmRelease, name: sibling}\n" +
		"  patch: '[{\"op\":\"replace\",\"path\":\"/metadata/name\",\"value\":\"renamed\"}]'\n"
	var firstFull string
	for _, tc := range []seedIdentityOutsideCase{
		{name: "outside first", resources: "outside/sibling.yaml, leaf/release.yaml"},
		{name: "inside first", resources: "leaf/release.yaml, outside/sibling.yaml"},
		{name: "FIFO", resources: "outside/sibling.yaml, leaf/release.yaml", extra: "sortOptions: {order: fifo}\n", fifo: true},
		{
			name: "explicit reordering", resources: "outside/sibling.yaml, leaf/release.yaml",
			extra: "sortOptions: {order: legacy}\n", ambiguous: true,
		},
		{name: "reduced output count", resources: "outside/local.yaml, outside/sibling.yaml, leaf/release.yaml", ambiguous: true},
		{name: "cross-scope name reference", ambiguous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := seedIdentityNamedFixture(t, "parent namespace")
			lookup := "renamed"
			fullNames := []string{"demo", "renamed"}
			recipe := "namespace: cluster-b\nresources: [" + tc.resources + "]\n" + rename + tc.extra
			switch tc.name {
			case "reduced output count":
				testutil.WriteFile(t, fixture.root, seedIdentityDefinition+"/outside/local.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: local\n"+
						"  annotations: {config.kubernetes.io/local-config: 'true'}\n")
			case "cross-scope name reference":
				lookup, fullNames = "winner", []string{"moved-winner", "winner"}
				recipe = seedIdentityCrossScopeReference(t, fixture.root, seedIdentityDefinition)
			}
			testutil.WriteFile(t, fixture.root, seedIdentityDefinition+"/kustomization.yaml", recipe)
			cache := t.TempDir()
			full := runCLI(
				t,
				"build",
				"all",
				"--only-kinds",
				"HelmRelease",
				"--concurrency",
				"2",
				"--path",
				filepath.Join(fixture.root, seedIdentityDefinition),
				"--cache-dir",
				cache,
				"--no-progress",
				"--log-level",
				"debug",
			)
			assert.FatalIf(t, full.code != 0, "full build: %+v", full)
			docs := seedIdentitySnapshot(t, full.stdout).Manifests[0].Documents
			names := make([]string, len(docs))
			for i, doc := range docs {
				names[i] = manifest.DocIdentity(doc).Name
			}
			slices.Sort(names)
			assert.FatalIf(
				t,
				!slices.Equal(names, fullNames),
				"full releases = %v, want %v\n%s",
				names,
				fullNames,
				full.stdout,
			)
			switch tc.name {
			case "outside first":
				firstFull = full.stdout
			case "inside first", "FIFO", "explicit reordering":
				assert.Diff(t, full.stdout, firstFull)
			}
			if tc.fifo {
				assert.Diff(t, strings.Count(full.stderr, seedIdentitySortWarning), 1)
			}
			fixture.name = lookup
			testSeedIdentityOutsideCaches(t, &fixture, cache, &tc)
		})
	}
}

func testSeedIdentityOutsideCaches(
	t *testing.T,
	fixture *seedIdentityFixture,
	cache string,
	tc *seedIdentityOutsideCase,
) {
	t.Helper()
	for _, test := range []struct{ label, cache string }{{"fresh cache", t.TempDir()}, {"full cache", cache}} {
		t.Run(test.label, func(t *testing.T) {
			narrow := runCLI(t, "build", "hr", fixture.name, "-n", "cluster-b", "--concurrency", "2",
				"--path", fixture.entry, "--cache-dir", test.cache, "--no-progress", "--log-level", "debug")
			assert.Diff(t, narrow.code, 1)
			assert.Diff(t, narrow.stdout, "")
			if tc.ambiguous {
				owner := "in owning Kustomization.kustomize.toolkit.fluxcd.io/" + seedIdentityDefinition
				for _, text := range []string{"cannot establish membership", fixture.name, fixture.entry, owner} {
					assert.FatalIf(t, !strings.Contains(narrow.stderr, text), "missing %q: %+v", text, narrow)
				}
				assert.FatalIf(
					t,
					strings.Contains(narrow.stderr, "no HelmRelease named"),
					"lookup result: %+v",
					narrow,
				)
				return
			}
			miss := "no HelmRelease named " + strconv.Quote(fixture.name) + " in --path"
			assert.FatalIf(t, !strings.Contains(narrow.stderr, miss), "lookup result: %+v", narrow)
			if tc.fifo {
				const build = "msg=\"kustomize: build\" path=" + seedIdentityDefinition
				assert.Diff(
					t,
					strings.Count(narrow.stderr, seedIdentitySortWarning),
					strings.Count(narrow.stderr, build),
				)
				assert.FatalIf(t, strings.Contains(narrow.stderr, "kustomize: observe input order"),
					"FIFO lookup rendered an observation: %s", narrow.stderr)
			}
		})
	}
}

func seedIdentityCrossScopeReference(t *testing.T, root, definition string) string {
	t.Helper()
	release, err := os.ReadFile(filepath.Join(root, definition, "leaf", "release.yaml"))
	assert.NoError(t, err)
	namespaced := strings.Replace(string(release), "metadata:\n", "metadata:\n  namespace: cluster-b\n", 1)
	testutil.WriteFile(
		t,
		root,
		definition+"/leaf/release.yaml",
		strings.Replace(namespaced, "name: demo", "name: winner", 1),
	)
	testutil.WriteFile(
		t,
		root,
		definition+"/outside/sibling.yaml",
		strings.Replace(namespaced, "name: demo", "name: placeholder", 1),
	)
	// Kustomize's namespaced reference index can see a transient duplicate during this name swap.
	testutil.WriteFile(t, root, definition+"/schema.json", `{
  "swagger": "2.0",
  "paths": {
    "/apis/helm.toolkit.fluxcd.io/v2/helmreleases": {
      "get": {"x-kubernetes-group-version-kind": {
        "group": "helm.toolkit.fluxcd.io", "version": "v2", "kind": "HelmRelease"
      }}
    }
  }
}
`)
	testutil.WriteFile(t, root, definition+"/leaf/config/map.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: placeholder, namespace: cluster-b}\n")
	testutil.WriteFile(t, root, definition+"/leaf/config/kustomization.yaml", `resources: [map.yaml]
patches:
- target: {kind: ConfigMap, name: placeholder}
  patch: '[{"op":"replace","path":"/metadata/name","value":"winner"}]'
`)
	testutil.WriteFile(t, root, definition+"/outside/config/map.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: winner, namespace: cluster-b}\n")
	testutil.WriteFile(t, root, definition+"/outside/config/kustomization.yaml",
		"resources: [map.yaml]\nnamePrefix: moved-\n")
	testutil.WriteFile(t, root, definition+"/references.yaml", `nameReference:
- kind: ConfigMap
  version: v1
  fieldSpecs:
  - group: helm.toolkit.fluxcd.io
    kind: HelmRelease
    path: metadata/name
`)
	return `resources: [leaf/release.yaml, leaf/config, outside/sibling.yaml, outside/config]
configurations: [references.yaml]
openapi: {path: schema.json}
`
}

func testSeedIdentityPairedOutside(t *testing.T) {
	t.Helper()
	base := seedIdentityNamedFixture(t, "parent namespace")
	head := seedIdentityNamedFixture(t, "parent namespace")
	for i, fixture := range []seedIdentityFixture{base, head} {
		testutil.WriteFile(t, fixture.root, seedIdentityDefinition+"/kustomization.yaml", `namespace: cluster-b
resources: [leaf/release.yaml, outside/sibling.yaml]
patches:
- target: {kind: HelmRelease, name: sibling}
  patch: '[{"op":"replace","path":"/metadata/name","value":"renamed"}]'
`)
		file := seedIdentityDefinition + "/outside/sibling.yaml"
		body, err := os.ReadFile(filepath.Join(fixture.root, file))
		assert.NoError(t, err)
		greeting := []string{"before outside greeting", "after outside greeting"}[i]
		testutil.WriteFile(t, fixture.root, file,
			strings.ReplaceAll(string(body), "hello from cluster-b", greeting))
	}
	flags := []string{
		"diff", "hr", "renamed", "-n", "cluster-b", "--concurrency", "2", "--no-progress",
		"--cache-dir", t.TempDir(), "-o", "diff",
	}
	full := runCLI(
		t,
		append(slices.Clone(flags), "--path", filepath.Dir(head.entry), "--path-orig", filepath.Dir(base.entry))...)
	assert.FatalIf(t, full.code != 0 || !strings.Contains(full.stdout, "before outside greeting") ||
		!strings.Contains(full.stdout, "after outside greeting"), "full pair: %+v", full)

	narrow := runCLI(t, append(slices.Clone(flags), "--path", head.entry, "--path-orig", base.entry)...)
	assert.Diff(t, narrow.code, 1)
	assert.Diff(t, narrow.stdout, "")
	assert.FatalIf(t, !strings.Contains(narrow.stderr, `no HelmRelease named "renamed" in --path or --path-orig`),
		"narrow pair: %+v", narrow)

	recipe := seedIdentityDefinition + "/kustomization.yaml"
	body, err := os.ReadFile(filepath.Join(head.root, recipe))
	assert.NoError(t, err)
	testutil.WriteFile(t, head.root, recipe, strings.ReplaceAll(string(body), "name: sibling", "name: demo"))
	file := seedIdentityDefinition + "/leaf/release.yaml"
	body, err = os.ReadFile(filepath.Join(head.root, file))
	assert.NoError(t, err)
	testutil.WriteFile(
		t,
		head.root,
		file,
		strings.ReplaceAll(string(body), "hello from cluster-b", "inside current greeting"),
	)
	paired := runCLI(t, append(slices.Clone(flags), "--path", head.entry, "--path-orig", base.entry)...)
	assert.FatalIf(t, paired.code != 0 || !strings.Contains(paired.stdout, "inside current greeting") ||
		strings.Contains(paired.stdout, "before outside greeting"), "different memberships: %+v", paired)

	testutil.WriteFile(t, base.root, "unrelated/kustomization.yaml", "namespace: other\nresources: [missing.yaml]\n")
	unresolved := runCLI(t, append(slices.Clone(flags), "--path", head.entry, "--path-orig", base.entry)...)
	assert.Diff(t, unresolved.code, 1)
	assert.Diff(t, strings.Count(unresolved.stderr, "cannot establish membership"), 1)
	for _, fragment := range []string{"orig snapshot:", base.entry, "missing.yaml"} {
		assert.FatalIf(t, !strings.Contains(unresolved.stderr, fragment), "missing %q: %+v", fragment, unresolved)
	}
	assert.FatalIf(t, strings.Contains(unresolved.stderr, "current snapshot:") ||
		strings.Contains(unresolved.stderr, "no HelmRelease named"), "paired unresolved lookup: %+v", unresolved)
}
