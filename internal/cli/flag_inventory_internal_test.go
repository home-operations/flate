package cli

import (
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/engine/helm"
	"github.com/home-operations/flate/internal/testutil/assert"
)

type flagSpec struct {
	name, shorthand, usage, env, kind string
	value                             any
}

type commandSpec struct {
	Use, Short string
	Aliases    []string
	localFlags []flagSpec
}

func commandInventory() map[string]commandSpec {
	common := []flagSpec{
		{
			"allow-missing-kinds",
			"",
			"Tolerate missing input kinds (comma-separated values)",
			"FLATE_ALLOW_MISSING_KINDS",
			"stringSlice",
			[]string{},
		},
		{"api-versions", "a", "Set Helm cluster APIs (comma-separated values)", "FLATE_API_VERSIONS", "string", ""},
		{"base", "", "Compare with a Git revision (exclusive with --path-orig)", "FLATE_BASE", "string", ""},
		{"cache-dir", "", "Store fetched sources and renders in this directory", "FLATE_CACHE_DIR", "string", ""},
		{
			"concurrency",
			"",
			"Limit parallel reconciles (0 means unbounded)",
			"FLATE_CONCURRENCY",
			"int",
			strconv.Itoa(DefaultConcurrency()),
		},
		{
			"disable-chart-digest-tracking",
			"",
			"Omit digest suffixes from OCI chart versions",
			"FLATE_DISABLE_CHART_DIGEST_TRACKING",
			"bool",
			"false",
		},
		{"enable-dns", "", "Enable DNS lookups in Helm templates", "FLATE_ENABLE_DNS", "bool", "false"},
		{"git-depth", "", "Set Git fetch depth (0 means full history)", "FLATE_GIT_DEPTH", "int", "1"},
		{
			"kube-version",
			"",
			"Set the cluster Kubernetes version for Helm",
			"FLATE_KUBE_VERSION",
			"string",
			helm.BundledKubeVersion(),
		},
		{"namespace", "n", "Select resources in this namespace", "FLATE_NAMESPACE", "string", ""},
		{"path", "p", "Read Flux manifests from this directory", "FLATE_PATH", "string", "."},
		{
			"path-orig",
			"P",
			"Compare with a baseline directory (exclusive with --base)",
			"FLATE_PATH_ORIG",
			"string",
			"",
		},
		{
			"registry-config",
			"",
			"Read OCI credentials from a native Docker or Helm store",
			"FLATE_REGISTRY_CONFIG",
			"string",
			"",
		},
		{
			"restrict-egress",
			"",
			"Block private-address fetches and local Git transports",
			"FLATE_RESTRICT_EGRESS",
			"bool",
			"false",
		},
		{
			"skip-kinds",
			"",
			"Exclude output kinds (comma-separated values)",
			"FLATE_SKIP_KINDS",
			"stringSlice",
			[]string{"Secret", "CustomResourceDefinition"},
		},
		{
			"strict-substitutions",
			"",
			"Reject undefined substitutions without defaults",
			"FLATE_STRICT_SUBSTITUTIONS",
			"bool",
			"false",
		},
		{"supply", "", "Read reference-only YAML objects (repeatable flag)", "FLATE_SUPPLY", "stringArray", []string{}},
	}
	build := []flagSpec{
		{
			"only-kinds",
			"",
			"Include only output kinds (comma-separated values)",
			"FLATE_ONLY_KINDS",
			"stringSlice",
			[]string{},
		},
		{"output", "o", "Emit: yaml, json", "FLATE_OUTPUT", "string", "yaml"},
	}
	list := flagSpec{"output", "o", "Emit: table, yaml, json, name", "FLATE_OUTPUT", "string", "table"}
	images := flagSpec{"output", "o", "Emit: name, yaml, json", "FLATE_OUTPUT", "string", "name"}
	cacheGC := []flagSpec{
		{"cache-dir", "", "Prune this cache directory (required)", "FLATE_CACHE_DIR", "string", ""},
		{"dry-run", "", "List cache entries to prune without deleting them", "FLATE_DRY_RUN", "bool", "false"},
		{
			"max-age",
			"",
			"Prune cache entries unused for this duration (0 disables)",
			"FLATE_MAX_AGE",
			"duration",
			(30 * 24 * time.Hour).String(),
		},
	}
	buildFlags := slices.Concat(common, build)
	getFlags := slices.Concat(common, []flagSpec{list})
	getSelectorFlags := slices.Concat(common, []flagSpec{
		list,
		{
			"selector",
			"l",
			"Select labels by key=value (comma-separated values)",
			"FLATE_SELECTOR",
			"stringToString",
			map[string]string{},
		},
	})
	diffFlags := slices.Concat(common, []flagSpec{
		{
			"output",
			"o",
			"Emit: human, github, diff, html, brief, gitlab, gitea, json",
			"FLATE_OUTPUT",
			"string",
			"human",
		},
		{
			"strip-attr",
			"",
			"Ignore a metadata key in diffs (repeatable flag)",
			"FLATE_STRIP_ATTR",
			"stringArray",
			[]string{"helm.sh/chart", "checksum/", "app.kubernetes.io/version", "chart"},
		},
		{
			"strip-field",
			"",
			"Ignore a dotted field in diffs (repeatable flag)",
			"FLATE_STRIP_FIELD",
			"stringArray",
			[]string{},
		},
	})
	diffImagesFlags := slices.Concat(common, []flagSpec{
		images,
		{
			"include-removed",
			"",
			"Include images found only in the baseline",
			"FLATE_INCLUDE_REMOVED",
			"bool",
			"false",
		},
	})

	ks := []string{"kustomization", "kustomizations"}
	hr := []string{"helmrelease", "helmreleases"}
	return map[string]commandSpec{
		"flate":             {"flate", "Render and compare Flux manifests offline", nil, nil},
		"flate build":       {"build", "Render Flux objects to YAML (defaults to all)", nil, buildFlags},
		"flate build all":   {"all", "Render all Flux producer output", nil, buildFlags},
		"flate build hr":    {"hr [name]", "Render HelmReleases", hr, buildFlags},
		"flate build ks":    {"ks [name]", "Render Kustomizations", ks, buildFlags},
		"flate cache":       {"cache", "Prune flate's persistent cache", nil, nil},
		"flate cache gc":    {"gc", "Prune stale cache tags and unused Git generations", nil, cacheGC},
		"flate diff":        {"diff", "Diff rendered output (defaults to all)", nil, diffFlags},
		"flate diff all":    {"all", "Diff all Flux producer output against a baseline", nil, diffFlags},
		"flate diff hr":     {"hr [name]", "Diff HelmReleases against another path", hr, diffFlags},
		"flate diff images": {"images", "Diff container images against a baseline", nil, diffImagesFlags},
		"flate diff ks":     {"ks [name]", "Diff Kustomizations against another path", ks, diffFlags},
		"flate get":         {"get", "List Flux resources (defaults to all)", nil, getFlags},
		"flate get all":     {"all", "List every Kustomization and HelmRelease", nil, getFlags},
		"flate get hr":      {"hr [name]", "List HelmReleases", hr, getSelectorFlags},
		"flate get images":  {"images", "List container images across the cluster", nil, getFlags},
		"flate get ks":      {"ks [name]", "List Kustomizations", ks, getSelectorFlags},
		"flate test":        {"test", "Report reconcile status (defaults to all)", nil, common},
		"flate test all":    {"all", "Validate Flux resources", nil, common},
		"flate test hr":     {"hr [name]", "Validate HelmReleases", hr, common},
		"flate test ks":     {"ks [name]", "Validate Kustomizations", ks, common},
	}
}

func TestNew_FlagInventory(t *testing.T) {
	t.Parallel()

	global := []flagSpec{
		{"log-level", "", "Set log level: debug,info,warn,error", "FLATE_LOG_LEVEL", "string", "error"},
		{"no-progress", "", "Hide live progress on stderr", "FLATE_NO_PROGRESS", "bool", "false"},
	}
	for path, spec := range commandInventory() {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			root := New("dev", RunConfig{})
			_, route, _ := strings.Cut(path, " ")
			cmd, args, err := root.Find(strings.Fields(route))
			assert.NoError(t, err)
			assert.Equal(t, len(args), 0)
			assert.Equal(t, cmd.CommandPath(), path)
			assert.NoError(t, cmd.ParseFlags(nil))
			assertFlagInventory(t, cmd.Flags(), slices.Concat(global, spec.localFlags))
		})
	}

	root := New("dev", RunConfig{})
	flateCommands := commandTree(root)
	root.InitDefaultHelpCmd()
	root.InitDefaultCompletionCmd()
	for _, cmd := range commandTree(root) {
		if slices.Contains(flateCommands, cmd) {
			continue
		}
		flags := cmd.InheritedFlags()
		t.Run(cmd.CommandPath(), func(t *testing.T) {
			t.Parallel()
			assertFlagInventory(t, flags, global)
		})
	}
}

func assertFlagInventory(t *testing.T, flags *pflag.FlagSet, specs []flagSpec) {
	t.Helper()
	var gotNames []string
	wantNames := make([]string, len(specs))
	flags.VisitAll(func(flag *pflag.Flag) {
		gotNames = append(gotNames, flag.Name)
	})
	for i, spec := range specs {
		wantNames[i] = spec.name
		flag := flags.Lookup(spec.name)
		assert.FatalIf(t, flag == nil, "missing --%s", spec.name)
		assert.ErrorIf(
			t,
			flag.Value.Type() != spec.kind,
			"--%s type: got %q, want %q",
			spec.name,
			flag.Value.Type(),
			spec.kind,
		)
		assert.Equal(t, flag.Shorthand, spec.shorthand)
		assert.Equal(t, flag.Usage, spec.usage+" [env: "+spec.env+"]")
		switch want := spec.value.(type) {
		case string:
			assert.Equal(t, flag.DefValue, want)
		case []string:
			got, ok := flag.Value.(pflag.SliceValue)
			assert.FatalIf(t, !ok, "--%s does not hold a slice", spec.name)
			assert.ErrorIf(
				t,
				!slices.Equal(got.GetSlice(), want),
				"--%s default: got %q, want %q",
				spec.name,
				got.GetSlice(),
				want,
			)
		case map[string]string:
			got, err := flags.GetStringToString(spec.name)
			assert.NoError(t, err)
			assert.ErrorIf(t, !maps.Equal(got, want), "--%s default: got %v, want %v", spec.name, got, want)
		}
	}
	slices.Sort(gotNames)
	slices.Sort(wantNames)
	assert.Diff(t, gotNames, wantNames)
}

func TestNew_CommandInventory(t *testing.T) {
	t.Parallel()

	want := commandInventory()
	got := make(map[string]*cobra.Command, len(want))
	for _, cmd := range commandTree(New("dev", RunConfig{})) {
		path := cmd.CommandPath()
		_, duplicate := got[path]
		assert.ErrorIf(t, duplicate, "duplicate route %q", path)
		got[path] = cmd
	}
	assert.Diff(t, slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(want)))
	for path, spec := range want {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			cmd := got[path]
			assert.FatalIf(t, cmd == nil, "missing route %q", path)
			assert.Equal(t, cmd.Use, spec.Use)
			assert.Equal(t, cmd.Short, spec.Short)
			assert.Diff(t, cmd.Aliases, spec.Aliases)
		})
	}
}

func commandTree(root *cobra.Command) []*cobra.Command {
	commands := []*cobra.Command{root}
	for i := 0; i < len(commands); i++ {
		commands = append(commands, commands[i].Commands()...)
	}
	return commands
}
