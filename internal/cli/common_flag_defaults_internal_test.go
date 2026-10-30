package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/helm"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

// TestBindCommon_DefaultValues sanity-checks the persistent flag set:
// every common flag binds with a known-good default.
func TestBindCommon_DefaultValues(t *testing.T) {
	t.Parallel()
	assert.Equal(t, buildOrchCfg(&commonFlags{}).SourceRetry, sourcecache.RetryConfig{
		Attempts: 3, MinWait: 200 * time.Millisecond, MaxWait: 3 * time.Second, Jitter: 0.1,
	})
	cmd := New("test")
	build, _, err := cmd.Find([]string{"build", "all"})
	assert.FatalIf(t, err != nil, "find build all: %v", err)
	for _, name := range []string{"path", "namespace", "output", "concurrency", "skip-kinds"} {
		assert.ErrorIf(t, build.Flags().Lookup(name) == nil, "expected common flag %q on `build all`", name)
	}
	var flags commonFlags
	bindCommon(pflag.NewFlagSet("test", pflag.ContinueOnError), &flags)
	assert.Diff(t, flags.skipKinds, []string{"Secret", "CustomResourceDefinition"})
	opts := buildOrchCfg(&flags).HelmOptions
	assert.Equal(t, opts.SkipCRDs, true)
	assert.Equal(t, opts.SkipTests, true)
	assert.Diff(t, opts.SkipKinds, []string{"Secret"})
}

func TestBindCommon_OnReconcilingSubcommands(t *testing.T) {
	t.Parallel()
	cmd := New("test")
	for _, tc := range []struct {
		path, use, short string
		aliases          []string
		maxArgs          int
	}{
		{path: "build", use: "build", short: "Render Flux objects to YAML (defaults to all)"},
		{path: "build all", use: "all", short: "Render all Flux producer output"},
		{path: "build ks", use: "ks [name]", short: "Render Kustomizations", aliases: []string{"kustomization", "kustomizations"}, maxArgs: 1},
		{path: "build hr", use: "hr [name]", short: "Render HelmReleases", aliases: []string{"helmrelease", "helmreleases"}, maxArgs: 1},
		{path: "test", use: "test", short: "Report reconcile status (defaults to all)"},
		{path: "test all", use: "all", short: "Validate Flux resources"},
		{path: "test ks", use: "ks [name]", short: "Validate Kustomizations", aliases: []string{"kustomization", "kustomizations"}, maxArgs: 1},
		{path: "test hr", use: "hr [name]", short: "Validate HelmReleases", aliases: []string{"helmrelease", "helmreleases"}, maxArgs: 1},
	} {
		sub, _, err := cmd.Find(strings.Fields(tc.path))
		assert.NoError(t, err)
		assert.Equal(t, sub.Use, tc.use)
		assert.Equal(t, sub.Short, tc.short)
		assert.Diff(t, sub.Aliases, tc.aliases)
		assert.FatalIf(t, sub.Args == nil, "%s: missing argument validator", tc.path)
		assert.NoError(t, sub.Args(sub, nil))
		{
			err := sub.Args(sub, []string{"one"})
			assert.ErrorIf(
				t,
				(err == nil) != (tc.maxArgs == 1),
				"%s: argument limit %d rejected/accepted one name: %v",
				tc.path,
				tc.maxArgs,
				err,
			)
		}
		{
			err := sub.Args(sub, []string{"one", "two"})
			assert.ErrorIf(t, err == nil, "%s: accepted two names", tc.path)
		}
	}
	for _, argv := range [][]string{
		{"build"},
		{"build", "ks"},
		{"build", "hr"},
		{"build", "all"},
		{"diff", "ks"},
		{"diff", "hr"},
		{"diff", "all"},
		{"diff", "images"},
		{"diff"},
		{"get"},
		{"get", "all"},
		{"get", "images"},
		{"get", "ks"},
		{"get", "hr"},
		{"test", "all"},
		{"test", "ks"},
		{"test", "hr"},
		{"test"},
	} {
		sub, _, err := cmd.Find(argv)
		assert.FatalIf(t, err != nil, "find %v: %v", argv, err)
		for _, binding := range []struct {
			name, typ, shorthand, def string
		}{
			{"kube-version", "string", "", helm.BundledKubeVersion()},
			{"api-versions", "string", "a", ""},
			{"enable-dns", "bool", "", "false"},
		} {
			flag := sub.Flags().Lookup(binding.name)
			assert.FatalIf(t, flag == nil, "%v: missing helm flag %s", argv, binding.name)
			assert.Equal(t, flag.Value.Type(), binding.typ)
			assert.Equal(t, flag.Shorthand, binding.shorthand)
			assert.Equal(t, flag.DefValue, binding.def)
			assert.ErrorIf(
				t,
				!strings.Contains(flag.Usage, envKey(binding.name)),
				"%v: missing environment usage for %s",
				argv,
				binding.name,
			)
		}
		for _, name := range []string{"path", "cache-dir"} {
			assert.ErrorIf(t, sub.Flags().Lookup(name) == nil, "%v: missing common flag %s", argv, name)
		}
		flag := sub.Flags().Lookup("disable-chart-digest-tracking")
		assert.FatalIf(
			t,
			flag == nil || flag.Value.Type() != "bool" || flag.NoOptDefVal != "true",
			"%v: missing boolean selection binding",
			argv,
		)
		assert.ErrorIf(
			t,
			!strings.Contains(flag.Usage, envKey("disable-chart-digest-tracking")),
			"%v: missing environment usage",
			argv,
		)
		assert.NoError(t, sub.ParseFlags([]string{"--kube-version=1.31.2", "-a", "fixture.example/v1", "--enable-dns"}))
		for name, want := range map[string]string{"kube-version": "1.31.2", "api-versions": "fixture.example/v1", "enable-dns": "true"} {
			assert.Equal(t, sub.Flags().Lookup(name).Value.String(), want)
		}
		fresh, _, err := New("test").Find(argv)
		assert.NoError(t, err)
		for _, name := range []string{"kube-version", "api-versions", "enable-dns"} {
			assert.Equal(t, fresh.Flags().Lookup(name).Value.String(), fresh.Flags().Lookup(name).DefValue)
		}
	}
	maintenance, _, err := cmd.Find([]string{"cache", "gc"})
	assert.NoError(t, err)
	assert.Equal(t, maintenance.Flags().Lookup("path") == nil, true)
	assert.Equal(t, maintenance.Flags().Lookup("cache-dir") != nil, true)
}
