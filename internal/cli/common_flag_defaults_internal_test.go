package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestBindCommon_DefaultValues(t *testing.T) {
	t.Parallel()
	assert.Equal(t, buildOrchCfg(&commonFlags{}).SourceRetry, sourcecache.RetryConfig{
		Attempts: 3, MinWait: 200 * time.Millisecond, MaxWait: 3 * time.Second, Jitter: 0.1,
	})
	var flags commonFlags
	bindCommon(pflag.NewFlagSet("test", pflag.ContinueOnError), &flags)
	assert.Diff(t, flags.skipKinds, []string{"Secret", "CustomResourceDefinition"})
	opts := buildOrchCfg(&flags).HelmOptions
	assert.Equal(t, opts.SkipCRDs, true)
	assert.Equal(t, opts.SkipTests, true)
	assert.Diff(t, opts.SkipKinds, []string{"CustomResourceDefinition", "Secret"})
}

func TestBindCommon_OnReconcilingSubcommands(t *testing.T) {
	t.Parallel()
	cmd := New("test", RunConfig{})
	for _, tc := range []struct {
		path    string
		maxArgs int
	}{
		{path: "build"},
		{path: "build all"},
		{path: "build ks", maxArgs: 1},
		{path: "build hr", maxArgs: 1},
		{path: "test"},
		{path: "test all"},
		{path: "test ks", maxArgs: 1},
		{path: "test hr", maxArgs: 1},
	} {
		sub, _, err := cmd.Find(strings.Fields(tc.path))
		assert.NoError(t, err)
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
		flag := sub.Flags().Lookup("disable-chart-digest-tracking")
		assert.FatalIf(t, flag == nil || flag.NoOptDefVal != "true", "%v: missing bare boolean binding", argv)
		assert.NoError(t, sub.ParseFlags([]string{"--kube-version=1.31.2", "-a", "fixture.example/v1", "--enable-dns"}))
		for name, want := range map[string]string{"kube-version": "1.31.2", "api-versions": "fixture.example/v1", "enable-dns": "true"} {
			assert.Equal(t, sub.Flags().Lookup(name).Value.String(), want)
		}
		fresh, _, err := New("test", RunConfig{}).Find(argv)
		assert.NoError(t, err)
		for _, name := range []string{"kube-version", "api-versions", "enable-dns"} {
			assert.Equal(t, fresh.Flags().Lookup(name).Value.String(), fresh.Flags().Lookup(name).DefValue)
		}
	}
}
