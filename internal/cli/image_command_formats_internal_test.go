package cli

import (
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Images_GetAndDiffFormatsMatch(t *testing.T) {
	current, orig := writeFixture(t), writeFixture(t)
	fixture := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: apps
  annotations:
    record: apiserver_request:burnrate1d
    address: 0.0.0.0:80
spec:
  selector: {matchLabels: {app: demo}}
  template:
    metadata: {labels: {app: demo}}
    spec:
      containers:
        - {name: z, image: ghcr.io/example/z:2}
        - name: a
          image: ghcr.io/example/a:1
          env:
            - {name: TOOL_IMAGE, value: ghcr.io/example/env:v1}
        - {name: untagged, image: nginx}
        - {name: duplicate, image: ghcr.io/example/z:2}
      initContainers:
        - {name: init, image: ghcr.io/example/init:1}
---
apiVersion: batch/v1
kind: CronJob
metadata: {name: job, namespace: apps}
spec:
  schedule: '0 * * * *'
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - {name: cron, image: ghcr.io/example/cron:1}
          initContainers:
            - {name: init, image: ghcr.io/example/init:1}
---
apiVersion: example.test/v1
kind: Workload
metadata:
  name: worker
  namespace: apps
  annotations:
    tag: 0.10.12@sha256:5338b99662e2e15048b4e14b49e17279d384970fdcb3c29169f509f806f49cbe
spec:
  imageName: ghcr.io/example/declared:v1
  worker:
    template:
      spec:
        containers:
          - {name: worker, image: ghcr.io/example/a:1}
        ephemeralContainers:
          - {name: debug, image: ghcr.io/example/debug:3}
`
	testutil.WriteFile(t, current, "apps/cm.yaml", fixture)
	cache := t.TempDir()
	want := []string{
		"ghcr.io/example/a:1",
		"ghcr.io/example/cron:1",
		"ghcr.io/example/debug:3",
		"ghcr.io/example/declared:v1",
		"ghcr.io/example/env:v1",
		"ghcr.io/example/init:1",
		"ghcr.io/example/z:2",
		"nginx",
	}
	for _, format := range []string{"name", "yaml", "json"} {
		t.Run(format, func(t *testing.T) {
			runImagesGetAndDiffFormatsMatchFormat(t, &current, &cache, struct {
				format *string
				orig   *string
				want   *[]string
			}{format: &format, orig: &orig, want: &want})
		})
	}
	noise, changed := writeFixture(t), writeFixture(t)
	testutil.WriteFile(t, noise, "apps/cm.yaml", strings.NewReplacer(
		"apiserver_request:burnrate1d", "apiserver_request:burnrate6h",
		"0.0.0.0:80", "127.0.0.1:9234",
	).Replace(fixture))
	testutil.WriteFile(t, changed, "apps/cm.yaml", strings.NewReplacer(
		"ghcr.io/example/a:1", "ghcr.io/example/a:2",
		"ghcr.io/example/declared:v1", "ghcr.io/example/declared:v2",
		"ghcr.io/example/env:v1", "ghcr.io/example/env:v2",
	).Replace(fixture))
	t.Run("noise/get", func(t *testing.T) { runImagesGetAndDiffFormatsMatchNoiseGet(t, &noise, &cache, &want) })
	for _, tc := range []struct {
		name           string
		current, orig  string
		includeRemoved bool
		want           string
	}{
		{name: "noise/additions", current: noise, orig: current},
		{name: "noise/including removals", current: noise, orig: current, includeRemoved: true},
		{name: "real/additions", current: changed, orig: current, want: "ghcr.io/example/a:2\nghcr.io/example/declared:v2\nghcr.io/example/env:v2\n"},
		{name: "real/including removals", current: changed, orig: current, includeRemoved: true, want: "ghcr.io/example/a:1\nghcr.io/example/a:2\nghcr.io/example/declared:v1\nghcr.io/example/declared:v2\nghcr.io/example/env:v1\nghcr.io/example/env:v2\n"},
		{name: "reversed/additions", current: current, orig: changed, want: "ghcr.io/example/a:1\nghcr.io/example/declared:v1\nghcr.io/example/env:v1\n"},
		{name: "reversed/including removals", current: current, orig: changed, includeRemoved: true, want: "ghcr.io/example/a:1\nghcr.io/example/a:2\nghcr.io/example/declared:v1\nghcr.io/example/declared:v2\nghcr.io/example/env:v1\nghcr.io/example/env:v2\n"},
	} {
		t.Run(tc.name, func(t *testing.T) { runImagesGetAndDiffFormatsMatchName(t, &tc, &cache) })
	}
	t.Run("empty/default", func(t *testing.T) { runImagesGetAndDiffFormatsMatchEmptyDefault(t, &orig, &cache) })
	t.Run("unchanged/json", func(t *testing.T) { runImagesGetAndDiffFormatsMatchUnchangedJSON(t, &current, &cache) })
}

func runImagesGetAndDiffFormatsMatchFormat(t *testing.T, current *string, cache *string, capturedInputs struct {
	format *string
	orig   *string
	want   *[]string
},
) {
	t.Helper()

	flags := make([]string, 0, 8)
	flags = append(flags, "--path", (*current), "--cache-dir", (*cache), "--concurrency", "2")
	flags = append(flags, "-o", (*capturedInputs.format))
	get, stderr, code := runCLI(t, append([]string{"get", "images"}, flags...)...)
	assert.FatalIf(t, code != 0, "get images exited %d: %s", code, stderr)
	diffArgs := append([]string{"diff", "images", "--path-orig", (*capturedInputs.orig)}, flags...)
	delta, stderr, code := runCLI(t, diffArgs...)
	assert.FatalIf(t, code != 0, "diff images exited %d: %s", code, stderr)
	assert.Diff(t, delta, get)
	var images []string
	if (*capturedInputs.format) == "name" {
		assert.Equal(t, get, strings.Join((*capturedInputs.want), "\n")+"\n")
		images = strings.Split(strings.TrimSuffix(get, "\n"), "\n")
	} else {
		prefix := "- "
		wire := "- " + strings.Join((*capturedInputs.want), "\n- ") + "\n"
		if (*capturedInputs.format) == "json" {
			prefix = "["
			wire = "[\n  \"" + strings.Join((*capturedInputs.want), "\",\n  \"") + "\"\n]\n"
		}
		assert.FatalIf(
			t,
			!strings.HasPrefix(get, prefix) || !strings.HasPrefix(delta, prefix),
			"wrong %s wire format: get=%q diff=%q",
			(*capturedInputs.format),
			get,
			delta,
		)
		assert.Equal(t, get, wire)
		assert.NoError(t, yaml.Unmarshal([]byte(get), &images))
	}
	assert.Diff(t, images, (*capturedInputs.want))
}

func runImagesGetAndDiffFormatsMatchNoiseGet(t *testing.T, noise *string, cache *string, want *[]string) {
	t.Helper()

	stdout, stderr, code := runCLI(
		t,
		"get",
		"images",
		"--path",
		(*noise),
		"--cache-dir",
		(*cache),
		"--concurrency",
		"2",
	)
	assert.FatalIf(t, code != 0, "noise get images: exit=%d stderr=%s", code, stderr)
	assert.Equal(t, stdout, "IMAGE\n"+strings.Join((*want), "\n")+"\n")
}

func runImagesGetAndDiffFormatsMatchName(t *testing.T, tc *struct {
	name           string
	current        string
	orig           string
	includeRemoved bool
	want           string
}, cache *string,
) {
	t.Helper()

	args := []string{
		"diff",
		"images",
		"--path",
		tc.current,
		"--path-orig",
		tc.orig,
		"--cache-dir",
		(*cache),
		"--concurrency",
		"2",
	}
	if tc.includeRemoved {
		args = append(args, "--include-removed")
	}
	stdout, stderr, code := runCLI(t, args...)
	assert.FatalIf(t, code != 0, "diff images: exit=%d stderr=%s", code, stderr)
	assert.Equal(t, stdout, tc.want)
}

func runImagesGetAndDiffFormatsMatchEmptyDefault(t *testing.T, orig *string, cache *string) {
	t.Helper()

	for _, verb := range []string{"get", "diff"} {
		t.Run(verb, func(t *testing.T) { runImagesGetAndDiffFormatsMatchEmptyDefaultVerb(t, &verb, &orig, &cache) })
	}
}

func runImagesGetAndDiffFormatsMatchUnchangedJSON(t *testing.T, current *string, cache *string) {
	t.Helper()

	stdout, stderr, code := runCLI(t, "diff", "images", "--path", (*current), "--path-orig", (*current),
		"--cache-dir", (*cache), "--concurrency", "2", "-o", "json")
	assert.FatalIf(t, code != 0, "unchanged diff images: exit=%d stderr=%s", code, stderr)
	{
		got := strings.TrimSpace(stdout)
		assert.FatalIf(t, got != "[]" && got != "null", "unchanged image diff = %q, want [] or null", stdout)
	}
}

func runImagesGetAndDiffFormatsMatchEmptyDefaultVerb(t *testing.T, verb *string, orig **string, cache **string) {
	t.Helper()

	args := []string{(*verb), "images", "--path", (*(*orig)), "--cache-dir", (*(*cache)), "--concurrency", "2"}
	if (*verb) == "diff" {
		args = append(args, "--path-orig", writeFixture(t))
	}
	stdout, stderr, code := runCLI(t, args...)
	want := ""
	if (*verb) == "get" {
		want = "IMAGE\n"
	}
	assert.FatalIf(
		t,
		code != 0 || stdout != want,
		"empty %s images: exit=%d stdout=%q stderr=%s",
		(*verb),
		code,
		stdout,
		stderr,
	)
	for _, format := range []string{"name", "yaml", "json"} {
		out, diagnostics, status := runCLI(t, append(args, "-o", format)...)
		expected := ""
		if format != "name" {
			expected = "[]\n"
			if (*verb) == "get" {
				expected = "null\n"
			}
		}
		assert.FatalIf(
			t,
			status != 0 || out != expected,
			"empty %s/%s: exit=%d stdout=%q stderr=%s",
			(*verb),
			format,
			status,
			out,
			diagnostics,
		)
	}
}
