package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type countedInput struct {
	reader io.Reader
	bytes  int
}

func (r *countedInput) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += n
	return n, err
}

func TestLoadInputs_Stdin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		paths        []string
		body         string
		cancel, fail bool
	}{
		{name: "single", paths: []string{"-"}},
		{name: "duplicate", paths: []string{"-", "-"}, fail: true},
		{name: "canceled", paths: []string{"-"}, cancel: true, fail: true},
		{name: "file"},
		{name: "encrypted Secret", paths: []string{"-"}, body: "apiVersion: v1\nkind: Secret\nmetadata: {name: encrypted, namespace: apps}\nstringData: {Z: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]', A: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]', PLAIN: readable, EMPTY: ''}\n"},
		{name: "encrypted ConfigMap", paths: []string{"-"}, body: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: encrypted, namespace: apps}\ndata: {Z: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]', A: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]', PLAIN: readable, EMPTY: ''}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testLoadInputsStdinCase1(t, &tc)
		})
	}
}

func testLoadInputsStdinCase1(t *testing.T, tc *struct {
	name   string
	paths  []string
	body   string
	cancel bool
	fail   bool
},
) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if tc.cancel {
		cancel()
	}
	body := tc.body
	if body == "" {
		body = externalCM
	}
	input := &countedInput{reader: strings.NewReader(body)}
	paths := tc.paths
	if paths == nil {
		paths = []string{substitutionInput(t, externalCM)}
	}
	c := commonFlags{supply: paths}
	err := c.loadInputs(ctx, input)
	assert.FatalIf(t, (err != nil) != tc.fail,
		"load: %v", err)

	want := 0
	if tc.name == "single" || tc.body != "" {
		want = len(body)
	}
	assert.FatalIf(t, input.bytes != want,
		"stdin bytes=%d, want %d", input.bytes, want)
	assert.FatalIf(t, !tc.fail && len(c.suppliedObjects) != 1,
		"objects=%d", len(c.suppliedObjects))

	if tc.body != "" {
		child := c.suppliedObjects[0]
		data := child.Obj.(manifest.Data)
		for _, key := range []string{"A", "Z"} {
			adaptedResult1, err := data.Value(key)
			present := adaptedResult1.Found
			assert.NoError(t, err)
			assert.Equal(t, present, false)
		}
		for key, want := range map[string]string{"PLAIN": "readable", "EMPTY": ""} {
			adaptedResult2, err := data.Value(key)
			got, present := adaptedResult2.Value, adaptedResult2.Found
			assert.NoError(t, err)
			assert.Equal(t, present, true)
			assert.Equal(t, got, want)
		}
		assert.Equal(t, len(child.Warnings), 1)
		assert.Equal(t, child.Warnings[0].Category, manifest.WarnSOPSUnreadable)
		assert.Diff(t, child.Warnings[0].Detail, []string{"A", "Z"})
	}
}

func TestLoadInputs_AnyKind(t *testing.T) {
	t.Parallel()
	body := externalCM + "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: apps}\nspec: {url: 'file:///fixture'}\n---\napiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: widgets.example.io}\nspec: {group: example.io}\n---\napiVersion: example.io/v1\nkind: Widget\nmetadata: {name: custom, namespace: apps}\nspec: {payload: retained}\n"
	cipher := "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	var bodySb112 strings.Builder
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		field := "data"
		if kind == manifest.KindSecret {
			field = "stringData"
		}
		bodySb112.WriteString(
			"---\napiVersion: v1\nkind: " + kind + "\nmetadata: {name: auth, namespace: apps}\n" + field + ": {VALUE: '" + cipher + "', EMPTY: '', PLAIN: readable}\nsops: {encrypted_regex: " + field + "}\n",
		)
	}
	body += bodySb112.String()
	c := commonFlags{supply: []string{"-"}}
	if err := c.loadInputs(t.Context(), strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	assert.FatalIf(t, len(c.suppliedObjects) != 6,
		"objects=%d", len(c.suppliedObjects))
	assert.FatalIf(
		t,
		c.suppliedObjects[3].Doc["spec"].(map[string]any)["payload"] != "retained",
		"%s",
		"raw payload lost",
	)

	for _, child := range c.suppliedObjects[4:] {
		data := child.Obj.(manifest.Data)
		adaptedResult3, err := data.Value("VALUE")
		present := adaptedResult3.Found
		assert.NoError(t, err)
		assert.Equal(t, present, false)
		for key, want := range map[string]string{"PLAIN": "readable", "EMPTY": ""} {
			adaptedResult4, err := data.Value(key)
			got, present := adaptedResult4.Value, adaptedResult4.Found
			assert.NoError(t, err)
			assert.Equal(t, present, true)
			assert.Equal(t, got, want)
		}
		assert.Equal(t, len(child.Warnings), 1)
		assert.Equal(t, child.Warnings[0].Category, manifest.WarnSOPSUnreadable)
		assert.Diff(t, child.Warnings[0].Detail, []string{"VALUE"})
	}
}

func testLoadInputsDevStdin(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/dev/stdin"); err != nil {
		t.Skip("/dev/stdin unavailable")
	}
	if os.Getenv("FLATE_TEST_DEV_STDIN") != "1" {
		executable, err := os.Executable()
		assert.FatalIf(t, err != nil, "%s", err)

		cmd := exec.CommandContext(t.Context(), executable, "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
		cmd.Env = append(os.Environ(), "FLATE_TEST_DEV_STDIN=1")
		cmd.Stdin = strings.NewReader(externalCM)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("stdin test: %v: %s", err, output)
		}
		return
	}
	input := &countedInput{reader: strings.NewReader("invalid injected stdin")}
	c := commonFlags{supply: []string{"/dev/stdin"}}
	if err := c.loadInputs(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	assert.FatalIf(
		t,
		len(c.suppliedObjects) != 1 || input.bytes != 0,
		"%s",
		"ordinary stdin path used the injected reader",
	)
}

func testRunSupplyStdin(t *testing.T) {
	t.Helper()
	root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}")
	input := &countedInput{reader: strings.NewReader(externalCM)}
	var stdout, stderr bytes.Buffer
	cmd := New("test", RunConfig{})
	cmd.SetIn(input)
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"build", "all", "--path", root, "--concurrency=2", "--supply", "-", "--no-progress"})
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		t.Fatalf("stdin command: %v: %s", err, stderr.String())
	}
	if got := substitutionOutput(t, stdout.String()); got != "supplied" || input.bytes != len(externalCM) {
		t.Fatalf("stdin result=%q bytes=%d", got, input.bytes)
	}
}
