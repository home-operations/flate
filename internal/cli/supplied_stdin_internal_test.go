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
		cancel, fail bool
	}{
		{name: "single", paths: []string{"-"}},
		{name: "duplicate", paths: []string{"-", "-"}, fail: true},
		{name: "canceled", paths: []string{"-"}, cancel: true, fail: true},
		{name: "file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			input := &countedInput{reader: strings.NewReader(externalCM)}
			paths := tc.paths
			if paths == nil {
				paths = []string{substitutionInput(t, externalCM)}
			}
			c := commonFlags{supply: paths}
			err := c.loadInputs(ctx, input)
			if (err != nil) != tc.fail {
				t.Fatalf("load: %v", err)
			}
			want := 0
			if tc.name == "single" {
				want = len(externalCM)
			}
			if input.bytes != want {
				t.Fatalf("stdin bytes=%d, want %d", input.bytes, want)
			}
			if !tc.fail && len(c.suppliedObjects) != 1 {
				t.Fatalf("objects=%d", len(c.suppliedObjects))
			}
		})
	}
}

func TestLoadInputs_AnyKind(t *testing.T) {
	t.Parallel()
	body := externalCM + "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: r" +
		"emote, namespace: apps}\nspec: {url: 'file:///fixture'}\n---\napiVersion: apiextensio" +
		"ns.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: widgets.example.io}\ns" +
		"pec: {group: example.io}\n---\napiVersion: example.io/v1\nkind: Widget\nmetadata: {na" +
		"me: custom, namespace: apps}\nspec: {payload: retained}\n"
	cipher := "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	body += "---\napiVersion: v1\nkind: Secret\nmetadata: {name: auth, namespace: apps}\nstringDat" +
		"a: {VALUE: '" + cipher + "'}\nsops: {encrypted_regex: stringData}\n"
	c := commonFlags{supply: []string{"-"}}
	if err := c.loadInputs(t.Context(), strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if len(c.suppliedObjects) != 5 {
		t.Fatalf("objects=%d", len(c.suppliedObjects))
	}
	if c.suppliedObjects[3].Doc["spec"].(map[string]any)["payload"] != "retained" {
		t.Fatal("raw payload lost")
	}
	value, err := c.suppliedObjects[4].Obj.(manifest.Data).Value("VALUE")
	got := value.Value
	if err != nil || got != cipher {
		t.Fatalf("supplied ciphertext changed: %q, %v", got, err)
	}
}

func testLoadInputsDevStdin(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/dev/stdin"); err != nil {
		t.Skip("/dev/stdin unavailable")
	}
	if os.Getenv("FLATE_TEST_DEV_STDIN") != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
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
	if len(c.suppliedObjects) != 1 || input.bytes != 0 {
		t.Fatal("ordinary stdin path used the injected reader")
	}
}

func testRunSupplyStdin(t *testing.T) {
	t.Helper()
	root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}")
	input := &countedInput{reader: strings.NewReader(externalCM)}
	var stdout, stderr bytes.Buffer
	cmd := New("test")
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
