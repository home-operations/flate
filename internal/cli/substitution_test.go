package cli

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func substitutionFixture(t *testing.T, postBuild, expression string) string {
	t.Helper()
	root := t.TempDir()
	if _, err := git.PlainInit(root, false); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "flux.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  interval: 1m\n  path: ./app\n"+postBuild)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata:\n  value: '"+expression+"'\n")
	return root
}

func substitutionInput(t *testing.T, body string) string {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "values.yaml", body)
	return filepath.Join(root, "values.yaml")
}

func substitutionReference(kind string, optional bool) string {
	return fmt.Sprintf("  postBuild:\n    substituteFrom:\n      - kind: %s\n        name: external\n        optional: %v\n", kind, optional)
}

func substitutionOutput(t *testing.T, output string) string {
	t.Helper()
	docs, err := manifest.SplitDocs([]byte(output))
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected only rendered ConfigMap, got %d documents: %s", len(docs), output)
	}
	name, namespace := manifest.DocMetadata(docs[0])
	if manifest.DocKind(docs[0]) != manifest.KindConfigMap || name != "rendered" || namespace != "apps" {
		t.Fatalf("unexpected emitted input: %v", docs[0])
	}
	value, _ := docs[0]["data"].(map[string]any)["value"].(string)
	return value
}

const externalCM = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: external, namespace: apps}\ndata: {VALUE: supplied}\n"
const externalSecret = "apiVersion: v1\nkind: Secret\nmetadata: {name: external, namespace: apps}\nstringData: {VALUE: supplied}\n"

func TestRun_ExternalSubstitution(t *testing.T) {
	cases := []struct {
		name, postBuild, input, want string
		pairs                        []string
		fail, strict                 bool
	}{
		{name: "required absent", postBuild: substitutionReference("ConfigMap", false), fail: true},
		{name: "required supplied", postBuild: substitutionReference("ConfigMap", false), input: externalCM, want: "supplied", strict: true},
		{name: "wrong kind", postBuild: substitutionReference("ConfigMap", false), input: externalSecret, fail: true},
		{name: "wrong namespace", postBuild: substitutionReference("ConfigMap", false), input: strings.Replace(externalCM, "namespace: apps", "namespace: other", 1), fail: true},
		{name: "wrong name", postBuild: substitutionReference("ConfigMap", false), input: strings.Replace(externalCM, "name: external", "name: other", 1), fail: true},
		{name: "overlay cannot satisfy reference", postBuild: substitutionReference("ConfigMap", false), pairs: []string{"VALUE=overlay"}, fail: true},
		{name: "optional absent", postBuild: substitutionReference("ConfigMap", true)},
		{name: "optional supplied", postBuild: substitutionReference("ConfigMap", true), input: externalCM, want: "supplied"},
		{name: "secret stringData", postBuild: substitutionReference("Secret", false), input: externalSecret, want: "supplied", strict: true},
		{name: "secret data", postBuild: substitutionReference("Secret", false), input: strings.Replace(externalSecret, "stringData: {VALUE: supplied}", "data: {VALUE: c3VwcGxpZWQ=}", 1), want: "supplied"},
		{name: "secret ciphertext unchanged", postBuild: substitutionReference("Secret", false), input: strings.Replace(externalSecret, "supplied", "'ENC[AES256_GCM,data:fixture]'", 1) + "sops: {encrypted_regex: data}\n", want: "ENC[AES256_GCM,data:fixture]"},
		{name: "missing unsupplied secret", postBuild: substitutionReference("Secret", false)},
		{name: "inline beats reference", postBuild: substitutionReference("ConfigMap", false) + "    substitute: {VALUE: inline}\n", input: externalCM, want: "inline", strict: true},
		{name: "overlay beats inline and reference", postBuild: substitutionReference("ConfigMap", false) + "    substitute: {VALUE: inline}\n", input: externalCM, pairs: []string{"VALUE=first", "VALUE=overlay"}, want: "overlay", strict: true},
		{name: "overlay with empty postBuild", postBuild: "  postBuild: {}\n", pairs: []string{"VALUE=overlay"}, want: "overlay", strict: true},
		{name: "empty overlay value", postBuild: "  postBuild: {}\n", pairs: []string{"VALUE="}, strict: true},
		{name: "embedded equals and comma", postBuild: "  postBuild: {}\n", pairs: []string{"VALUE=left,right=tail"}, want: "left,right=tail"},
		{name: "overlay without postBuild", pairs: []string{"VALUE=overlay"}, want: "${VALUE}", strict: true},
		{name: "later reference wins", postBuild: substitutionReference("ConfigMap", false) + "      - kind: Secret\n        name: external\n", input: externalCM + "---\n" + strings.Replace(externalSecret, "supplied", "second", 1), want: "second", strict: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := substitutionFixture(t, tc.postBuild, "${VALUE}")
			var first string
			for _, workers := range []int{2, 4} {
				args := []string{"build", "all", "--path", root, "--concurrency", fmt.Sprint(workers), "--cache-dir", t.TempDir(), "--skip-secrets=false"}
				if tc.input != "" {
					args = append(args, "--substitute-from", substitutionInput(t, tc.input))
				}
				for _, pair := range tc.pairs {
					args = append(args, "--substitute", pair)
				}
				if tc.strict {
					args = append(args, "--strict-substitutions")
				}
				out, diagnostic, code := runCLI(t, args...)
				if (code != 0) != tc.fail {
					t.Fatalf("workers=%d exit=%d: %s", workers, code, diagnostic)
				}
				if tc.fail {
					if !strings.Contains(diagnostic, "ConfigMap") || !strings.Contains(diagnostic, "apps/external") {
						t.Fatalf("missing reference diagnostic: %s", diagnostic)
					}
					continue
				}
				if got := substitutionOutput(t, out); got != tc.want {
					t.Fatalf("got %q, want %q", got, tc.want)
				}
				if workers == 2 {
					first = out
				} else if out != first {
					t.Fatal("output varies with concurrency")
				}
			}
		})
	}
}

func TestLoadSubstitutions_InputValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		ordinal    int
	}{
		{"scalar", "private-fixture-value\n", 1},
		{"sequence", "[private-fixture-value]\n", 1},
		{"ordinal after null", "---\nnull\n---\n\n---\n[private-fixture-value]\n", 3},
		{"decoder error", "data: [private-fixture-value\n", 1},
		{"duplicate YAML key", "data: {VALUE: private-fixture-value, VALUE: another}\n", 1},
		{"empty mapping", "{}\n", 1},
		{"wrong api", strings.Replace(externalCM, "apiVersion: v1", "apiVersion: example.io/v1", 1), 1},
		{"unsupported kind", strings.Replace(externalCM, "kind: ConfigMap", "kind: List", 1), 1},
		{"invalid data bag", strings.Replace(externalCM, "data: {VALUE: supplied}", "data: [private-fixture-value]", 1), 1},
		{"missing namespace", strings.Replace(externalCM, ", namespace: apps", "", 1), 1},
		{"missing name", strings.Replace(externalCM, "name: external, ", "", 1), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := substitutionInput(t, tc.body)
			c := commonFlags{substituteFrom: []string{path}}
			err := c.loadSubstitutions(t.Context())
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("expected input error: %v", err)
			}
			for _, text := range []string{path, fmt.Sprintf("document %d", tc.ordinal)} {
				if !strings.Contains(err.Error(), text) {
					t.Fatalf("missing %q in %v", text, err)
				}
			}
			if strings.Contains(err.Error(), "private-fixture-value") {
				t.Fatal("input value leaked in error")
			}
		})
	}
	t.Run("unreadable", func(t *testing.T) {
		c := commonFlags{substituteFrom: []string{filepath.Join(t.TempDir(), "absent.yaml")}}
		if err := c.loadSubstitutions(t.Context()); !errors.Is(err, manifest.ErrInput) {
			t.Fatalf("expected input error: %v", err)
		}
	})
	t.Run("empty and null", func(t *testing.T) {
		c := commonFlags{substituteFrom: []string{substitutionInput(t, "---\nnull\n---\n\n")}}
		if err := c.loadSubstitutions(t.Context()); err != nil || len(c.substitutionSources) != 0 {
			t.Fatalf("empty documents: %v", err)
		}
	})
}

func TestRun_SubstitutionFilesAndDuplicates(t *testing.T) {
	root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}-${OLD:-gone}")
	cwd := t.TempDir()
	t.Chdir(cwd)
	testutil.WriteFile(t, cwd, "first,values.yaml", strings.Replace(externalCM, "data: {VALUE: supplied}", "data: {VALUE: first, OLD: old}", 1))
	testutil.WriteFile(t, cwd, "last.yaml", "---\nnull\n---\n"+strings.Replace(externalCM, "supplied", "second", 1)+"---\n"+strings.Replace(externalCM, "supplied", "last", 1))
	out, diagnostic, code := runCLI(t, "build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "4", "--substitute-from", "first,values.yaml", "--substitute-from", "last.yaml")
	if code != 0 {
		t.Fatal(diagnostic)
	}
	if got := substitutionOutput(t, out); got != "last-gone" {
		t.Fatalf("duplicate objects merged or ordered incorrectly: %q", got)
	}
}

func TestRun_SubstitutionEnvironment(t *testing.T) {
	input := substitutionInput(t, externalCM)
	second := substitutionInput(t, strings.Replace(externalCM, "supplied", "last", 1))
	for _, tc := range []struct {
		name, files, pairs, strict, want string
		flags                            []string
	}{
		{name: "colon file list", files: input + ":" + second, strict: "true", want: "last"},
		{name: "CSV pairs", files: input, pairs: `"VALUE=left,right=tail",VALUE=final`, strict: "1", want: "final"},
		{name: "quoted CSV value", files: input, pairs: `"VALUE=left,right=tail"`, want: "left,right=tail"},
		{name: "empty env lists", want: "", flags: []string{"--substitute-from", input, "--substitute", "VALUE="}},
		{name: "file flags override invalid env", files: ":", strict: "true", want: "supplied", flags: []string{"--substitute-from", input}},
		{name: "pair flags override invalid env", files: input, pairs: `"private-fixture-value`, want: "flag", flags: []string{"--substitute", "VALUE=flag"}},
		{name: "false flag overrides invalid bool env", files: input, strict: "invalid", want: "supplied", flags: []string{"--strict-substitutions=false"}},
		{name: "false flag overrides true env", files: input, strict: "true", want: "supplied", flags: []string{"--strict-substitutions=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_SUBSTITUTE_FROM", tc.files)
			t.Setenv("FLATE_SUBSTITUTE", tc.pairs)
			t.Setenv("FLATE_STRICT_SUBSTITUTIONS", tc.strict)
			if tc.strict == "" {
				t.Setenv("FLATE_STRICT_SUBSTITUTIONS", "false")
			}
			root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}")
			args := append([]string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2"}, tc.flags...)
			out, diagnostic, code := runCLI(t, args...)
			if code != 0 {
				t.Fatal(diagnostic)
			}
			if got := substitutionOutput(t, out); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRun_SubstitutionDiagnosticsRedacted(t *testing.T) {
	for _, tc := range []struct{ name, env, flag, origin, position string }{
		{"missing equals flag", "", "private-fixture-value", "--substitute", "pair 1"},
		{"invalid name flag", "", "INVALID-NAME=private-fixture-value", "--substitute", "pair 1"},
		{"empty name flag", "", "=private-fixture-value", "--substitute", "pair 1"},
		{"invalid env pair", "VALUE=private-fixture-value,bad", "", "FLATE_SUBSTITUTE", "pair 2"},
		{"malformed CSV", `VALUE=ok,"private-fixture-value`, "", "FLATE_SUBSTITUTE", "pair 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_SUBSTITUTE", tc.env)
			root := substitutionFixture(t, "  postBuild: {}\n", "${VALUE}")
			args := []string{"build", "all", "--path", root, "--cache-dir", t.TempDir()}
			if tc.flag != "" {
				args = append(args, "--substitute", tc.flag)
			}
			_, diagnostic, code := runCLI(t, args...)
			if code != 1 || !strings.Contains(diagnostic, tc.origin) || !strings.Contains(diagnostic, tc.position) || strings.Contains(diagnostic, "private-fixture-value") {
				t.Fatalf("unredacted or incomplete diagnostic: %s", diagnostic)
			}
		})
	}
	for _, input := range []string{":", "file:", ":file"} {
		if _, err := substitutionEnv("substitute-from", input); !errors.Is(err, manifest.ErrInput) {
			t.Fatalf("empty file component accepted: %v", err)
		}
	}
}

func TestRun_StrictSubstitutions(t *testing.T) {
	for _, tc := range []struct {
		name, expression, postBuild, disabled, want string
		strict, fail                                bool
	}{
		{name: "default undefined", expression: "${VALUE}", postBuild: "  postBuild: {}\n"},
		{name: "strict undefined", expression: "${VALUE}", postBuild: "  postBuild: {}\n", strict: true, fail: true},
		{name: "defined empty", expression: "${VALUE}", postBuild: "  postBuild:\n    substitute: {VALUE: ''}\n", strict: true},
		{name: "assignment default", expression: "${VALUE:=fallback}", postBuild: "  postBuild: {}\n", strict: true, want: "fallback"},
		{name: "colon default", expression: "${VALUE:-fallback}", postBuild: "  postBuild: {}\n", strict: true, want: "fallback"},
		{name: "empty with default", expression: "${VALUE:-fallback}", postBuild: "  postBuild:\n    substitute: {VALUE: ''}\n", strict: true, want: "fallback"},
		{name: "escape", expression: "$${VALUE}", postBuild: "  postBuild: {}\n", strict: true, want: "${VALUE}"},
		{name: "annotation disabled", expression: "${VALUE}", postBuild: "  postBuild: {}\n", disabled: "annotations", strict: true, want: "${VALUE}"},
		{name: "label disabled", expression: "${VALUE}", postBuild: "  postBuild: {}\n", disabled: "labels", strict: true, want: "${VALUE}"},
		{name: "no postBuild", expression: "${VALUE}", strict: true, want: "${VALUE}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := substitutionFixture(t, tc.postBuild, tc.expression)
			if tc.disabled != "" {
				testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: rendered\n  namespace: apps\n  "+tc.disabled+": {kustomize.toolkit.fluxcd.io/substitute: disabled}\ndata:\n  value: '"+tc.expression+"'\n")
			}
			args := []string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "4"}
			if tc.strict {
				args = append(args, "--strict-substitutions")
			}
			out, diagnostic, code := runCLI(t, args...)
			if (code != 0) != tc.fail {
				t.Fatalf("exit=%d: %s", code, diagnostic)
			}
			if tc.fail {
				for _, want := range []string{"apps/consumer", "VALUE", "variable not set (strict mode)"} {
					if !strings.Contains(diagnostic, want) {
						t.Fatalf("diagnostic lost %q: %s", want, diagnostic)
					}
				}
			} else if got := substitutionOutput(t, out); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRun_SubstitutionHelpAndSharedCommands(t *testing.T) {
	root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}")
	input := substitutionInput(t, externalCM)
	for _, verb := range []string{"build", "get", "test", "diff"} {
		t.Run(verb, func(t *testing.T) {
			help, diagnostic, code := runCLI(t, verb, "all", "--help")
			if code != 0 {
				t.Fatal(diagnostic)
			}
			for _, flag := range []string{"substitute-from", "substitute", "strict-substitutions"} {
				for _, want := range []string{"--" + flag, envKey(flag)} {
					if !strings.Contains(help, want) {
						t.Fatalf("help missing %s", want)
					}
				}
			}
			args := []string{verb, "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--substitute-from", input, "--substitute", "VALUE=overlay", "--strict-substitutions"}
			if verb == "diff" {
				base := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}-base")
				args = append(args, "--path-orig", base, "-o", "diff", "--skip-secrets=false")
			}
			out, diagnostic, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("shared inputs failed: %s", diagnostic)
			}
			if verb == "diff" && (strings.Contains(out, "name: external") || !strings.Contains(out, "overlay")) {
				t.Fatalf("diff emitted input or omitted selected consumer: %s", out)
			}
		})
	}
}

func TestRun_SubstitutionIsolation(t *testing.T) {
	for _, ref := range []string{"secretRef", "certSecretRef", "proxySecretRef"} {
		t.Run(ref, func(t *testing.T) {
			root := substitutionFixture(t, "", "literal")
			testutil.WriteFile(t, root, "flux.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./app\n  sourceRef: {kind: OCIRepository, name: remote}\n")
			testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: remote, namespace: apps}\nspec:\n  url: oci://example.invalid/fixture\n  "+ref+": {name: external}\n")
			input := substitutionInput(t, externalSecret)
			_, diagnostic, code := runCLI(t, "build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--substitute-from", input)
			if code != 1 || !strings.Contains(diagnostic, "external") || !strings.Contains(diagnostic, "not found") {
				t.Fatalf("source input leaked into %s: %s", ref, diagnostic)
			}
		})
	}
	for _, kind := range []string{"ConfigMap", "Secret"} {
		t.Run("Helm values "+kind, func(t *testing.T) {
			root := substitutionFixture(t, "", "literal")
			testutil.WriteFile(t, root, "hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: chart, namespace: apps}\nspec:\n  interval: 1m\n  chart:\n    spec:\n      chart: charts/app\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  valuesFrom:\n    - kind: "+kind+"\n      name: external\n")
			testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
			testutil.WriteFile(t, root, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-output}\n")
			input := externalCM
			if kind == "Secret" {
				input = externalSecret
			}
			_, diagnostic, code := runCLI(t, "build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "4", "--substitute-from", substitutionInput(t, input))
			if code != 1 || !strings.Contains(diagnostic, "external") {
				t.Fatalf("external %s satisfied Helm values: %s", kind, diagnostic)
			}
		})
	}
}

func TestRun_StrictSubstitutionEnvironmentPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, env      string
		override, fail bool
	}{
		{name: "enabled", env: "true", fail: true},
		{name: "disabled", env: "false"},
		{name: "explicit false", env: "true", override: true},
		{name: "overridden invalid env", env: "invalid", override: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_STRICT_SUBSTITUTIONS", tc.env)
			root := substitutionFixture(t, "  postBuild: {}\n", "${UNDEFINED}")
			args := []string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2"}
			if tc.override {
				args = append(args, "--strict-substitutions=false")
			}
			out, diagnostic, code := runCLI(t, args...)
			if (code != 0) != tc.fail {
				t.Fatalf("exit=%d: %s", code, diagnostic)
			}
			if !tc.fail && substitutionOutput(t, out) != "" {
				t.Fatal("undefined variable did not expand empty")
			}
		})
	}
}

func TestRun_SubstitutionRepositoryCollision(t *testing.T) {
	root := substitutionFixture(t, substitutionReference("ConfigMap", false), "${VALUE}")
	testutil.WriteFile(t, root, "producer.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: producer, namespace: apps}\nspec: {path: ./producer}\n")
	testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [input.yaml]\n")
	testutil.WriteFile(t, root, "producer/input.yaml", strings.Replace(externalCM, "supplied", "repository", 1))
	input := substitutionInput(t, strings.Replace(externalCM, "supplied", "private-fixture-value", 1))
	out, diagnostic, code := runCLI(t, "build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "4", "--substitute-from", input, "--strict-substitutions", "--log-level", "warn")
	if code != 0 {
		t.Fatal(diagnostic)
	}
	if strings.Count(diagnostic, "repository substitution source takes precedence") != 1 || !strings.Contains(diagnostic, input) || !strings.Contains(diagnostic, "ConfigMap/apps/external") || strings.Contains(diagnostic, "private-fixture-value") {
		t.Fatalf("collision diagnostic: %s", diagnostic)
	}
	if strings.Contains(out, "private-fixture-value") {
		t.Fatal("supplied collision values emitted")
	}
	docs, err := manifest.SplitDocs([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range docs {
		name, _ := manifest.DocMetadata(doc)
		if name == "rendered" && doc["data"].(map[string]any)["value"] != "repository" {
			t.Fatalf("repository values lost: %v", doc)
		}
	}
}

func TestRun_SubstitutionSecretReadableAdvisory(t *testing.T) {
	root := substitutionFixture(t, substitutionReference("Secret", false), "${VALUE}")
	out, diagnostic, code := runCLI(t, "test", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--substitute-from", substitutionInput(t, externalSecret), "--strict-substitutions")
	if code != 0 || strings.Contains(out+diagnostic, "could not be read offline") {
		t.Fatalf("supplied Secret reported unreadable: %s %s", out, diagnostic)
	}
}

func TestRun_StrictSubstitutionCachedRender(t *testing.T) {
	root := substitutionFixture(t, "  postBuild: {}\n", "${VALUE}")
	cache := t.TempDir()
	args := []string{"build", "all", "--path", root, "--cache-dir", cache, "--concurrency", "2"}
	first, diagnostic, code := runCLI(t, args...)
	if code != 0 {
		t.Fatal(diagnostic)
	}
	_, diagnostic, code = runCLI(t, append(slices.Clone(args), "--strict-substitutions")...)
	if code != 1 || !strings.Contains(diagnostic, "variable not set (strict mode)") {
		t.Fatalf("cached raw output bypassed strictness: %s", diagnostic)
	}
	last, diagnostic, code := runCLI(t, args...)
	if code != 0 || first != last {
		t.Fatalf("strict run changed cached default output: %s", diagnostic)
	}
}
