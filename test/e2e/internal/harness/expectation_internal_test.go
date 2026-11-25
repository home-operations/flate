package harness

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/engine/diff"
	"github.com/home-operations/flate/internal/engine/helm"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestResolveDependencyTokens_Expansion(t *testing.T) {
	t.Parallel()
	highlighter, css, err := diff.NewHighlighter()
	assert.NoError(t, err)
	version := helm.BundledKubeVersion()
	for _, tc := range []struct{ name, stream, want string }{
		{
			name:   "default and override",
			stream: "kube: v<KUBE_VERSION_DEFAULT>\n(default \"<KUBE_VERSION_DEFAULT>\")\nexplicit: v1.30.2\n",
			want:   "kube: v" + version + "\n(default \"" + version + "\")\nexplicit: v1.30.2\n",
		},
		{
			name:   "stylesheet",
			stream: "<style>\n<CHROMA_CSS>\n</style>\n",
			want:   "<style>\n" + css + "\n</style>\n",
		},
		{
			name:   "consecutive line tokens",
			stream: "<CHROMA_LINE:[0,0,\"  kube: v<KUBE_VERSION_DEFAULT>\"]><CHROMA_LINE:[8,9,\"  é: \\\"<x>\\\"\"]>\n",
			want: string(highlighter.Emit("  kube: v"+version, 0, 0)) +
				string(highlighter.Emit("  é: \"<x>\"", 8, 9)) + "\n",
		},
		{
			name:   "unknown tokens",
			stream: "<CHROMA_OTHER><other>\n<CHROMA_CSS>",
			want:   "<CHROMA_OTHER><other>\n" + css,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveDependencyTokens("expect/example.stdout", tc.stream)
			assert.NoError(t, err)
			assert.Diff(t, got, tc.want)
		})
	}
}

func TestResolveDependencyTokens_MalformedLine(t *testing.T) {
	t.Parallel()
	const path = "expect/malformed.stderr"
	for _, tc := range []struct{ name, token, message string }{
		{name: "truncated", token: "[0,0,\"x\"", message: "decode tuple"},
		{name: "missing delimiter", token: "[0,0,\"x\"]", message: "missing closing >"},
		{name: "extra field", token: "[0,0,\"x\",0]>", message: "expected [start, end, line]"},
		{name: "wrong type", token: "[0,0,null]>", message: "expected two rune offsets and a string"},
		{name: "fractional start", token: "[0.5,0,\"x\"]>", message: "invalid rune range"},
		{name: "fractional end", token: "[0,0.5,\"x\"]>", message: "invalid rune range"},
		{name: "trailing newline", token: "[1,2,\"x\\n\"]>", message: "expected a single line"},
		{name: "negative", token: "[-1,0,\"x\"]>", message: "invalid rune range"},
		{name: "reversed", token: "[1,0,\"x\"]>", message: "invalid rune range"},
		{name: "past rune count", token: "[0,2,\"é\"]>", message: "invalid rune range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := resolveDependencyTokens(path, "before <CHROMA_LINE:"+tc.token)
			want := path + ": <CHROMA_LINE: " + tc.message
			assert.FatalIf(
				t,
				err == nil || !strings.HasPrefix(err.Error(), want),
				"want error starting %q, got %v",
				want,
				err,
			)
		})
	}
}
