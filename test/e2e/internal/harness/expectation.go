package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/home-operations/flate/internal/cli"
	"github.com/home-operations/flate/internal/engine/diff"
	"github.com/home-operations/flate/internal/engine/helm"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/testfixture"
)

const (
	concurrencyToken = "<CONCURRENCY_DEFAULT>"
	kubeVersionToken = "<KUBE_VERSION_DEFAULT>"
	chromaPrefix     = "<CHROMA_"
	chromaCSSToken   = "CSS>"
	chromaLineToken  = "LINE:"
	chromaLineFields = 3
)

func readExpectation(t *testing.T, fixture *testfixture.Fixture, path, cache string) string {
	t.Helper()
	stream, err := resolveDependencyTokens(path, fixture.Read(t, path, cache))
	assert.NoError(t, err)
	return stream
}

func resolveDependencyTokens(path, stream string) (string, error) {
	if strings.Contains(stream, concurrencyToken) {
		stream = strings.ReplaceAll(stream, concurrencyToken, strconv.Itoa(cli.DefaultConcurrency()))
	}
	if strings.Contains(stream, kubeVersionToken) {
		stream = strings.ReplaceAll(stream, kubeVersionToken, helm.BundledKubeVersion())
	}
	var highlighter *diff.Highlighter
	var css string
	var out strings.Builder
	for {
		before, after, found := strings.Cut(stream, chromaPrefix)
		if !found {
			if out.Len() == 0 {
				return before, nil
			}
			out.WriteString(before)
			return out.String(), nil
		}
		out.WriteString(before)
		var token string
		switch {
		case strings.HasPrefix(after, chromaCSSToken):
			token = chromaCSSToken
		case strings.HasPrefix(after, chromaLineToken):
			token = chromaLineToken
		default:
			out.WriteString(chromaPrefix)
			stream = after
			continue
		}
		if highlighter == nil {
			var err error
			highlighter, css, err = diff.NewHighlighter()
			if err != nil {
				return "", fmt.Errorf("%s: %w", path, err)
			}
		}
		stream = after[len(token):]
		if token == chromaCSSToken {
			out.WriteString(css)
			continue
		}
		rest, err := writeChromaLine(&out, highlighter, stream)
		if err != nil {
			return "", fmt.Errorf("%s: <CHROMA_LINE: %w", path, err)
		}
		stream = rest
	}
}

func writeChromaLine(out *strings.Builder, highlighter *diff.Highlighter, token string) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(token))
	decoder.UseNumber()
	var tuple []any
	if err := decoder.Decode(&tuple); err != nil {
		return "", fmt.Errorf("decode tuple: %w", err)
	}
	rest, closed := strings.CutPrefix(token[decoder.InputOffset():], ">")
	if !closed {
		return "", errors.New("missing closing >")
	}
	if len(tuple) != chromaLineFields {
		return "", errors.New("expected [start, end, line]")
	}
	start, startOK := tuple[0].(json.Number)
	end, endOK := tuple[1].(json.Number)
	line, lineOK := tuple[2].(string)
	if !startOK || !endOK || !lineOK {
		return "", errors.New("expected two rune offsets and a string")
	}
	if strings.ContainsRune(line, '\n') {
		return "", errors.New("expected a single line")
	}
	const invalidRange = "invalid rune range [%s,%s)"
	lo, loErr := strconv.Atoi(string(start))
	hi, hiErr := strconv.Atoi(string(end))
	if loErr != nil || hiErr != nil {
		return "", fmt.Errorf(invalidRange, start, end)
	}
	if lo < 0 || hi < lo || hi > utf8.RuneCountInString(line) {
		return "", fmt.Errorf(invalidRange, start, end)
	}
	out.WriteString(string(highlighter.Emit(line, lo, hi)))
	return rest, nil
}
