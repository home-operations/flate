package helm

import (
	"slices"
	"strconv"
	"strings"
)

type schemaDisplayError struct {
	text  string
	cause error
}

func (e *schemaDisplayError) Error() string { return e.text }

func (e *schemaDisplayError) Unwrap() error { return e.cause }

func schemaError(err error) error {
	// Helm erases structured validation causes before the action returns.
	text := err.Error()
	if !strings.HasPrefix(text, "values don't meet the specifications of the schema(s) in the following chart(s):\n") {
		return err
	}
	lines := strings.Split(text, "\n")
	var out []string
	values := false
	for i := 0; i < len(lines); {
		line := lines[i]
		if strings.HasSuffix(line, ":") {
			values = false
			if i+1 < len(lines) {
				schemaDiagnosticResult1 := schemaDiagnostic(lines[i+1])
				indent, _, ok := schemaDiagnosticResult1.indent, schemaDiagnosticResult1.message, schemaDiagnosticResult1.valid
				values = ok && indent == 0
			}
		}
		schemaDiagnosticResult2 := schemaDiagnostic(line)
		if indent, _, ok := schemaDiagnosticResult2.indent, schemaDiagnosticResult2.message, schemaDiagnosticResult2.valid; ok &&
			values {
			schemaForestResult3 := schemaForest(lines[i:], indent)
			forest, consumed := schemaForestResult3.text, schemaForestResult3.consumed
			out = append(out, forest)
			i += consumed
		} else {
			out = append(out, line)
			i++
		}
	}
	canonical := strings.Join(out, "\n")
	if canonical == text {
		return err
	}
	return &schemaDisplayError{text: canonical, cause: err}
}

type diagnosticLine struct {
	indent  int
	message string
	valid   bool
}

func schemaDiagnostic(line string) diagnosticLine {
	trimmed := strings.TrimLeft(line, " ")
	indent := len(line) - len(trimmed)
	location, ok := strings.CutPrefix(trimmed, "- at ")
	if !ok || indent%2 != 0 {
		return diagnosticLine{indent: 0, message: "", valid: false}
	}
	schemaQuotedResult4 := schemaQuoted(location)
	_, rest, ok := schemaQuotedResult4.value, schemaQuotedResult4.rest, schemaQuotedResult4.valid
	if !ok {
		return diagnosticLine{indent: 0, message: "", valid: false}
	}
	message, ok := strings.CutPrefix(rest, ": ")
	return diagnosticLine{indent: indent, message: message, valid: ok}
}

type diagnosticForest struct {
	text     string
	consumed int
}

func schemaForest(lines []string, indent int) diagnosticForest {
	var blocks []string
	i := 0
	for i < len(lines) {
		schemaDiagnosticResult5 := schemaDiagnostic(lines[i])
		depth, message, ok := schemaDiagnosticResult5.indent, schemaDiagnosticResult5.message, schemaDiagnosticResult5.valid
		if !ok || depth != indent {
			break
		}
		block := []string{lines[i][:len(lines[i])-len(message)] + schemaProperties(message)}
		i++
	diagnostics:
		for i < len(lines) {
			schemaDiagnosticResult6 := schemaDiagnostic(lines[i])
			depth, _, diagnostic := schemaDiagnosticResult6.indent, schemaDiagnosticResult6.message, schemaDiagnosticResult6.valid
			switch {
			case diagnostic:
				if depth <= indent {
					break diagnostics
				}
				schemaForestResult7 := schemaForest(lines[i:], depth)
				children, consumed := schemaForestResult7.text, schemaForestResult7.consumed
				block = append(block, children)
				i += consumed
			case len(lines[i])-len(strings.TrimLeft(lines[i], " ")) > indent:
				block = append(block, lines[i])
				i++
			default:
				break diagnostics
			}
		}
		blocks = append(blocks, strings.Join(block, "\n"))
	}
	slices.Sort(blocks)
	return diagnosticForest{text: strings.Join(blocks, "\n"), consumed: i}
}

func schemaProperties(message string) string {
	core := strings.TrimRight(message, " \t\r")
	var prefix string
	switch {
	case strings.HasPrefix(core, "missing properties "):
		prefix = "missing properties "
	case strings.HasPrefix(core, "additional properties "):
		prefix = "additional properties "
	case strings.HasPrefix(core, "properties "):
		prefix = "properties "
	default:
		return message
	}
	rest := core[len(prefix):]
	var tokens []string
	for {
		schemaQuotedResult8 := schemaQuoted(rest)
		token, tail, ok := schemaQuotedResult8.value, schemaQuotedResult8.rest, schemaQuotedResult8.valid
		if !ok {
			return message
		}
		tokens = append(tokens, token)
		rest = tail
		if next, ok := strings.CutPrefix(rest, ", "); ok {
			rest = next
			continue
		}
		break
	}
	switch prefix {
	case "missing properties ":
		if rest != "" {
			return message
		}
	case "additional properties ":
		if rest != " not allowed" {
			return message
		}
	case "properties ":
		trigger, ok := strings.CutPrefix(rest, " required, if ")
		if !ok {
			return message
		}
		schemaQuotedResult9 := schemaQuoted(trigger)
		_, tail, ok := schemaQuotedResult9.value, schemaQuotedResult9.rest, schemaQuotedResult9.valid
		if !ok || tail != " exists" {
			return message
		}
	}
	slices.Sort(tokens)
	return prefix + strings.Join(tokens, ", ") + rest + message[len(core):]
}

type quotedText struct {
	value string
	rest  string
	valid bool
}

func schemaQuoted(text string) quotedText {
	if !strings.HasPrefix(text, "'") {
		return quotedText{value: "", rest: "", valid: false}
	}
	for rest := text[1:]; rest != ""; {
		if rest[0] == '\'' {
			end := len(text) - len(rest) + 1
			return quotedText{value: text[:end], rest: text[end:], valid: true}
		}
		_, _, tail, err := strconv.UnquoteChar(rest, '\'')
		if err != nil {
			return quotedText{value: "", rest: "", valid: false}
		}
		rest = tail
	}
	return quotedText{value: "", rest: "", valid: false}
}
