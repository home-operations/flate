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
				indent, _, ok := schemaDiagnostic(lines[i+1])
				values = ok && indent == 0
			}
		}
		if indent, _, ok := schemaDiagnostic(line); ok && values {
			forest, consumed := schemaForest(lines[i:], indent)
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

func schemaDiagnostic(line string) (int, string, bool) {
	trimmed := strings.TrimLeft(line, " ")
	indent := len(line) - len(trimmed)
	location, ok := strings.CutPrefix(trimmed, "- at ")
	if !ok || indent%2 != 0 {
		return 0, "", false
	}
	_, rest, ok := schemaQuoted(location)
	if !ok {
		return 0, "", false
	}
	message, ok := strings.CutPrefix(rest, ": ")
	return indent, message, ok
}

func schemaForest(lines []string, indent int) (string, int) {
	var blocks []string
	i := 0
	for i < len(lines) {
		depth, message, ok := schemaDiagnostic(lines[i])
		if !ok || depth != indent {
			break
		}
		block := []string{lines[i][:len(lines[i])-len(message)] + schemaProperties(message)}
		i++
		for i < len(lines) {
			depth, _, diagnostic := schemaDiagnostic(lines[i])
			if diagnostic {
				if depth <= indent {
					break
				}
				children, consumed := schemaForest(lines[i:], depth)
				block = append(block, children)
				i += consumed
			} else if len(lines[i])-len(strings.TrimLeft(lines[i], " ")) > indent {
				block = append(block, lines[i])
				i++
			} else {
				break
			}
		}
		blocks = append(blocks, strings.Join(block, "\n"))
	}
	slices.Sort(blocks)
	return strings.Join(blocks, "\n"), i
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
		token, tail, ok := schemaQuoted(rest)
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
		_, tail, ok := schemaQuoted(trigger)
		if !ok || tail != " exists" {
			return message
		}
	}
	slices.Sort(tokens)
	return prefix + strings.Join(tokens, ", ") + rest + message[len(core):]
}

func schemaQuoted(text string) (string, string, bool) {
	if !strings.HasPrefix(text, "'") {
		return "", "", false
	}
	for rest := text[1:]; rest != ""; {
		if rest[0] == '\'' {
			end := len(text) - len(rest) + 1
			return text[:end], text[end:], true
		}
		_, _, tail, err := strconv.UnquoteChar(rest, '\'')
		if err != nil {
			return "", "", false
		}
		rest = tail
	}
	return "", "", false
}
