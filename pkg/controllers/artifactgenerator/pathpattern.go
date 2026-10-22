package artifactgenerator

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
)

type request struct {
	Output   output
	Captures map[string]string
}

var captureName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func expand(ctx context.Context, spec specification, inputs map[string]fs.FS) ([]request, error) {
	var matcher *regexp.Regexp
	var input fs.FS
	if spec.PathPattern != "" {
		alias, pattern, err := sourcePath(spec.PathPattern)
		if err != nil {
			return nil, err
		}
		input = inputs[alias]
		if input == nil {
			return nil, fmt.Errorf("unknown pathPattern source %q", alias)
		}
		var expression strings.Builder
		expression.WriteByte('^')
		seen := map[string]bool{}
		for pattern != "" {
			i := strings.IndexAny(pattern, "{}")
			if i < 0 {
				expression.WriteString(regexp.QuoteMeta(pattern))
				break
			}
			expression.WriteString(regexp.QuoteMeta(pattern[:i]))
			name, rest, ok := strings.Cut(pattern[i+1:], "}")
			if pattern[i] != '{' || !ok || !captureName.MatchString(name) || seen[name] || len(content.IsLabelKey(name)) != 0 {
				return nil, fmt.Errorf("invalid or repeated pathPattern capture %q", name)
			}
			seen[name] = true
			expression.WriteString("(?P<" + name + ">[^/]+)")
			pattern = rest
		}
		expression.WriteByte('$')
		matcher, err = regexp.Compile(expression.String())
		if err != nil {
			return nil, err
		}
	}
	var requests []request
	names := map[string]bool{}
	add := func(raw map[string]string) error {
		normalized := make(map[string]string, len(raw))
		for k, v := range raw {
			normalized[k] = strings.ToLower(v)
			if len(content.IsLabelValue(normalized[k])) != 0 {
				return fmt.Errorf("invalid capture value %q", v)
			}
		}
		for _, artifact := range spec.Artifacts {
			var err error
			artifact.Name, err = render(artifact.Name, normalized)
			if err != nil {
				return err
			}
			if len(apivalidation.NameIsDNSSubdomain(artifact.Name, false)) != 0 || names[artifact.Name] {
				return fmt.Errorf("invalid or duplicate output name %q", artifact.Name)
			}
			names[artifact.Name] = true
			artifact.Copy = append([]operation(nil), artifact.Copy...)
			for i := range artifact.Copy {
				artifact.Copy[i].From, err = render(artifact.Copy[i].From, raw)
				if err != nil {
					return err
				}
				artifact.Copy[i].To, err = render(artifact.Copy[i].To, raw)
				if err != nil {
					return err
				}
			}
			requests = append(requests, request{artifact, maps.Clone(normalized)})
		}
		return nil
	}
	if matcher == nil {
		err := add(nil)
		return requests, err
	}
	err := fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." || !entry.IsDir() {
			return nil
		}
		matches := matcher.FindStringSubmatch(name)
		if matches == nil {
			return nil
		}
		captures := map[string]string{}
		for i, key := range matcher.SubexpNames() {
			if i != 0 {
				captures[key] = matches[i]
			}
		}
		return add(captures)
	})
	return requests, err
}

func render(template string, captures map[string]string) (string, error) {
	var out strings.Builder
	for template != "" {
		i := strings.IndexAny(template, "{}")
		if i < 0 {
			out.WriteString(template)
			break
		}
		out.WriteString(template[:i])
		name, rest, ok := strings.Cut(template[i+1:], "}")
		value, exists := captures[name]
		if template[i] != '{' || !ok || !exists {
			return "", fmt.Errorf("undefined template capture %q", name)
		}
		out.WriteString(value)
		template = rest
	}
	return out.String(), nil
}
