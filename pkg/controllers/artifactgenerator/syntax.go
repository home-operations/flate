package artifactgenerator

import (
	"fmt"
	"strings"

	"github.com/home-operations/flate/pkg/source/safepath"
)

const strategyExtract = "Extract"

type sourceSelection struct {
	alias   string
	pattern string
}

func sourcePath(value string) (sourceSelection, error) {
	alias, pattern, ok := strings.Cut(value, "/")
	if !ok || !strings.HasPrefix(alias, "@") || !aliasName.MatchString(strings.TrimPrefix(alias, "@")) {
		return sourceSelection{alias: "", pattern: ""}, fmt.Errorf("invalid source %q", value)
	}
	if _, err := safepath.ArchiveName(pattern); pattern != "" && err != nil {
		return sourceSelection{alias: "", pattern: ""}, err
	}
	return sourceSelection{alias: alias[1:], pattern: pattern}, nil
}

func destinationPath(value string) (string, error) {
	destination, ok := strings.CutPrefix(value, "@artifact/")
	if !ok {
		return "", fmt.Errorf("invalid destination %q", value)
	}
	if destination == "" {
		return ".", nil
	}
	_, err := safepath.ArchiveName(destination)
	return destination, err
}
