package artifactgenerator

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/source/safepath"
)

const strategyExtract = "Extract"

type sourceSelection struct {
	alias   string
	pattern string
}

func sourcePath(value string) (sourceSelection, error) {
	selector, marked := strings.CutPrefix(value, "@")
	if !marked {
		return sourceSelection{}, fmt.Errorf("source must start with '@'")
	}
	alias, pattern, ok := strings.Cut(selector, "/")
	if !ok {
		return sourceSelection{}, fmt.Errorf("source format must be '@alias/pattern'")
	}
	if _, err := safepath.ArchiveName(pattern); pattern != "" && err != nil {
		return sourceSelection{alias: "", pattern: ""}, err
	}
	return sourceSelection{alias: alias, pattern: pattern}, nil
}

func destinationPath(value string) (string, error) {
	destination, ok := strings.CutPrefix(value, "@artifact/")
	if !ok {
		return "", fmt.Errorf("destination must start with '@artifact/'")
	}
	if destination == "" {
		return ".", nil
	}
	if !filepath.IsLocal(destination) {
		return "", fmt.Errorf("destination path must stay within the artifact root")
	}
	if slices.Contains(strings.Split(filepath.ToSlash(destination), "/"), "..") {
		return "", fmt.Errorf("destination path must not contain '..'")
	}
	_, err := safepath.ArchiveName(destination)
	return destination, err
}
