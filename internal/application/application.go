// Package application screens content for possible entry applications.
package application

import "bytes"

// Contains MUST retain producer kinds when templates construct their API groups.
func Contains(content []byte) bool {
	if bytes.Contains(content, []byte("kustomize.toolkit.fluxcd.io")) ||
		bytes.Contains(content, []byte("fluxcd.controlplane.io")) ||
		bytes.Contains(content, []byte("helm.toolkit.fluxcd.io")) {
		return true
	}
	for {
		index := bytes.Index(content, []byte("kind"))
		if index < 0 {
			return false
		}
		quotedKey := index > 0 && isQuote(content[index-1])
		content = content[index+len("kind"):]
		field := content
		if quotedKey && len(field) > 0 && isQuote(field[0]) {
			field = field[1:]
		}
		if producerField(field) {
			return true
		}
	}
}

func producerField(field []byte) bool {
	// RE2 whitespace excludes vertical tabs and Unicode spaces.
	field = bytes.TrimLeft(field, "\t\n\f\r ")
	if len(field) == 0 || field[0] != ':' {
		return false
	}
	value := bytes.TrimLeft(field[1:], "\t\n\f\r ")
	if len(value) > 0 && isQuote(value[0]) {
		value = value[1:]
	}
	return producerValue(value)
}

func producerValue(value []byte) bool {
	var suffix []byte
	switch {
	case bytes.HasPrefix(value, []byte("{{")), bytes.HasPrefix(value, []byte("<<")):
		return true
	case bytes.HasPrefix(value, []byte("Kustomization")):
		suffix = value[len("Kustomization"):]
	case bytes.HasPrefix(value, []byte("FluxInstance")):
		suffix = value[len("FluxInstance"):]
	case bytes.HasPrefix(value, []byte("HelmRelease")):
		suffix = value[len("HelmRelease"):]
	case bytes.HasPrefix(value, []byte("ResourceSet")):
		suffix = value[len("ResourceSet"):]
	default:
		return false
	}
	return len(suffix) == 0 || (suffix[0] < 'A' || suffix[0] > 'Z') &&
		(suffix[0] < 'a' || suffix[0] > 'z') && (suffix[0] < '0' || suffix[0] > '9') && suffix[0] != '_'
}

func isQuote(value byte) bool {
	return value == '\'' || value == '"'
}
