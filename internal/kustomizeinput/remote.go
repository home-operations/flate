// Copyright 2019 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package kustomizeinput

import (
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// GitBase carries the effective repository, root and command policy of a remote operand.
type GitBase struct {
	RepoURL    string
	SubPath    string
	Ref        string
	Timeout    time.Duration
	Submodules bool
}

var gitUsername = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9-]*)@`)

// ParseGitBase follows kustomize/api v0.21.2 internal/git.NewRepoSpecFromURL;
// Go's internal visibility requires adapting its grammar at this seam.
func ParseGitBase(raw string) (GitBase, bool) {
	if filepath.IsAbs(raw) {
		return GitBase{}, false
	}
	n, query, _ := strings.Cut(raw, "?")
	ref, timeout, submodules := parseQuery(query)
	n, _ = trimPrefixFold(n, "git::")
	var scheme string
	for _, prefix := range []string{"ssh://", "https://", "http://", "file://"} {
		if rest, ok := trimPrefixFold(n, prefix); ok {
			scheme, n = prefix, rest
			break
		}
	}
	username := gitUsername.FindString(n)
	n = strings.TrimPrefix(n, username)
	lower := strings.ToLower(n)
	github := strings.HasPrefix(lower, "github.com/") || strings.HasPrefix(lower, "github.com:")
	scp := scheme == "" && (username != "" || github)
	if scheme == "" && !scp {
		return GitBase{}, false
	}
	var host, repoPath string
	if scheme == "file://" {
		host, repoPath = scheme, username+n
	} else {
		sep := strings.Index(n, "/")
		if scp {
			colon := strings.Index(n, ":")
			if sep == -1 || colon > 0 && colon < sep {
				sep = colon
			}
		}
		host = n
		if sep >= 0 {
			host, repoPath = n[:sep+1], n[sep+1:]
		}
		if github {
			if scheme == "ssh://" || username != "" {
				scheme, host = "", "github.com:"
			} else {
				scheme, username, host = "https://", "", "github.com/"
			}
		}
		if host == "" {
			return GitBase{}, false
		}
		host = scheme + username + host
	}
	var subpath string
	switch {
	case strings.Contains(repoPath, "_git/"):
		before, after, _ := strings.Cut(repoPath, "_git/")
		repo, rest, _ := strings.Cut(after, "/")
		repoPath, subpath = before+"_git/"+repo, rest
	case strings.Contains(repoPath, "//"):
		repoPath, subpath, _ = strings.Cut(repoPath, "//")
	case strings.Contains(repoPath, ".git"):
		before, after, _ := strings.Cut(repoPath, ".git")
		repoPath, subpath = before+".git", after
	case scheme != "file://":
		segments := strings.Split(repoPath, "/")
		if len(segments) < 2 {
			return GitBase{}, false
		}
		repoPath, subpath = strings.Join(segments[:2], "/"), strings.Join(segments[2:], "/")
	}
	clean := filepath.Clean(strings.TrimPrefix(subpath, string(filepath.Separator)))
	first, _, _ := strings.Cut(clean, string(filepath.Separator))
	if repoPath == "" || first == ".." {
		return GitBase{}, false
	}
	return GitBase{RepoURL: host + repoPath, SubPath: strings.TrimPrefix(subpath, "/"), Ref: ref, Timeout: timeout, Submodules: submodules}, true
}

func parseQuery(query string) (string, time.Duration, bool) {
	const defaultTimeout = 27 * time.Second
	values, err := url.ParseQuery(query)
	if err != nil {
		return "", defaultTimeout, true
	}
	ref := values.Get("version")
	if value := values.Get("ref"); value != "" {
		ref = value
	}
	timeout := defaultTimeout
	if value := values.Get("timeout"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
			timeout = time.Duration(seconds) * time.Second
		} else if duration, err := time.ParseDuration(value); err == nil && duration > 0 {
			timeout = duration
		}
	}
	submodules := true
	if value := values.Get("submodules"); value != "" {
		if enabled, err := strconv.ParseBool(value); err == nil {
			submodules = enabled
		}
	}
	return ref, timeout, submodules
}

// CutHTTPScheme splits the HTTP scheme used by file acquisition.
func CutHTTPScheme(s string) (string, string, bool) {
	for _, scheme := range []string{"https://", "http://"} {
		if rest, ok := trimPrefixFold(s, scheme); ok {
			return scheme, rest, true
		}
	}
	return "", "", false
}

func trimPrefixFold(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):], true
	}
	return s, false
}
