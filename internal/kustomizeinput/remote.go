package kustomizeinput

import (
	"net/url"
	"path"
	"strings"
)

// GitBase is a resources: entry classified as a remote git base,
// decomposed into the parts FetchRemoteBase + the rewrite need.
type GitBase struct {
	RepoURL string // clone URL: scheme + host + repoPath (no ?query, no //SubPath)
	SubPath string // kustomization root within the repo ("" = repo root)
	Ref     string // ?ref= / ?version= value ("" = the repo's default branch)
}

// git URL markers, mirroring sigs.k8s.io/kustomize/api/internal/git/repospec.go
// (tryExplicitMarkerSplit + ignoreForcedGitProtocol).
const (
	gitForcedProtocol = "git::" // go-getter prefix; stripped, marks a git base
	gitRootDelimiter  = "_git/" // Azure DevOps: repo root is the segment after _git/
	gitSubPathSep     = "//"    // double-slash separates repo root from kust subpath
	gitSuffix         = ".git"  // .git is part of the repo dir name
)

// ParseGitBase reports whether a resources: entry is a remote git base (vs
// a plain HTTP file, an OCI ref, or a local path) and decomposes it into
// (RepoURL, SubPath, ref).
//
// It mirrors kustomize's own classification (internal/git/repospec.go) but
// WITHOUT a host allowlist — flate renders self-hosted Gitea / GitLab / Azure
// too, and kustomize's git-vs-file decision is driven by URL markers, not the
// host. To stay strictly additive over flate's existing HTTP-file preflight,
// classification is limited to the http/https schemes preflight already
// intercepts: a URL is a git base iff it carries an explicit marker (_git/,
// //, .git, or a git:: prefix) OR a ?ref=/?version= query. A marker-less,
// ref-less http(s) URL stays on the unchanged single-file fetch path; every
// other scheme (file://, ssh://, oci://, …) and local path is left untouched
// for kustomize to resolve as it does today.
func ParseGitBase(raw string) (GitBase, bool) {
	s, forced := trimPrefixFold(raw, gitForcedProtocol)
	scheme, hostAndPath, ok := CutHTTPScheme(s)
	if !ok {
		return GitBase{}, false
	}
	// Cut the query (?ref=/?version=) before touching the path — per rfc3986
	// "?" only appears in the query, so this split is unambiguous.
	pathPart, query, _ := strings.Cut(hostAndPath, "?")
	ref := refFromQuery(query)

	// Host is everything up to the first "/", kept with its trailing slash to
	// match kustomize's Host (e.g. "https://github.com/"); the rest is the path.
	host, repoSegs, found := strings.Cut(pathPart, "/")
	if !found {
		return GitBase{}, false // host only, no repo path
	}
	hostPrefix := scheme + host + "/"

	repoPath, SubPath, ok := markerSplit(repoSegs)
	if !ok {
		// No explicit marker: only a git:: prefix or a ?ref=/?version= query
		// makes this a git base; otherwise it's a plain HTTP file left on the
		// existing fetch path. With a ref/marker, apply kustomize's default
		// org/repo (2-segment) split.
		if !forced && ref == "" {
			return GitBase{}, false
		}
		if repoPath, SubPath, ok = defaultSplit(repoSegs); !ok {
			return GitBase{}, false
		}
	}
	if repoPath == "" || subPathEscapes(SubPath) {
		return GitBase{}, false
	}
	return GitBase{RepoURL: hostPrefix + repoPath, SubPath: SubPath, Ref: ref}, true
}

// markerSplit splits a repo path on the first explicit kustomize git marker
// (_git/, then //, then .git), mirroring repospec.go's tryExplicitMarkerSplit
// order. Returns ok=false when none is present.
func markerSplit(p string) (repoPath, SubPath string, ok bool) {
	if before, after, found := strings.Cut(p, gitRootDelimiter); found {
		// _git/: repo root is the FIRST segment after the delimiter.
		seg, rest, _ := strings.Cut(after, "/")
		return before + gitRootDelimiter + seg, rest, true
	}
	if before, after, found := strings.Cut(p, gitSubPathSep); found {
		// //: a convention-only separator, dropped from the result.
		return before, after, true
	}
	if i := gitSuffixIndex(p); i >= 0 {
		// .git: part of the repo dir name; the subpath follows it.
		end := i + len(gitSuffix)
		return p[:end], strings.TrimPrefix(p[end:], "/"), true
	}
	return "", "", false
}

// gitSuffixIndex finds ".git" only as a path-segment suffix (followed by "/"
// or end of string), so a ".gitignore"/".gitattributes" file resource is not
// mistaken for a repo boundary. Returns -1 when absent.
func gitSuffixIndex(p string) int {
	for from := 0; ; {
		i := strings.Index(p[from:], gitSuffix)
		if i < 0 {
			return -1
		}
		abs := from + i
		end := abs + len(gitSuffix)
		if end == len(p) || p[end] == '/' {
			return abs
		}
		from = end
	}
}

// defaultSplit takes the first two path segments as the repo (org/repo) and
// the rest as the subpath, mirroring repospec.go's orgRepoSegments default.
// Fewer than two non-empty segments is not a valid git base.
func defaultSplit(p string) (repoPath, SubPath string, ok bool) {
	org, rest, _ := strings.Cut(p, "/")
	repo, SubPath, _ := strings.Cut(rest, "/")
	if org == "" || repo == "" {
		return "", "", false
	}
	return org + "/" + repo, SubPath, true
}

// refFromQuery returns the git ref from a URL query: ?ref= takes precedence
// over ?version= (matching kustomize's parseQuery).
func refFromQuery(query string) string {
	if query == "" {
		return ""
	}
	v, err := url.ParseQuery(query)
	if err != nil {
		return ""
	}
	if ref := v.Get("ref"); ref != "" {
		return ref
	}
	return v.Get("version")
}

// subPathEscapes reports whether SubPath climbs out of the repo root (e.g. a
// crafted //../../etc). Mirrors repospec.go's kustRootPathExitsRepo: clean the
// path and reject a leading "..".
func subPathEscapes(SubPath string) bool {
	if SubPath == "" {
		return false
	}
	clean := path.Clean(strings.TrimPrefix(SubPath, "/"))
	first, _, _ := strings.Cut(clean, "/")
	return first == ".."
}

// CutHTTPScheme splits an http:// or https:// scheme prefix from s. Other
// schemes (file://, ssh://, oci://) and scheme-less paths return ok=false:
// classification stays within the http(s) set flate's preflight already
// intercepts, so no other resource kind changes behavior.
func CutHTTPScheme(s string) (scheme, rest string, ok bool) {
	for _, sc := range []string{"https://", "http://"} {
		if r, found := trimPrefixFold(s, sc); found {
			return sc, r, true
		}
	}
	return "", "", false
}

// trimPrefixFold strips a case-insensitive prefix (kustomize lowercases
// schemes and the git:: marker), returning the remainder and whether it matched.
func trimPrefixFold(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):], true
	}
	return s, false
}
