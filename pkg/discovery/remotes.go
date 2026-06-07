package discovery

import (
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"

	gogit "github.com/go-git/go-git/v5"
)

// selfRemotes returns the normalized remote-URL set used to recognize a
// self-referential GitRepository. When the caller supplied Config.SelfURLs
// explicitly (an SDK consumer rendering an extracted tree with no
// .git/config), those are used; otherwise it falls back to the working
// tree's .git remotes — byte-identical to prior behavior when SelfURLs is
// empty.
func (d *discoverer) selfRemotes(repoRoot string) map[string]struct{} {
	if len(d.cfg.SelfURLs) == 0 {
		return readWorkingTreeRemotes(repoRoot)
	}
	out := make(map[string]struct{}, len(d.cfg.SelfURLs))
	for _, u := range d.cfg.SelfURLs {
		if n := normalizeGitURL(u); n != "" {
			out[n] = struct{}{}
		}
	}
	return out
}

// readWorkingTreeRemotes returns the set of remote URLs configured on
// the git repository at repoRoot, normalized for comparison against
// Flux GitRepository.spec.url. Returns nil if repoRoot is not a git
// repo or no remotes are configured.
//
// Consumed by aliasBootstrapSources to recognize a file-loaded
// GitRepository whose URL points at the SAME repo the user is
// running flate against — in those cases the SSH/HTTPS fetch would
// either round-trip to a real host (slow, offline-unfriendly) or
// fail due to SOPS-wiped credentials. Aliasing the GitRepository to
// the working tree avoids both pitfalls.
func readWorkingTreeRemotes(repoRoot string) map[string]struct{} {
	// DetectDotGit follows a `.git` file pointer (git worktrees,
	// submodules), so the URL-match aliasing still works when flate
	// is run inside a `git worktree add`'d checkout. Without it,
	// PlainOpen errors out and the self-referential override silently
	// no-ops.
	repo, err := gogit.PlainOpenWithOptions(repoRoot, &gogit.PlainOpenOptions{DetectDotGit: true})
	if err != nil {
		return nil
	}
	cfg, err := repo.Config()
	if err != nil {
		return nil
	}
	out := map[string]struct{}{}
	for _, remote := range cfg.Remotes {
		for _, u := range remote.URLs {
			if n := normalizeGitURL(u); n != "" {
				out[n] = struct{}{}
			}
		}
	}
	return out
}

// normalizeGitURL reduces git URL variants to a canonical
// host/owner/repo key suitable for cross-syntax equality. Strips
// scheme, userinfo, port, query, fragment, trailing .git/`/`, and
// lowercases the result (GitHub/GitLab paths are case-insensitive
// in practice). IPv6 hosts are returned bare (no brackets).
//
// Examples normalize to "github.com/owner/repo":
//
//	ssh://git@github.com/Owner/Repo.git
//	git@github.com:owner/repo.git
//	https://github.com/owner/repo
//	https://user:pass@github.com/owner/repo.git
//	https://github.com/owner/repo?deploy_key=prod#section
//	https://github.com:443/owner/repo.git/
//
// Returns "" for inputs that don't describe a remote (file://,
// local paths, empty) — those don't participate in bootstrap-alias
// matching.
func normalizeGitURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	host, path, ok := parseGitURL(raw)
	if !ok || host == "" || path == "" {
		return ""
	}
	path = strings.TrimSuffix(path, "/")
	path = strings.TrimSuffix(path, ".git")
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return ""
	}
	return strings.ToLower(host + "/" + path)
}

// parseGitURL extracts the host and path components of a remote git
// URL, handling both URL-form (scheme://...) and scp-style
// (user@host:path). Returns ok=false for non-remote shapes (file://,
// local paths, malformed input). Userinfo, port, query, and fragment
// are dropped — net/url handles the URL-form parsing including
// bracketed IPv6 hosts.
func parseGitURL(raw string) (host, path string, ok bool) {
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", "", false
		}
		switch strings.ToLower(u.Scheme) {
		case "http", "https", "ssh", "git":
			// supported remote schemes
		default:
			// file://, ftp://, anything else — not a remote we
			// can compare against a working-tree clone.
			return "", "", false
		}
		// url.Hostname strips both port and IPv6 brackets correctly.
		return u.Hostname(), u.Path, true
	}
	// scp-style: user@host:owner/repo (or host:owner/repo without
	// user). The first `:` after the first `@` (or after the start
	// when no `@`) separates host from path. Anything that doesn't
	// match this shape is treated as a local path and rejected.
	rest := raw
	if _, after, ok := strings.Cut(rest, "@"); ok {
		rest = after
	}
	host, path, ok = strings.Cut(rest, ":")
	if !ok || host == "" {
		return "", "", false
	}
	if strings.ContainsAny(host, "/\\") {
		// Looks like a relative path containing a colon, not
		// host:path. Reject.
		return "", "", false
	}
	return host, "/" + path, true
}

// debugLogRemotes is a small helper to keep the discovery flow log
// readable when working-tree remote inspection is requested.
func debugLogRemotes(remotes map[string]struct{}) {
	if len(remotes) == 0 {
		return
	}
	// Sorted so repeated runs over the same working tree log the
	// remotes in a stable order.
	keys := slices.Sorted(maps.Keys(remotes))
	slog.Debug("discovery: working tree remotes", "count", len(keys), "remotes", keys)
}
