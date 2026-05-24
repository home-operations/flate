package discovery

import (
	"log/slog"
	"strings"

	gogit "github.com/go-git/go-git/v5"
)

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
	repo, err := gogit.PlainOpen(repoRoot)
	if err != nil {
		return nil
	}
	cfg, err := repo.Config()
	if err != nil {
		return nil
	}
	out := map[string]struct{}{}
	for _, remote := range cfg.Remotes {
		for _, url := range remote.URLs {
			if n := normalizeGitURL(url); n != "" {
				out[n] = struct{}{}
			}
		}
	}
	return out
}

// normalizeGitURL reduces git URL variants to a canonical
// host/owner/repo key suitable for cross-syntax equality. Strips
// scheme, leading user@, port, trailing .git, and lowercases the
// result (GitHub/GitLab paths are case-insensitive in practice).
//
// Examples normalize to "github.com/owner/repo":
//
//	ssh://git@github.com/Owner/Repo.git
//	git@github.com:owner/repo.git
//	https://github.com/owner/repo
//	https://user:pass@github.com/owner/repo.git
//
// Returns "" for inputs that don't look like a remote URL (e.g.
// file:// or local paths) — those shouldn't participate in
// bootstrap-alias matching.
func normalizeGitURL(url string) string {
	u := strings.TrimSpace(url)
	if u == "" {
		return ""
	}
	// scp-style: git@host:owner/repo — convert to a uniform shape
	// before stripping scheme.
	if !strings.Contains(u, "://") {
		if at := strings.Index(u, "@"); at >= 0 {
			if colon := strings.Index(u[at+1:], ":"); colon >= 0 {
				u = u[at+1:colon+at+1] + "/" + u[at+1+colon+1:]
			}
		} else {
			// Local path or other non-remote shape — skip.
			return ""
		}
	} else {
		u = u[strings.Index(u, "://")+3:]
		// Strip optional userinfo (user[:pass]@).
		if at := strings.Index(u, "@"); at >= 0 && at < strings.IndexByte(u+"/", '/') {
			u = u[at+1:]
		}
		// Strip port (host:1234/...).
		if slash := strings.IndexByte(u, '/'); slash > 0 {
			host := u[:slash]
			if colon := strings.Index(host, ":"); colon >= 0 {
				u = host[:colon] + u[slash:]
			}
		}
	}
	// Discard file:// and other non-remote schemes that may have
	// slipped past the scheme check.
	if strings.HasPrefix(u, "/") {
		return ""
	}
	u = strings.TrimSuffix(u, "/")
	u = strings.TrimSuffix(u, ".git")
	return strings.ToLower(u)
}

// debugLogRemotes is a small helper to keep the discovery flow log
// readable when working-tree remote inspection is requested.
func debugLogRemotes(remotes map[string]struct{}) {
	if len(remotes) == 0 {
		return
	}
	keys := make([]string, 0, len(remotes))
	for k := range remotes {
		keys = append(keys, k)
	}
	slog.Debug("discovery: working tree remotes", "count", len(keys), "remotes", keys)
}
