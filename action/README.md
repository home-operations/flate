# flate GitHub Action

Install the [flate](https://github.com/home-operations/flate) CLI on Linux, macOS, or Windows runners — with optional caching of flate's on-disk store between runs.

```yaml
steps:
  # Installs flate matching the pinned action ref and reuses its
  # on-disk cache across runs.
  - uses: home-operations/flate/action@0.1.30
    with:
      cache: true
  - run: flate get ks --path ./kubernetes
```

The action downloads the release archive from GitHub, verifies its SHA-256, installs into `$RUNNER_TOOL_CACHE` (or the path given via `bindir`), and exports `FLATE_CACHE_DIR` so subsequent `flate` calls write to a predictable location.

## Inputs

| Name      | Description                                                                                 | Default                   |
| --------- | ------------------------------------------------------------------------------------------- | ------------------------- |
| `version` | flate version to install (e.g. `0.1.25`) or `latest`                                        | _see [Version](#version)_ |
| `token`   | GitHub token used to resolve releases and avoid unauthenticated rate limits on GitHub's API | `${{ github.token }}`     |
| `cache`   | Restore and save flate's on-disk cache between runs                                         | `false`                   |
| `bindir`  | Alternative install location for the flate binary                                           | `$RUNNER_TOOL_CACHE/...`  |

## Outputs

| Name        | Description                                                                    |
| ----------- | ------------------------------------------------------------------------------ |
| `version`   | Resolved flate version (without the leading `v`)                               |
| `cache-hit` | `'true'` when a previous flate cache was restored (only set when `cache=true`) |

## Version

When `version` is not set, the action inspects the ref it was pinned to (`github.action_ref`) and resolves it to a flate release. Explicit `version:` always wins; otherwise:

1. If the ref is a SemVer tag (e.g. `@0.1.26`, `@v0.1.26`), that version is installed.
2. If the ref is a commit SHA (full or short), the action queries the GitHub `/tags` API for its own repo and installs the SemVer tag whose commit matches — this is the path used by workflows that pin actions to immutable digests for supply-chain security.
3. Otherwise (branches, dangling SHAs), the action falls back to `latest`.

| Pinned as                                             | Installed version |
| ----------------------------------------------------- | ----------------- |
| `home-operations/flate/action@0.1.26`                 | `0.1.26`          |
| `home-operations/flate/action@v0.1.26`                | `0.1.26`          |
| `home-operations/flate/action@2e8f4c8…` (release SHA) | matching tag      |
| `home-operations/flate/action@main`                   | `latest`          |
| `home-operations/flate/action@<non-release-sha>`      | `latest`          |
| any of the above, with `version: 0.1.20`              | `0.1.20`          |

The SHA-to-tag lookup uses `inputs.token` (defaulted to `${{ github.token }}`) and degrades to `latest` if the API call fails or `jq` isn't available.

> On non-github.com forges (Forgejo, Gitea, GHES), `${{ github.token }}` is issued by that forge and rejected by `api.github.com`. The action detects this via `GITHUB_SERVER_URL` and drops the token for its github.com calls, so passing `token: ""` is not required.

## Caching

The action always exports `FLATE_CACHE_DIR=${RUNNER_TEMP}/flate-cache` (auto-bound to flate's `--cache-dir`), so every `flate` invocation in the job writes to that path regardless of OS.

Setting `cache: true` wraps the install in [`actions/cache`](https://github.com/actions/cache), pointed at the same path. This persists fetched git sources, Helm chart tarballs, OCI blobs, and bare git mirrors across runs.

The cache key is keyed on OS, architecture, and the current run — every run saves a fresh entry and restores the most recent one for that runner shape. flate's cache is content-addressed, so cross-run reuse is safe regardless of which flate version produced it.

For finer-grained control, leave `cache: false` and wire your own `actions/cache` step around the action using `${{ env.FLATE_CACHE_DIR }}` as the path.
