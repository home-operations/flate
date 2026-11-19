# flate GitHub Action

Install [flate](../README.md) in a GitHub Actions job. The action supports Linux, macOS, and Windows runners.

## Usage

Add these steps after your job checks out the repository. Replace `./kubernetes` with the directory that contains your Flux files.

```yaml
- uses: home-operations/flate/action@v0.7.0
  with:
    cache: true
- run: flate get ks --path ./kubernetes
```

The action downloads the release and compares its SHA-256 checksum with the published value. It adds `flate` to `PATH` for later steps.

## Inputs

| Input       | Purpose                                                         | Default                                     |
| ----------- | --------------------------------------------------------------- | ------------------------------------------- |
| `base`      | Git revision to compare with.                                   | The repository default branch               |
| `bindir`    | Directory for the flate executable.                             | A directory under `$RUNNER_TOOL_CACHE`      |
| `cache`     | Save and restore the cache across workflow runs.                | `false`                                     |
| `cache_dir` | Parent directory for the cache. The action appends `/flate`.    | `${{ runner.temp }}`                        |
| `token`     | Token for GitHub API requests.                                  | `${{ github.token }}`                       |
| `version`   | flate release to install. Accepts a version number or `latest`. | See [Version selection](#version-selection) |

The action sets `FLATE_BASE` from `base`. This makes `build`, `get`, and `test` select resources affected by changes from that revision. To process the full tree for one command, pass `--base=`.

## Version selection

The action picks a release in this order.

1. If you set `version`, the action uses that value.
2. If `uses` names a release tag, the action uses that tag.
3. If `uses` points to a Git commit, the action looks for a matching release tag.
4. If the action finds no version, it installs the latest release.

The commit lookup uses `jq` and the GitHub API. If the workflow runs outside `github.com`, the action ignores `token` for its requests to GitHub.

## Caching

The action sets `FLATE_CACHE_DIR` to `<cache_dir>/flate`, which defaults to `$RUNNER_TEMP/flate`. flate uses this value as the default for `--cache-dir`.

To keep downloaded sources and render results across runs, set `cache: true`. The action uses [`actions/cache`](https://github.com/actions/cache) and looks for a saved cache for the same runner operating system and architecture.

If you manage the cache in a separate step, leave `cache: false`. Use `${{ env.FLATE_CACHE_DIR }}` as the path in that step.

## Outputs

| Output      | Value                                                                                             |
| ----------- | ------------------------------------------------------------------------------------------------- |
| `version`   | The installed flate version without the leading `v`.                                              |
| `cache-hit` | The string `true` when the cache key matches exactly. The output is available with `cache: true`. |
