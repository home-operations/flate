# flate GitHub Action

Install the [flate](https://github.com/home-operations/flate) CLI on Linux, macOS, or Windows runners — with optional caching of flate's on-disk store between runs.

```yaml
steps:
  - uses: home-operations/flate/action@main
    with:
      version: latest
      cache: true
  - run: flate get ks --path ./kubernetes
```

Installation is handled by [`jdx/mise-action`](https://github.com/jdx/mise-action) under the hood via mise's `github:` backend, so the same install path works on every runner without bespoke download/checksum scripting.

## Inputs

| Name      | Description                                                                         | Default               |
| --------- | ----------------------------------------------------------------------------------- | --------------------- |
| `version` | flate version to install (e.g. `0.1.25`), or `latest`                               | `latest`              |
| `token`   | GitHub token mise uses to resolve and download releases (avoids unauth rate limits) | `${{ github.token }}` |
| `cache`   | Restore and save flate's XDG cache between runs                                     | `false`               |

## Outputs

| Name        | Description                                                                    |
| ----------- | ------------------------------------------------------------------------------ |
| `version`   | Resolved flate version (without the leading `v`)                               |
| `cache-dir` | On-disk path flate uses for its persistent cache on this runner                |
| `cache-hit` | `'true'` when a previous flate cache was restored (only set when `cache=true`) |

## Caching

Setting `cache: true` wraps the install in [`actions/cache`](https://github.com/actions/cache), pointed at flate's [XDG cache](https://github.com/home-operations/flate/blob/main/pkg/source/cacheroot/layout.go) (`$XDG_CACHE_HOME/flate` on Linux, `~/Library/Caches/flate` on macOS, `%LOCALAPPDATA%\flate` on Windows). This persists fetched git sources, Helm chart tarballs, OCI blobs, and bare git mirrors across runs.

The cache key is keyed on OS, architecture, and the current run — every run saves a fresh entry and restores the most recent one for that runner shape. flate's cache is content-addressed, so cross-run reuse is safe regardless of which flate version produced it.

For finer-grained control, leave `cache: false` and wire your own `actions/cache` step around the action using the `cache-dir` output.
