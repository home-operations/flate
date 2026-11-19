# flate

Preview the Kubernetes YAML from your Flux repository without a running cluster.

[![CI](https://github.com/home-operations/flate/actions/workflows/ci.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/ci.yaml) [![Release](https://img.shields.io/github/v/release/home-operations/flate)](https://github.com/home-operations/flate/releases) [![Discord](https://img.shields.io/discord/673534664354430999?label=discord&logo=discord&logoColor=white&color=blue)](https://discord.gg/home-operations)

Use flate to see what a pull request changes and catch render errors before deployment. It runs as one executable and fetches sources as needed.

## Install

| Method   | How to install                                                                                             |
| -------- | ---------------------------------------------------------------------------------------------------------- |
| Download | Get the archive for your system from [GitHub Releases](https://github.com/home-operations/flate/releases). |
| Homebrew | `brew install --cask home-operations/tap/flate`                                                            |
| Go       | `go install github.com/home-operations/flate/cmd/flate@latest`                                             |
| Docker   | `docker pull ghcr.io/home-operations/flate:latest`                                                         |

## Quick start

Run these commands from the root of your Flux repository.

```sh
flate build # Print the rendered YAML
flate diff  # Show what changed in the rendered YAML
flate test  # Report render errors and exit with status 1 if any occur
flate get   # List Flux resources
```

Use `-p` to choose a directory. Use `flate <command> --help` to see the flags, defaults, and output formats.

## Common tasks

| Task                                                         | Command                                      |
| ------------------------------------------------------------ | -------------------------------------------- |
| Compare with `main` and format the diff for a GitHub comment | `flate diff --base main -o github`           |
| Render one HelmRelease and its dependencies                  | `flate build hr demo -n apps`                |
| Render resources affected by changes from `main`             | `flate build --base main`                    |
| List container images                                        | `flate get images`                           |
| Supply values and other objects that a render needs          | `flate build --supply ./offline-inputs.yaml` |
| Reuse downloaded sources and rendered output across runs     | `flate build --cache-dir ~/.cache/flate`     |

Use `--base` to compare with a Git revision. Use `--path-orig` to compare with another directory. If you omit both flags, `flate diff` looks for a Git revision to compare against.

## GitHub Actions and pre-commit

Add these steps to your GitHub Actions job after checkout. See the [action documentation](action/README.md) for version and cache configuration.

```yaml
- uses: home-operations/flate/action@v0.7.0
- run: flate test
```

To run the same test before each commit, add this hook to `.pre-commit-config.yaml`.

```yaml
repos:
  - repo: https://github.com/home-operations/flate
    rev: v0.7.0
    hooks:
      - id: flate
```

## What flate supports

- Renders Flux Kustomizations, HelmReleases, and ResourceSets.
- Fetches content from GitRepository, OCIRepository, HelmRepository, HelmChart, and Bucket sources.
- Builds artifacts with ArtifactGenerator and reads them through ExternalArtifact.

### Differences from a live cluster

- flate treats SOPS-encrypted keys as missing. Use `--supply` to provide dummy values or other missing inputs. Referenced values can appear in the rendered YAML.
- Some ResourceSet input providers need saved inputs when they run without a cluster.
- Private sources need credentials from your repository, supplied objects, or the machine that runs flate.
- Helm renders each release as a new install. Use `--kube-version` and `--api-versions` to match the target cluster.
- flate skips Git and OCI signature checks.
- By default, flate omits Secrets and CustomResourceDefinitions from the output.

## Go library

Use [`pkg/orchestrator`](https://pkg.go.dev/github.com/home-operations/flate/pkg/orchestrator) to render and read results. Use [`pkg/manifest`](https://pkg.go.dev/github.com/home-operations/flate/pkg/manifest) for resource types and identities.

If a render returns a non-nil run, call `Close` after you finish using its results, even if the render returns an error.

## Help and license

Ask questions on [Discord](https://discord.gg/home-operations). Report bugs or request features in [GitHub Issues](https://github.com/home-operations/flate/issues).

flate uses the [AGPL-3.0 license](LICENSE). Its behavior and test fixtures draw on [flux-local](https://github.com/allenporter/flux-local), which uses the Apache-2.0 license.
