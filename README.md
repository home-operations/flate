# flate

> A single-binary Go rewrite of [flux-local](https://github.com/allenporter/flux-local) — render and diff Flux GitOps repositories **fully offline**, without a cluster, `kubectl`, or shell calls.

[![Tests](https://github.com/home-operations/flate/actions/workflows/tests.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/tests.yaml)
[![Build](https://github.com/home-operations/flate/actions/workflows/build.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/build.yaml)
[![Lint](https://github.com/home-operations/flate/actions/workflows/lint.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/lint.yaml)
[![Release](https://img.shields.io/github/v/release/home-operations/flate)](https://github.com/home-operations/flate/releases)
[![License](https://img.shields.io/github/license/home-operations/flate)](LICENSE)

---

## Contents

- [Why](#why)
- [Quick start](#quick-start)
- [Subcommands](#subcommands)
  - [`flate get`](#flate-get)
  - [`flate build`](#flate-build)
  - [`flate diff`](#flate-diff)
  - [`flate test`](#flate-test)
  - [`flate diag`](#flate-diag)
- [Changed-only mode](#changed-only-mode)
- [Output formats](#output-formats)
- [Configuration](#configuration)
  - [Common flags](#common-flags)
  - [Helm rendering](#helm-rendering)
  - [Build flags](#build-flags)
  - [Diff flags](#diff-flags)
  - [Get flags](#get-flags)
- [Defaults](#defaults)
- [Architecture](#architecture)
- [Development](#development)
- [Deferred](#deferred)
- [License](#license)

---

## Why

Validating a Flux GitOps repo from CI used to mean a `kind` cluster, a `flux install`, and a stack of binaries — `helm`, `kustomize`, `kubectl`, `flux` itself. flate folds that into a single static binary that links helm, kustomize, go-git, and oras-go as native Go libraries.

- 🪶 **Single binary, no shellouts** — no `helm`, `kustomize`, `flux`, or `kubectl` on `PATH`.
- ⚡ **PR-fast** — changed-only mode reconciles only the resources whose source files moved; one-file PRs drop from seconds to tens of milliseconds.
- 🧪 **Diff-aware** — unified diffs of rendered output, with parent KS / HR headers and a flat image-set diff for CI pull-tests.
- 📦 **CI-friendly** — JSON / YAML / name output to stdout, no TTY assumptions.
- 🧬 **Native Go SDKs** — helm v4 client-only, krusty, go-git, oras-go — linked in, not shelled out.

## Quick start

```bash
go install github.com/home-operations/flate/cmd/flate@latest
```

…or pull the container image (used by the `home-operations/flate` GitHub Action):

```bash
docker pull ghcr.io/home-operations/flate:latest
```

A typical first run against a Flux repo:

```bash
flate get ks       --path ./kubernetes
flate build hr     --path ./kubernetes
flate diff ks      --path ./kubernetes --path-orig ../baseline/kubernetes
flate diff images  --path ./kubernetes --path-orig ../baseline/kubernetes -o json
```

## Subcommands

| Command | Aliases | Purpose |
|---|---|---|
| `flate get ks` | `kustomization`, `kustomizations` | List Kustomizations |
| `flate get hr` | `helmrelease`, `helmreleases` | List HelmReleases |
| `flate get all` | | Cluster summary (`--enable-images`, `--only-images`) |
| `flate build ks` | | Render Kustomizations to YAML |
| `flate build hr` | | Render HelmReleases to YAML |
| `flate build all` | | Render every Kustomization and HelmRelease |
| `flate diff ks` | | Unified diff of rendered Kustomizations |
| `flate diff hr` | | Unified diff of rendered HelmReleases |
| `flate diff images` | | Set diff of container images (CI-friendly) |
| `flate test ks` | `kustomization`, `kustomizations` | Validate Kustomization reconcile status |
| `flate test hr` | `helmrelease`, `helmreleases` | Validate HelmRelease reconcile status |
| `flate test all` | | Validate every Kustomization and HelmRelease |
| `flate diag` | | YAML / `.krmignore` sanity checks |

Every command takes `--path <dir>` (default `.`). Add `--path-orig <dir>` to switch into [changed-only mode](#changed-only-mode).

### `flate get`

```bash
flate get ks --path ./kubernetes -o json
flate get hr --path ./kubernetes -l app.kubernetes.io/part-of=media
flate get all --path ./kubernetes --only-images
```

`get all --enable-images` groups images per HelmRelease; `--only-images` emits a flat, deduplicated list across both HelmReleases and Kustomization-managed workloads (Deployments, StatefulSets, Pods, Jobs).

### `flate build`

Renders everything that would be applied to the cluster:

```bash
flate build hr --path ./kubernetes              # all HRs
flate build hr --path ./kubernetes media/plex   # one HR
flate build all --path ./kubernetes -o yaml
```

`--show-only` / `-s` accepts repeated `templates/<file>.yaml` paths to narrow the helm render.

### `flate diff`

Compare a working tree against a baseline. `--path-orig` is required.

```bash
git worktree add ../baseline main
flate diff ks --path ./kubernetes --path-orig ../baseline/kubernetes
flate diff hr --path ./kubernetes --path-orig ../baseline/kubernetes
```

Output is unified diff with rich, reader-friendly headers and a blank line after every separator:

```diff
--- HelmRelease: media/qui Deployment: media/qui

+++ HelmRelease: media/qui Deployment: media/qui

@@ -61,13 +61,13 @@

         envFrom:
         - secretRef:
             name: qui-secret
-        image: ghcr.io/autobrr/qui:v1.18.0@sha256:…
+        image: ghcr.io/autobrr/qui:v1.19.0@sha256:…
```

The header always shows the **parent** (HelmRelease or Kustomization) and the rendered **child resource**. For KS parents the Flux `spec.path` prepends the line.

#### `flate diff images`

A diff-aware companion to `get all --only-images`. Emits images whose string actually changed between `--path` and `--path-orig` — touching an env var doesn't produce noise.

| Flag | Effect |
|---|---|
| _(default)_ | Emit images present in `--path` but not in `--path-orig` (newly added). |
| `--include-removed` | Emit the full symmetric difference (added + removed). |

```bash
flate diff images --path ./pull --path-orig ./default -o json
# ["ghcr.io/foo/bar:v2.1@sha256:…"]
```

`-o name` (the default for non-structured output) gives one image per line, ideal for piping into a CI pull-test loop.

### `flate test`

Reports the reconcile status of every Kustomization and HelmRelease in the repo in a pytest-like progress format. A case passes when the resource reached `Ready`; it fails otherwise. Resources skipped by [changed-only mode](#changed-only-mode) appear as `SKIPPED`.

```bash
flate test all --path ./kubernetes              # both kinds
flate test ks  --path ./kubernetes              # only Kustomizations
flate test hr  --path ./kubernetes media/plex   # one HelmRelease by name
```

### `flate diag`

Static sanity checks that don't require reconciliation — YAML parseability, `.krmignore` coverage, dangling sourceRefs:

```bash
flate diag --path ./kubernetes
```

## Changed-only mode

Pass `--path-orig` to compare a working tree against a baseline. flate diffs files, picks the **most-specific Flux Kustomization that owns each change** (longest matching `spec.path`, including `spec.components`), and reconciles only that subtree.

What's in the keep-set:

- **direct edits** — every resource whose source file changed
- **chart sources / KS `sourceRef` / HR `valuesFrom`** — content dependencies, pulled in transitively
- **kustomize components** — touching a shared component (Flux v1 `spec.components` or a kustomize-level `components:` entry) re-renders **every consumer Kustomization**

What's _not_:

- **`dependsOn`** — this is a reconcile-ordering signal in Flux, not a content dependency. Skipped resources still get marked `Ready` so downstream depwait completes naturally.
- **meta-Kustomizations** — a top-level KS rooted at `apps/` doesn't claim files inside `apps/media/plex/app/` when a deeper KS owns them.

```bash
git worktree add ../baseline main
flate diff ks --path ./kubernetes --path-orig ../baseline/kubernetes
```

One-file PRs in a 70-Kustomization repo drop reconcile time from seconds to tens of milliseconds.

> **Narrow entries.** `--path` can point at a Flux entry like `./kubernetes/flux/cluster` — flate iteratively follows each loaded KS's `spec.path` to discover the full content tree without you having to widen the flag.

## Output formats

| `-o` | Effect |
|---|---|
| `table` _(default for `get`)_ | Aligned columns. |
| `wide` | Extra columns (`get` only). |
| `yaml` | Multi-document YAML. |
| `json` | One JSON array. |
| `name` | `<namespace>/<name>` per line — script-friendly. |
| `diff` _(default for `diff`)_ | Unified diff. |

All output is written to stdout — redirect with `> file.yaml` to capture it. Logs go to stderr, so structured formats stay clean for piping into `jq`, `yq`, or further processing.

## Configuration

### Common flags

| Flag | Default | Description |
|---|---|---|
| `--path` | `.` | Flux cluster directory. |
| `--path-orig` | _(unset)_ | Baseline path; enables changed-only mode for every command. |
| `-n`, `--namespace` | _(all)_ | Limit to a single namespace. Auto-scopes to the touched namespaces in changed-only mode. |
| `-l`, `--selector` | _(none)_ | `key=value` label selector, repeatable. |
| `-o`, `--output` | `table` | `table` / `wide` / `yaml` / `json` / `name`. |
| `--skip-crds` | `true` | Drop CRD objects from rendered output. |
| `--skip-secrets` | `true` | Drop Secret objects from rendered output. |
| `--skip-kinds` | _(none)_ | Extra kinds to drop, repeatable. |
| `--enable-oci` | `true` | Reconcile OCIRepository sources. |
| `--registry-config` | _(none)_ | Docker `config.json` for OCI auth. |
| `--log-level` | `info` | `debug` / `info` / `warn` / `error`. |

### Helm rendering

| Flag | Default | Description |
|---|---|---|
| `--kube-version` | bundled `k8s.io/api` minor | Kubernetes version for `.Capabilities.KubeVersion`. |
| `-a`, `--api-versions` | _(empty)_ | Extra API versions for `.Capabilities.APIVersions`. |
| `--is-upgrade` | `false` | Set `.Release.IsUpgrade` instead of `.Release.IsInstall`. |
| `--no-hooks` | `false` | Exclude hook-annotated templates. |
| `-s`, `--show-only` | _(none)_ | Restrict render to specific template paths, repeatable. |
| `--enable-dns` | `false` | Allow DNS lookups during `helm template`. |

### Build flags

Apply to `build ks` / `build hr` / `build all`:

| Flag | Default | Description |
|---|---|---|
| `--only-crds` | `false` | Emit only `CustomResourceDefinition` resources (implies `--skip-crds=false`). |

### Diff flags

Apply to `diff ks` / `diff hr`:

| Flag | Default | Description |
|---|---|---|
| `-u`, `--unified` | `6` | Unified diff context lines. |
| `--strip-attr` | `helm.sh/chart`, `checksum/config`, `app.kubernetes.io/version`, `chart` | Annotation / label key to strip before diffing, repeatable. Supplying any value replaces the default set. |
| `--limit-bytes` | `65536` | Truncate per-resource diffs (0 = unlimited; default matches GitHub issue body limit). |

`diff images` adds:

| Flag | Default | Description |
|---|---|---|
| `--include-removed` | `false` | Emit images dropped from `--path-orig` alongside newly added ones. |

### Get flags

Apply to `get all`:

| Flag | Default | Description |
|---|---|---|
| `--enable-images` | `false` | Group container images per HelmRelease in the summary. |
| `--only-images` | `false` | Emit only the deduplicated image list (no counts or HelmRelease grouping). |

## Defaults

- `--enable-oci` → **true**.
- `--kube-version` defaults to the Kubernetes minor bundled with flate's `k8s.io/api` dependency — Helm charts gated on `KubeVersion` render against the latest version flate knows about.
- Secrets are always replaced with `..PLACEHOLDER_<key>..` (matches flux-local).
- `--skip-crds`, `--skip-secrets` → **true** for `build` / `diff`.
- **Fast-fail dependency waits** for offline use: 30-second per-dep ceiling (vs. several minutes upstream) and a 2-second missing-grace, so typo'd `dependsOn` or broken `sourceRef` fail with `dependency not found` instead of stalling out the budget.

## Architecture

```
              ┌──────────────────────┐
              │   ResourceLoader     │
              │  walk + namespace    │
              │   inheritance        │
              └─────────┬────────────┘
                        ▼
              ┌──────────────────────┐
              │        Store         │◀── events ──┐
              │  objects + status +  │             │
              │  artifacts + pubsub  │             │
              └──┬──────┬────────┬───┘             │
                 │      │        │                 │
                 ▼      ▼        ▼                 │
       ┌────────────┐ ┌──────────────┐ ┌──────────────────┐
       │ SourceCtrl │ │ KSController │ │ HRController     │
       │ go-git +   │ │ krusty +     │ │ helm v4          │
       │ oras-go    │ │ Flux gen     │ │ (ClientOnly)     │
       └────────────┘ └──────────────┘ └──────────────────┘
```

Orchestrator pipeline: load → iterative `spec.path` discovery → namespace inheritance → `dependsOn` validation → existence-only-ready → change-filter resolution → controllers → diff / build / get.

## Development

```bash
go build ./cmd/flate            # build the CLI
go test ./...                   # full test suite, including in-process E2E
go test -race ./...             # race detector
go vet ./...                    # vet
```

Tool versions are pinned via [mise](https://mise.jdx.dev) (`mise install`).

Testdata lives in [`testdata/`](testdata/); the [`test/e2e`](test/e2e/) suite runs the cobra command tree in-process via `cli.Run` — no fork/exec, no freshly built binary required.

## Deferred

- `diff --branch-orig <branch>` (auto-worktree)
- `shell` interactive REPL
- Bucket sources, ResourceSet (Flux Operator), in-cluster `secretRef` for OCI auth

## License

AGPL-3.0 — see [LICENSE](LICENSE). flate borrows behavior and test fixtures from [flux-local](https://github.com/allenporter/flux-local) (Apache-2.0).
