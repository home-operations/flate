# flate

> Render and diff Flux GitOps repositories fully offline — one static binary, no cluster, no `kubectl`, no shellouts.

[![CI](https://github.com/home-operations/flate/actions/workflows/ci.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/ci.yaml)
[![Release](https://img.shields.io/github/v/release/home-operations/flate)](https://github.com/home-operations/flate/releases)
[![License](https://img.shields.io/github/license/home-operations/flate)](LICENSE)
[![Discord](https://img.shields.io/discord/673534664354430999?label=discord&logo=discord&logoColor=white&color=blue)](https://discord.gg/home-operations)

flate is a Go rewrite of [flux-local](https://github.com/allenporter/flux-local). Helm, kustomize, go-git, and oras-go are linked as native libraries, so a `kind` cluster plus a stack of CLIs (`helm`, `kustomize`, `flux`, `kubectl`) collapse into one binary that runs in CI in seconds, not minutes. Changed-only mode reconciles just the subtree a PR touches, dropping single-file diffs to tens of milliseconds on real home-ops repos.

At a glance:

- **Offline** — one static binary; no cluster, `kubectl`, `helm`/`kustomize`/`flux` CLIs, or shellouts.
- **Fast** — changed-only mode reconciles just the subtree a PR touches.
- **CI-native** — seconds not minutes; a GitHub Action ships in the repo.
- **Embeddable** — `pkg/orchestrator` is a library entry point.

## Contents

- [Install](#install)
- [Use](#use)
- [Changed-only mode](#changed-only-mode)
- [Source kinds and auth](#source-kinds-and-auth)
- [Behaviors](#behaviors)
- [Limits](#limits)
- [Architecture](#architecture)
- [Library use](#library-use)
- [Development](#development)
- [License](#license)

## Install

```bash
brew install --cask home-operations/tap/flate
go install github.com/home-operations/flate/cmd/flate@latest
docker pull ghcr.io/home-operations/flate:latest
```

…or in a GitHub Actions workflow:

```yaml
- uses: home-operations/flate/action@main
```

## Use

```bash
flate get          --path ./kubernetes
flate get ks       --path ./kubernetes
flate build        --path ./kubernetes
flate build hr     --path ./kubernetes plex
flate diff ks      --path ./kubernetes --path-orig ../baseline/kubernetes
flate diff         --path ./kubernetes --path-orig ../baseline/kubernetes
flate diff         --path ./kubernetes --path-orig ../baseline/kubernetes -o json
flate diff images  --path ./kubernetes --path-orig ../baseline/kubernetes -o json
flate test         --path ./kubernetes
```

`build`, `diff`, `get`, and `test` default to `all` when no subcommand is given; `flate build -p ./kubernetes` is equivalent to `flate build all -p ./kubernetes`. Group help lists the subcommands and the flags for `all`.

The `[name]` positional on `get ks/hr` and `build`/`diff`/`test ks/hr` is matched against the resource's bare name (`metadata.name`), not `namespace/name`. Use `-n / --namespace` to scope.

Named builds base their exit status on the selected objects and their required dependencies.

Every reconcile-running command takes `--path <dir>` (default `.`); `--path-orig <dir>` switches into changed-only mode. `flate <verb> --help` lists every flag. `get`, `build`, `diff`, and `test` all run the same offline reconcile pipeline before producing output, so referenced Git, OCI, Helm, Bucket, or remote kustomize sources must be reachable. Source caches respect Flux intervals: immutable pins reuse cache; mutable refs refresh when their interval expires.

| Verb    | Targets                     | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `get`   | `ks`, `hr`, `images`, `all` | List or summarize. `-o table` / `yaml` / `json` / `name`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `build` | `ks`, `hr`, `all`           | Render Kustomizations and HelmReleases to YAML or JSON.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `diff`  | `ks`, `hr`, `images`, `all` | Path-keyed diff against `--path-orig` via [dyff](https://github.com/homeport/dyff). K8s-aware: list entries match by identifier (container name, env-var name), so a reorder shows as `⇆ order changed` instead of phantom value churn. `-o` styles: `human` (default), `github`, `brief`, `gitlab`, `gitea`, a plain unified `diff`, structured `json`, and a self-contained `html` page (filterable navigation tree with keyboard nav, per-resource side-by-side ⇄ unified views, word-level syntax-highlighted diffs, light/dark theme). |
| `test`  | `ks`, `hr`, `all`           | Pytest-style `PASS` / `FAIL` / `SKIPPED` per resource. Non-zero exit on any failure.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

`get ks` and `get hr` accept `-l/--selector key=value` for label filtering. `diff` accepts `--strip-attr <key>` (repeatable) to drop annotation/label keys before comparison; the default set covers chart-bump noise (`helm.sh/chart`, `checksum/config`, `checksum/secret`, `app.kubernetes.io/version`, `chart`). Helm template flags are available on every reconcile-running subcommand because flate reconciles the full graph before filtering output. Reconcile-running subcommands accept `--allow-missing-secrets` to soft-skip source auth Secrets and omit generated HR `valuesFrom` refs that only exist in the live cluster — see [Behaviors](#behaviors).

**JSON diffs.** `diff ks`, `diff hr`, and `diff all` accept `-o json` and emit one 2-space-indented JSON object with `changes` and `suppressed` arrays, including `[]` when empty. Each change has `parent`, `apiVersion`, `kind`, `namespace`, `name`, `status` (`added`, `changed`, or `removed`), normalized `old`/`new` manifests, and `oldChart`/`newChart` labels captured before stripping. `old` is `null` for additions; `new` is `null` for removals; absent chart labels are empty strings. Parents have `kind`, `namespace`, `name`, and `path` (empty for HelmReleases). Changes are sorted by parent and resource identity, including API version. Suppressions have `parent`, `side` (`orig` or `current`, identifying the failed side), `count` (withheld objects), and `reason`. These camelCase field names are the stable JSON contract of `diff.Change`, `diff.Parent`, and `diff.Suppression`. Render failures still produce stderr diagnostics and exit status 1; JSON on stdout can contain other producers' changes.

`--allow-missing-crds` (default off; `FLATE_ALLOW_MISSING_CRDS`) accepts CRD dependencies that are absent from the offline inputs because the cluster installs them at runtime.

**Build substitution inputs.** These options also work with `get`, `test`, and both sides of `diff`.

| Flag                                    | Environment                                                            | Behavior                                                                                                                                          |
| --------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--substitute-from <file>` (repeatable) | `FLATE_SUBSTITUTE_FROM`: paths separated by the OS path-list separator | Supply ConfigMap/Secret values for exact `postBuild.substituteFrom` identities; bootstrap-known repository objects and producers take precedence. |
| `--substitute KEY=VALUE` (repeatable)   | `FLATE_SUBSTITUTE`: CSV comma-separated pairs                          | Override referenced values and authored inline substitutions, in that order, whenever `postBuild` is present.                                     |
| `--strict-substitutions`                | `FLATE_STRICT_SUBSTITUTIONS`: boolean                                  | Fail undefined variables without defaults; disabled by default.                                                                                   |

Explicit flags replace the corresponding environment input, including `--strict-substitutions=false`. File paths are relative to the invocation's working directory. Files accept multiple YAML documents, each a core `v1` ConfigMap or Secret with explicit name and namespace; empty/null documents are skipped. Later files/documents replace duplicate kind/namespace/name identities as whole objects. Later overlay pairs replace earlier keys; empty values and embedded equals signs are valid. CLI pairs retain commas; use CSV quoting for commas within environment values.

Supplied manifests contain CI-side test values, satisfy only matching postBuild references, and are never emitted, diffed, or used to select changed-only consumers. They cannot supply Helm values, source authentication, certificates, or proxies. Supplied Secrets bypass SOPS processing and placeholder wiping. Required references still need matching objects; an overlay cannot satisfy them or enable substitution without `postBuild`.

Repository precedence is frozen after discovery and before parallel reconciliation. A known collision logs one warning with its input file and identity. A producer discovered only during rendering does not replace an accepted supplied source, including for consumers discovered later. Strict expansion uses Flux's engine: defined empty values, `${VAR:=default}`, `${VAR:-default}`, and escaped `$${VAR}` keep their usual behavior. Resources with the `kustomize.toolkit.fluxcd.io/substitute: disabled` annotation or label bypass expansion.

**Default output filters.** `--skip-kinds` defaults to `Secret,CustomResourceDefinition` and replaces the whole exclusion list when set. Pass `--skip-kinds CustomResourceDefinition` to include Secrets, `--skip-kinds Secret` to include CRDs, or `--skip-kinds ''` to include both. `--skip-kinds Job` replaces the defaults and permits rendered Secrets and CRDs, subject to the HelmRelease CRD policy. To exclude Job while preserving the previous exclusions, use `--skip-kinds Secret,CustomResourceDefinition,Job`. Build's `--only-kinds CustomResourceDefinition` emits only CRDs and removes them from the default exclusions; an explicit `--skip-kinds` list wins over inclusion. Filtering affects emitted objects, while dependencies remain available. These filters are distinct from `--allow-missing-secrets`, which gates source auth and generated HR values Secret readiness.

**Scoping the scan.** A gitignore-syntax `.krmignore` at `--path` filters which files the scan loads (`!` re-includes work, so an allowlist like `/*`, `!/common/**`, `!/environments/production/**` mirrors a `GitRepository` `spec.ignore`). Sources also honor their conventional `.sourceignore` files.

**CLI upgrade notes.** `--skip-crds`, `--skip-secrets`, and `--only-crds` and their environment variables are replaced by the kind lists above (`FLATE_SKIP_KINDS` and build-only `FLATE_ONLY_KINDS`). Previous `--skip-secrets=false` becomes `--skip-kinds CustomResourceDefinition`; `--skip-crds=false` becomes `--skip-kinds Secret`; both false becomes `--skip-kinds ''`; `--only-crds` becomes `build --only-kinds CustomResourceDefinition`. `--krmignore`, all four `--source-retry-*` flags, and `--skip-schema-validation` are removed along with their environment variables. Source retries use 3 total attempts with 200 ms minimum wait, 3 s maximum wait, and 0.1 jitter. Helm schema validation follows `spec.install.disableSchemaValidation` by default and `spec.upgrade.disableSchemaValidation` with `--is-upgrade`; unresolved value placeholders remain exempt. Profiling uses only `FLATE_PROFILE` (`cpu`, `mem`, `block`, `mutex`, or `trace`) and `FLATE_PROFILE_OUT` (default `.`); `--profile` and `--profile-out` are removed; use `set -gx FLATE_PROFILE cpu` and `set -gx FLATE_PROFILE_OUT ./profiles` in fish before invoking flate. `--stream` and `FLATE_STREAM` are removed by the inherited U0 change; output stays buffered.

**Cache.** An empty cache root keeps source fetches in memory. Set `--cache-dir` or `FLATE_CACHE_DIR` to persist sources and eligible completed Helm and Kustomize renders through one cache owner. Source freshness follows Flux intervals. `flate cache gc` requires an explicit root: it retires named tags by payload last-use, prunes unreachable Git generations, and delegates blob reclamation to ORAS graph GC.

Mutable Git sources refresh at the source interval, and mutable remote Git bases use the referencing Kustomization interval. Zero interval refreshes each run. Only commit-only references and raw hash bases are immutable; a branch plus a commit still requires validation. Compatible HTTP operations share connections within one invocation, with authentication kept on each client. Customized Git operations retain private transports: go-git v5.19.3 `plumbing/transport/http/transport.go`, `(*client).fetchTransport`, calls the mutating LRU `transports.Get` under `RLock`. Its `transportOptions.proxyURL` key is built in `newSession` (`plumbing/transport/http/common.go`) and passed to `addTransport`; authenticated proxies receive fresh `url.URL.User` pointers and therefore distinct keys. These paths can open a connection per operation.

In a container, flate sets its soft memory limit to 80% of the cgroup limit unless `GOMEMLIMIT` is set (`AUTOMEMLIMIT` adjusts the ratio; `AUTOMEMLIMIT=off` disables it).

## Changed-only mode

`--path-orig` flips every command into change-aware reconcile. flate diffs the two paths, walks ownership backwards (longest matching Flux KS `spec.path`, including `spec.components`, plus any file a KS reads through a `resources:` reference outside its `spec.path`), and reconciles only the touched subtree plus its content dependencies.

In the keep-set: direct file edits, chart sources, KS `sourceRef`, HR `valuesFrom`, kustomize components (touching a shared component re-renders every consumer).

Out: `dependsOn` (reconcile-ordering, not content — skipped resources still get marked `Ready` so downstream waits unblock) and meta-Kustomizations that don't claim the deeper file.

```bash
git worktree add ../baseline main
flate diff ks --path ./kubernetes --path-orig ../baseline/kubernetes
```

`--path` can point at a narrow Flux entry like `./kubernetes/flux/cluster`; flate iteratively follows each loaded KS's `spec.path` to discover the rest of the tree.

`--base <rev>` materializes the rev's tree instead of needing a second worktree, and `flate diff` without either flag picks the merge-base with the branch's upstream. A rev the checkout doesn't hold (a `fetch-depth: 1` clone of another branch, or files written over `git init` with only a remote) is fetched from `origin`, so `--base main` works there too.

## Source kinds and auth

| Kind               | Status         | Auth (`spec.secretRef`)                                                                                                                    |
| ------------------ | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `GitRepository`    | `generic` only | HTTPS: `username` + `password` or `bearerToken`. SSH: `identity` (+ optional `password`, `known_hosts`). `github`/`azure`/`aws` fail loud. |
| `OCIRepository`    | `generic` only | `.dockerconfigjson`. Unset or missing offline: `--registry-config`, then `~/.docker/config.json`. `aws`/`gcp`/`azure` fail loud.           |
| `HelmRepository`   | full           | HTTP basic: `username` + `password`; OCI flavor routes through the OCI puller.                                                             |
| `HelmChart`        | full           | Inline (`HR.spec.chart`) and standalone CRD.                                                                                               |
| `Bucket`           | `generic` only | `accesskey` + `secretkey`. `aws`/`gcp`/`azure` fail loud — use static creds.                                                               |
| `ExternalArtifact` | `file://` only | `status.artifact.url` must be a local path.                                                                                                |

flate renders your own repo offline, so Secret values pass through verbatim — only SOPS ciphertext is wiped to `..PLACEHOLDER_<key>..` (flate can't decrypt it, and raw `ENC[…]` poisons rendering). A wiped (or genuinely missing) auth value is treated as missing — auth fails with a clear "missing username/password" instead of attempting auth with the placeholder. See [Behaviors](#behaviors) for `--allow-missing-secrets`, which soft-skips affected sources end-to-end.

## Behaviors

**Chart digest tracking** - direct OCIRepository chart references include a digest suffix in chart versions. flate disables tracking when file-loaded HelmRelease values contain `DisableChartDigestTracking=true` under `instance.kustomize.patches[]`, with no target or `name: helm-controller`, including Kustomization-owned releases. `--disable-chart-digest-tracking=true/false` or `FLATE_DISABLE_CHART_DIGEST_TRACKING=true/false` overrides detection; the CLI flag wins. Revision validation and full source cache identity apply in both modes. Gates in directly committed FluxInstance CRs or Deployment patches require the override.

**SOPS** — `spec.decryption` is not implemented. Encrypted Secret/ConfigMap values get wiped to `..PLACEHOLDER_<key>..` (flate can't decrypt offline, and raw `ENC[…]` ciphertext poisons downstream rendering). Cleartext Secret values are NOT wiped — flate renders your own repo, not a live cluster. Downstream `postBuild.substituteFrom` lookups resolve a SOPS value to the placeholder rather than failing.

**`spec.suspend`** — honored on every reconcilable CR. Suspended resources mark `Ready / "suspended"` and produce no rendered output.

**Producer-aware auto-skip (ExternalSecret / SealedSecret)** — on by default, no flag. When a referenced Secret is missing but an in-repo ExternalSecret or SealedSecret declares it as a target (`spec.target.name` / `spec.template.metadata.name`), flate has positive evidence it materializes live and skips it automatically: a source whose auth `secretRef` is producer-backed marks `Ready / "skipped: …"`; a HelmRelease `valuesFrom` ref with a declared producer is omitted so the release renders with the remaining values. This removes the need for `--allow-missing-secrets` in the common ExternalSecrets/SealedSecrets setup. A missing Secret with **no** declared producer still fails loud — a real typo is never silently swallowed. The scan reads the producer's _static_ target name; a kustomize `namePrefix`/`nameSuffix` that rewrites the generated Secret name (or an ES generated by a ResourceSet, for source auth) is not followed, and those fall back to fail-loud or the flag below.

**`--allow-missing-secrets`** — off by default; the blanket fallback for missing secrets that have no declared in-repo producer. When set, _any_ source whose auth `secretRef` is missing or PLACEHOLDER-wiped marks `Ready / "skipped: …"` instead of `Failed`, and consumers (KS `sourceRef`, HR `chartRef`) propagate the skip so `flate test` reports SKIPPED rather than a cascade of FAILED. HelmRelease `valuesFrom` Secret/ConfigMap refs that cannot materialize offline are omitted so the release can render with the remaining values. Absent or wiped auth, cert and proxy material skips the whole source when a producer exists or `--allow-missing-secrets` is set; malformed real material fails loud.

**`--force-generic-provider`** — off by default. A `GitRepository`, `OCIRepository`, or `Bucket` whose `spec.provider` is not `generic` fails loud, since flate implements neither cloud workload identity nor the GitHub App token exchange those providers authenticate with. This flag routes them down the generic credential path instead, and what that yields depends on `spec.secretRef`. With one declared, static credentials render offline, and a missing or PLACEHOLDER-wiped Secret marks `Ready / "skipped: …"` under `--allow-missing-secrets` rather than blocking every consumer. With no `secretRef` — the usual shape for `azure`/`aws` workload identity — Git and Bucket attempt anonymous access and OCI falls back to `--registry-config`, then `~/.docker/config.json`, so a private source fails on authentication and is **not** soft-skipped.

**`spec.dependsOn[].readyExpr` (CEL)** — evaluated against `self` and `dep` projections, matching upstream kustomize- and helm-controller binding:

```yaml
dependsOn:
  - name: infra-controllers
    readyExpr: |
      dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")
```

**Substitution opt-out** — the `kustomize.toolkit.fluxcd.io/substitute: disabled` label or annotation is honored per-resource, matching kustomize-controller. Used for ConfigMaps embedding bash array expansions envsubst can't parse.

**Signature verification** — flate does **not** verify signatures. `spec.verify` on `OCIRepository` (cosign) and `GitRepository` (PGP) is ignored: the artifact is pulled and rendered unconditionally, with no warning. flate is an offline renderer of what Flux _would_ apply, not an admission gate — signature enforcement belongs in-cluster (see [Limits](#limits)).

**ResourceSet inputs (`inputs` / `inputsFrom`)** — a `ResourceSet` renders its `resources` / `resourcesTemplate` once per input set. Inline `spec.inputs` and `spec.inputsFrom` both contribute sets; each `inputsFrom` entry references a `ResourceSetInputProvider` by `name` or by label `selector` (scoped to the ResourceSet's namespace). The built-in `inputs.provider` block on every set reflects the _sourcing_ CR's `apiVersion`/`kind`/`name`/`namespace` — the referenced provider for `inputsFrom`, the ResourceSet itself for inline.

Combination follows `spec.inputStrategy`: **Flatten** (default) concatenates all sets (`<< inputs.foo >>`); **Permute** Cartesian-products across providers, nesting each under its normalized name (`<< inputs.<provider>.foo >>`) plus a synthetic `inputs.id`, capped at 10000 permutations. A ResourceSet that _emits_ `ResourceSetInputProvider` objects is resolved by the discovery fixed-point pass, so a later ResourceSet's `inputsFrom.selector` picks them up (the two-stage namespace→deployment pattern). `Static` providers export `spec.defaultValues`; for dynamic providers, pre-bake `status.exportedInputs` to render them offline (see [Limits](#limits)).

## Limits

flate is rendering-only.

- **Custom resource scope.** Cluster-scoped custom resources unknown to
  Kustomize's schema, such as a ClusterIssuer or a kyverno or NVIDIA
  ClusterPolicy, receive a Kustomization `targetNamespace` or HelmRelease
  release namespace in rendered output, as `flux build` prints them. The API
  server clears the namespace on apply. flate does not infer custom resource
  scope from CRDs.

- **No SOPS decryption.** SOPS-encrypted values are wiped to a placeholder; pre-decrypt if you need them in the diff. (Cleartext Secret values render as-is.)
- **No signature verification.** `spec.verify` on `OCIRepository` (cosign) and `GitRepository` (PGP) is ignored — artifacts are pulled and rendered unconditionally. flate shows what Flux would apply; it does not gate on signatures. Enforce signatures in-cluster.
- **No cloud workload identity.** `spec.serviceAccountName` is a no-op; use static creds in a Secret.
- **No `healthChecks`.** flate tracks resource readiness, not status conditions of rendered objects.
- **`ResourceSetInputProvider`: `Static` resolves offline** (from `spec.defaultValues`). Dynamic providers (GitHubPullRequest, GitLab, OCIArtifactTag, ExternalService, …) need network access and contribute zero inputs — unless you pre-bake them by setting `status.exportedInputs` on the provider manifest, which flate honors directly (see [Behaviors](#behaviors)).
- **Diff output isn't a source patch.** `flate diff` defaults to dyff's human-readable report; `-o github` emits path-keyed syntax (`@@ <path> @@`) that GitHub's diff lexer renders natively, `-o diff` a standard unified diff, and `-o html` a self-contained, syntax-highlighted HTML page — a filterable left tree (with `j`/`k` keyboard nav) grouping each changed resource under its Flux `HelmRelease`/`Kustomization` → kind, per-resource side-by-side or unified views with word-level change highlighting, and a light/dark theme — for browser or CI-artifact review (`flate diff all -o html > diff.html`). All forms diff the _rendered_ manifests on each side, so none apply with `patch` / `git apply` against your repo — use the rendered output of `flate build` if you need a literal patch.
- **Unavailable output is withheld from diffs.** Objects from failed or suspended `Kustomization` and `HelmRelease` producers are withheld, including when both sides are unavailable. One-sided soft skips are also withheld. Document diffs disclose affected producers in the output; `diff images` writes the same notes to stderr before extracting images. Render failures retain a non-zero exit code. Producer paths affect presentation, so moving a producer between paths does not by itself report its resources as added or deleted.

## Architecture

```
discovery → Store ⇄ events ⇄ controllers (source · kustomization · helmrelease)
```

Pipeline: bootstrap-source seed → loader pre-pass excluding `configMapGenerator`/`secretGenerator` data files → file walk → `spec.path` + ResourceSet fixed-point expansion → one entry-bootstrap seam for seeded input, missing `GitRepository`/`OCIRepository`/`Bucket` refs, and self-referential Git sources, with inferred OCI/Bucket artifact-root subtrees frozen at publication → namespace inheritance → parent index → `dependsOn` cycle preflight → change-filter → controllers fire → render → render-time keep-set extension for emitted children → orphan demotion → output.

The Store is the single source of truth. Every stored manifest is immutable; mutation routes through `Store.Mutate[T]` (clone, mutate, AddObject). Helm chart loads coalesce through a per-path keylock — N parallel reconciles of the same chart issue exactly one parse.

## Library use

`pkg/orchestrator` is the embed entry point.

```go
import (
    "context"
    "github.com/home-operations/flate/pkg/orchestrator"
)

ctx := context.Background()
o, _ := orchestrator.New(ctx, orchestrator.Config{Path: "/path/to/cluster"})
res, err := o.Render(ctx)
// res is non-nil even when err != nil — partial output stays usable.
for id, docs := range res.Manifests {
    // rendered YAML docs for the KS / HR with this id
}
for id, info := range res.Failed {
    // structured failure list keyed by NamedResource
}
```

SDK callers that inject `source.Network` own its lifetime: join renders and close TreeCache workers before calling `Network.Close`. Orchestrators borrow injected networks; an internally created network closes after its render or both `RenderTrees` sides finish.

Low-level OCI, Bucket and HelmChart fetchers require a caller-owned, non-nil `source.Network`: set `oci.Fetcher.Network` and `bucket.Fetcher.Network`, or pass it through `helmchart.Config.Network` to `helmchart.New`. These fetchers borrow the network; the caller must join all fetches before calling `Network.Close`.

Other entry points worth knowing:

- `Orchestrator.WithFetcher(kind, f)` — swap any source fetcher (in-memory fakes for tests, custom kinds).
- `Store.AddListener(kind, fn, replay)` - the only store observer; payloads are `manifest.BaseManifest`, `StatusInfo` or `Artifact` by `EventKind`.
- `helm.Prepare(hr, lookup, provider)` then `helmClient.Template(...)` and `manifest.SplitDocs(...)` — render one HelmRelease without the orchestrator. `lookup` is a `manifest.HelmChartLookup` (`func(manifest.NamedResource) *manifest.HelmChartSource`). `kustomize.Prepare(ks, provider, nil)` is the symmetric helper for Kustomizations.
- `discovery.Run(ctx, Config{Path, Store, WipeSecrets})` — load phase as a standalone unit.
- `change.Filter.AddEmitted(emitter, child)` — extend the changed-only-mode keep set at runtime when a custom controller emits a child that wasn't visible at filter-build time; records the emitter→child edge. Call BEFORE `Store.AddObject(child)` so the synchronous listener sees the extended set.
- `Store.Mutate[T]` — clone-then-AddObject helper encoding the immutability contract. See [`pkg/manifest/doc.go`](pkg/manifest/doc.go) for the full rule.

## Development

```bash
go build ./cmd/flate
go test ./...
go test -race ./...
golangci-lint run ./...
```

Tool versions pin via [mise](https://mise.jdx.dev). Testdata lives in [`testdata/`](testdata/); the scenario packages in [`test/e2e`](test/e2e/) run the cobra command tree in process through one shared harness. Run them with `go test -race ./test/e2e/...`; no freshly built binary is needed.

## Releases

Release artifacts (archives, the Homebrew cask, SBOMs, and Cosign signatures) are built and published with [GoReleaser Pro](https://goreleaser.com/pro/). If it's useful to your own projects, consider [sponsoring its author](https://github.com/sponsors/caarlos0).

## Usage as pre-commit hook

This repo provides a [pre-commit](https://pre-commit.com/) hook, which runs `flate test all`.
It can also be used with [prek](https://prek.j178.dev/).

Example `.pre-commit-config.yaml`:

```yaml
---
repos:
  - repo: https://github.com/home-operations/flate
    rev: v0.7.0
    hooks:
      - id: flate
        verbose: true # optional
```

## License

AGPL-3.0. flate borrows behavior and test fixtures from [flux-local](https://github.com/allenporter/flux-local) (Apache-2.0).
