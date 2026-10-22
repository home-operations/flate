# AGENTS.md: flate

flate renders Flux (Kustomization, HelmRelease, ResourceSet, sources) offline as one static
binary: no cluster, no kubectl, no shellouts; helm, kustomize, git and OCI are linked libraries.
Output matches Flux bit-for-bit and is byte-identical run to run. Speed is the point:
changed-only renders, a bounded parallel DAG, aggressive dedup. `pkg/` is a public SDK that
konflate embeds.

## Working here

- **Agent setup.** If `.claude/settings.local.json` is missing, create it first with
  `{"attribution": {"commit": "", "pr": "", "sessionUrl": false}}`. It is personal and gitignored.
- **AI policy.** The org [AI Usage Policy](https://github.com/home-operations/.github/blob/main/CONTRIBUTING.md#ai-usage-policy)
  applies. The PR body follows the org template (Overview, Additional information, Requirements)
  and keeps its Requirements items. The AI disclosure there names each model that contributed,
  one line per model with its effort and what it did, and is the only place a PR mentions AI.
- **Titles and commits.** PR titles are [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
  such as `fix(schedule): ...`; they become the squash commit and drive release-please, so they
  never carry bead, workflow or run ids. Sign off commits (`git commit -s`).
- **Pushing.** Never commit, push or open a PR unless asked. A branch pushed for review holds one
  signed-off commit whose message is the PR title, rebased onto `origin/main`, with
  `CGO_ENABLED=0 go build ./...` and `mise run test` passing after the rebase. A rebase conflict
  outside `_test.go` stops the push. Never merge or approve; maintainers gate the merge queue.
- **Boundaries.** Never touch secrets or gitignored files, except the agent settings file above and a
  build's own artifact directory under `plans/`. Verify library APIs against the module cache or
  pkg.go.dev, not memory.

## Design rules

- **Smallest diff.** Solve the stated problem: no speculative abstractions, no interface or options
  struct with one caller, no new flags, no new dependencies, no drive-by refactors. Remove what
  your change orphans. Clean up touched code within the same concern (simplify, delete dead code,
  fix stale comments); leave other dead code alone and mention it. Approved dependency
  exceptions: `automemlimit`, `doublestar/v4`.
- **Replace, don't add.** A change that introduces a new way migrates every consumer and deletes
  the old path in the same PR; never land a new path beside the old one. In a planned series one
  PR may grow when a later PR deletes; the series as a whole shrinks. PRs state production lines
  as +added/-removed.
- **Redesign on entry, never bolt on.** A change that adds a new case to a concern that already has
  a mechanism (a new source kind, producer, input or key) owns the unification: it redesigns the
  concern so every case goes through one seam (one classification, one derivation, one
  publication, one warning) and deletes the mechanisms it folds in. A sibling path, or a per-kind
  branch in a consumer, is a blocking finding in the plan and in review. A fix inside an existing
  mechanism does not trigger this; it names the remaining multiplicity in its PR.
- **No legacy paths, ever.** No code whose purpose is an earlier flate layout, format, key or
  flag: no cleanup commands, migrations, fallbacks, tolerances or aliases. The word "legacy" in
  code, help text or docs is itself a finding. Replacing a layout deletes the old one; users
  discard old cache directories; upgrade notes go in the PR body.
- **Call upstream.** Use helm, Flux, kustomize, go-git, oras and the stdlib instead of
  re-implementing them; if upstream lacks a hook, post-process its output. No compatibility shims
  or aliases past their last caller. `internal/kustomize.Build` is the one approved replacement
  (it keeps the builtin schema loaded across builds).
- **The SDK.** List every exported `pkg/` API or `Warning.Category` change in the PR; remove unused
  or test-only exports when the result is leaner. konflate is not a constraint: never keep an
  export, option or cache for it; it migrates.

## Layout

`cmd/flate` is the entrypoint. `internal/cli` owns flags, exit codes (0 or 1), signal handling
and `slog.SetDefault`. Everything else is `pkg/`. Dependencies flow one way and never upward;
`depguard` in `.golangci.yml` enforces this graph:

```
manifest <- store <- {loader, values, depwait, change, source}
         <- {kustomize, helm, discovery} <- controllers/base <- controllers/* <- orchestrator <- internal/cli
```

Leaf packages (`tree`, `manifest`, `task`, `schedule` behind its `Dispatcher` seam, `source/{safepath,sourceignore,ssrfguard}`,
`internal/assert`) are the graph's bottom; `depguard` holds the exact rules.

**Concern owners.** Each concern is decided in exactly one place:

| Concern                                                                                                                 | Owner                          |
| ----------------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| Identity and per-kind behaviour                                                                                         | `manifest`                     |
| Objects, relations, emitted children                                                                                    | `store`                        |
| Entry bootstrap (seeded source, missing source refs of any kind, self-referential sources, artifact roots) and aliasing | `discovery`                    |
| Cache paths and fetched content                                                                                         | the cache owner under `source` |
| Substitution                                                                                                            | `values`                       |
| Change detection                                                                                                        | `change`                       |
| Readiness                                                                                                               | `depwait`                      |
| Scheduling                                                                                                              | `schedule`                     |
| Render selection and lifecycle                                                                                          | `orchestrator`                 |
| Flags and exit codes                                                                                                    | `internal/cli`                 |

**Vocabulary.**

- _bootstrap source_: the working tree admitted as the cluster's own source.
- _alias_: a synthetic source that resolves to that tree.
- _seed_: a file-indexed object discovered before any render.
- _orphan_: a seed no Kustomization owns, promoted as a static root.
- _emitted child_: an object a producer publishes through `Emit`.
- _fingerprint_: the content key the cache owner derives.
- _changed-only_: rendering only what the diff base reaches.

## Invariants

- **One mechanism per concern.** Store events (`Store.AddListener`) are the only observer
  mechanism; no `On*` hooks. Use an event only where a direct call would invert the dependency
  flow; otherwise call directly. Children are published through `base.Controller.Emit`. Per-kind
  behaviour is a method on the manifest type, never a kind switch in a consumer.
- **One fingerprint.** Cache and change keys are `manifest.Fingerprint` over content, derived
  where the cache lives; consumers never hash. No hand-rolled hashers, tags, framing or mtimes.
- **Immutable manifests.** Clone, mutate the copy, `AddObject` again. Never write to an embedded
  `Spec` after parse.
- **Listeners never block the store.** They run inline; writers snapshot them with
  `fireUnderLock` and dispatch after unlocking. Locking more than one shard uses
  `lockAll`/`rLockAll`.
- **The scheduler holds no store, pool or dispatcher under its mutex.** Drop `mu` before
  `tasks.Go`. Termination is a structural fixpoint plus drain, never a timeout.
- **One global serialization point.** Every krusty build holds `internal/kustomize.BuildMutex`;
  add no other on the render path.
- **Changed-only mode.** `KeepEmitted`/`AddEmitted` before `AddObject`.
- **Secrets.** Only SOPS ciphertext is wiped (`..PLACEHOLDER_<key>..`). Missing secrets fail loud
  unless a producer exists or `--allow-missing-secrets` is set; absent or wiped auth, cert and
  proxy material follows the same whole-source skip; malformed real material fails loud.
- **The cache owner alone chooses storage paths.** Flate-owned fetched-content writes go through
  `safepath` and the owner's `os.Root` boundary with atomic publication; ORAS and go-billy
  writers stay library-owned. The SSRF guard is process-global and opt-in via `RestrictEgress`.
- **Deterministic output.** Order never depends on goroutine scheduling; sort anything derived
  from a map.
- **Flux parity beats strictness.** Where Flux renders, flate never fails: conflicting producers
  yield one object, a deterministic winner and one `ConflictingProducers` warning.

## Performance and concurrency

- Reconcile bodies run only through `task.Service.Go`, bounded by `--concurrency` (default
  `NumCPU*4`); wrap waits on other slot-gated work in `task.YieldSlot`. errgroup only for fixed
  fan-out.
- No new locks, widened critical sections, channel round-trips or serialization on the render,
  store, discovery, change-detection or scheduler paths. Prefer immutable snapshots, existing
  shards, atomics, and work computed once before the parallel phase.
- Deduplicate expensive work by key (`keylock`, per-URL `sync.Once`, fingerprint dedup, template
  and disk caches) instead of re-rendering, re-fetching or re-parsing.
- Allocation-free hot paths stay allocation-free; allocs/op never rises on an existing benchmark.
- A PR touching a package with benchmarks pastes `benchstat`: `COUNT=10 mise run bench` on base
  and branch, then `benchstat <base>.txt <branch>.txt` (results in `bench/results/`).
  `bench/baseline.txt` is a placeholder until `mise run bench-baseline` regenerates it. A
  regression over 5% on a hot path, or any allocs/op increase, blocks review unless argued with
  numbers and accepted explicitly.
- Concurrent code gets a `-race` test at concurrency >= 2, not a serialized stand-in.

## Code style

- **Go.** The version is `go.mod`'s directive, pinned to the lowest patch; `.mise/config.toml`
  pins the toolchain. Write idiomatic Go for it: `slices`, `maps`, `cmp.Or`, `strings.Cut`,
  `for range N`, `min`/`max`, generics constrained on `manifest.BaseManifest`, `errors.AsType`.
  Run `go fix` after touching a package. No `interface{}`, naked returns, or reflection on hot
  paths. No cgo, ever: builds use `CGO_ENABLED=0`; the race detector needs cgo,
  so run `mise run test` as configured.
- **Context.** `ctx context.Context` is the first parameter of anything doing I/O and is never
  stored in a struct. `context.Background()` belongs to `internal/cli`; `forbidigo` enforces it and
  its exclusion list is the record of shared work that outlives one caller.
- **Errors.** `fmt.Errorf("<lowercase op>: %w", err)`; tag with a `manifest.Err*` sentinel as
  `fmt.Errorf("%w: detail", manifest.ErrInput)`. Every domain error wraps `manifest.ErrFlux`.
  Classify with `errors.Is`/`errors.AsType`, never by string. Panic only on impossible state.
- **Logging.** Package-level `log/slog` (`slog.Debug/Warn`) with a `"pkg: "` message prefix; the
  message is a constant and values are attrs. No logger threading.
- **Signatures.** At most four parameters (median is one); beyond that an `Options`/`Config`
  struct. No functional options. Receivers are one or two letters, consistent per type.
- **Interfaces belong to the consumer.** A package declares a small unexported interface listing
  only the methods it calls; constructors return concrete types. An exported interface exists
  only where an SDK embedder must implement it (`schedule.Dispatcher`, `values.Provider`), and
  the PR names that embedder.
- **Comments.** Package and exported docs are lint-enforced (one sentence starting with the
  identifier). Default to no other comment; add one only for a constraint or
  rationale (MUST, never, invariant, an upstream quirk with its issue). Banned: step narration,
  section banners, restating the name above a function, describing your own edit, commented-out
  code, unprompted TODO or FIXME. Delete narration you find in code you touch.
- **Elegance.** Names state intent; small functions with one job and straight-line flow; types
  that make wrong states unrepresentable; no wrappers, single-use helpers or pass-through types;
  one obvious way, consistent with the surrounding code. Working but convoluted, repetitive or
  clever code is a review finding.

## Tests

- Stdlib `testing` only; never testify or Ginkgo. Names: `TestThing_Behavior`, `TestE2E_*`
  (in-process via `cli.Run`, no build tags, no network), `BenchmarkThing_Case` with `b.Loop()`.
- Table-driven with `t.Run`; helpers call `t.Helper()`.
- Tests and subtests call `t.Parallel` unless they touch process-global state: `t.Setenv`,
  `t.Chdir`, `slog.SetDefault` or `log.SetOutput` (including through `cli.Run`),
  `ssrfguard.Restrict`, go-git protocol registration, a package variable, or
  `testing.AllocsPerRun`. Such a test stays serial. Never call `t.Parallel` inside a
  `synctest` bubble.
- A matrix axis exists only where the code under test branches on it. Worker counts vary only
  where concurrency is the subject; elsewhere use one count >= 2. Determinism is shown with
  two runs. No iteration loops to provoke races: use explicit synchronization or
  `testing/synctest`; no real `time.Sleep` outside benchmarks.
- `TestE2E_*` asserts only through `cli.Run`; anything else belongs in the package's tests.
- Fixtures: `t.TempDir()` plus `testutil.WriteFile`; shared corpora live in `testdata/<scenario>/`
  and are documented in `testdata/TESTDATA.md`. Assert with `internal/assert` or `cmp.Diff`
  (`-want +got`). No golden files.
- Every fix lands with a regression test for the reported symptom.
- Test flate's own logic: nothing an upstream library already covers, nothing for trivial
  guards; adapt an existing test before adding one.

## Before a PR

```
mise run lint
go mod tidy && git diff --exit-code go.mod go.sum
mise run test        # go test -race, all packages including test/e2e
mise run vulncheck
mise run bench       # when touching a package with benchmarks; paste benchstat, base against branch
```

CI runs the same plus a goreleaser snapshot and workflow lint; "Build Success" is the merge gate
behind a squash-only merge queue; lint enforces the shape of `//nolint` directives.
