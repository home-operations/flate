# Testdata

This directory holds fixtures used by `pkg/` unit tests and the `test/e2e/`
integration suite. Fixtures stay here while tests run in scenario packages beneath
`test/e2e/`, using one shared `test/e2e/internal/harness` package. Run all scenarios
with `go test -race ./test/e2e/...`.

| Package | Scenarios |
| --- | --- |
| `.` | ArtifactGenerator strategies, bootstrap inputs, partial failure and emitted changed-only diff |
| `sourceref` | Source references, pinned trees and discovery |
| `scope` | Selection, ownership, orphans, diff and change scope |
| `helm` | Helm listing, builds, diagnostics and stale values |
| `crds` | ResourceSet expansion and CRD availability |
| `substitution` | Substitution producers, data reading and inherited patches |

## Layout

- `simple/` - a hand-crafted minimal cluster used across the scenario packages.
  Exercises the Kustomization + HelmRelease pipeline end-to-end without any network
  access.
    - `cluster/` — Flux GitRepository, Kustomization, and HelmRelease objects.
    - `apps/` — kustomize-rendered ConfigMap + Namespace.
    - `charts/mychart/` — a tiny local Helm chart referenced via
      `sourceRef.kind: GitRepository`.

- `components/` — two consumer apps (`app-a`, `app-b`) referencing a shared
  kustomize component (`components/shared`). Exists to test change-detection
  propagation in `test/e2e/scope`: editing the shared component must surface in
  both consumers' diff output (`TestE2E_ComponentChangePropagatesToAllConsumers`) and an
  app-local edit must NOT propagate to the sibling
  (`TestE2E_NonSharedChangeDoesNotPropagate`).

- `parent-patches/` — the stripped-down bjw-s home-ops cluster pattern.
  A top-level `cluster-apps` Flux Kustomization injects
  `postBuild.substituteFrom` via `spec.patches` into every child KS rendered
  from `./apps`. The fixture also ships a SOPS-encrypted Secret alongside the
  patched leaf to exercise the wipe-to-PLACEHOLDER behavior end-to-end. Its raw data value uses complete SOPS AES256_GCM framing. Used
  by `test/e2e/substitution` tests `TestE2E_ParentPatchesPropagateToChildren`,
  `TestE2E_SOPSValueWipedToPlaceholder`, and
  `TestE2E_SubstituteDisabledAnnotation`.

- `orphans/` — a parent kustomize tree that lists only one child
  Kustomization (`wired`) while a second one (`orphan`) exists on disk but
  is intentionally NOT referenced. Real Flux would never reconcile the
  orphan; flate does not discover it. Used by
  `test/e2e/scope` test `TestE2E_OrphanedKustomizationNotDiscovered`.

- `resourceset-documents/` - ResourceSet JSON and YAML admission through SDK and CLI output: incomplete identities, nonrecursive list items, directives, duplicates, sanitized ciphertext with complete SOPS AES256_GCM framing, authored and explicit namespaces, and one tracked child render.

ArtifactGenerator CLI fixtures are created in temporary repositories by
`test/e2e/artifactgenerator_test.go`. `AllStrategies` combines nested directory
copying, YAML merging and a local gzip tar archive, then compares two CLI runs.
`PartialFailure` publishes outputs A and B before output C fails and checks that
their consumers still render. `BootstrapInput` selects a static generator when
only its consumer declaration changes. `BuiltBootstrapInput` patches a generator
copy input and UID in a bare Kustomize entry, then checks consumed output and
deterministic bytes. `EmittedChangedOnlyDiff` renders the generator from
a Component, changes its selected copy input, and checks both changed-only build
and forward/reverse diff with shallow and absent history on opposite sides,
without selecting an unrelated consumer. Each case runs with
two workers and compares two equal CLI runs. Consumed-input successor scheduling
remains deferred to X2 and is not asserted by these fixtures.

## Independent render model

`pkg/orchestrator/trees_model{,_render}_test.go` constructs bounded before/after
fixtures in temporary directories. `TestRenderTrees_IndependentModel` and
`FuzzRenderTrees_IndependentModel` share the byte decoder and four-side checker.
The first byte chooses a seed; the second chooses a feature variant, with its
high bit requesting directory comparison. Three-byte instructions compose
content, comment, rename, identity, namespace, path, suspension, unconsumed-file
and owner-removal edits. The `ff` prefix encodes the four metamorphic controls.
Unsupported combinations are counted. The decoder records replayable file and
producer operations, bounded to 12 producers, 48 files/documents, depth 4, three
namespaces, two sources, 4-KiB bodies and 24 operations. `0300` and `0301`
retain the optional-input directory and public committed-baseline witnesses.

Set `FLATE_E8_ARTIFACT_ROOT` to the build artifact directory to retain failing
bytes, readable worlds, operations, expected/actual identities and reproduction
argv. Diagnostic chart archives are rendered as hex. `TestRenderTrees_IndependentModel_Metamorphic`
checks disjoint growth, source renaming, reverse edits, equivalent placement and
two explicit successful replays. Fixture commits use go-git; no test subprocess
or shared generated fixture is required. The corpus includes pinned/alias and
second-source collisions with dirty-byte controls, local chart archives,
generators, components, ancestor-injected patches, owner/source/root/group moves,
ignore flips, empty output and unavailable data/CRDs. Shallow and missing-tree
controls assert baseline admission failure without claiming a completed render.
Future ResourceSet output semantics remain explicitly pending freeze.

## Upstream attribution

flate's controller architecture and behavior follow the design of
[`flux-local`](https://github.com/allenporter/flux-local). The flate
`testdata/simple` corpus is intentionally minimal and was authored from
scratch for the Go port; the larger `flux-local/tests/testdata/*` clusters
remain available in upstream for reference.

The `composed-owner-removal-orphan` input preserves native fuzz witness
`392c59303030303030`. Its ownerless package is eligible for static discovery;
a suspended or replacement owner retains ownership. Static store inventory and
selected output remain distinct, including source coordinates.
The `last-owner-local-chart` witness removes the final Kustomization while
changing a local Helm chart template; it retains the source alias and exact
HelmRelease admission expectations.

## E8 memory workloads

Routine tests are offline and run without environment overrides. Large fixtures
require `FLATE_E8_SLOW=1`; their normal skips mean `not-run`, never memory passes.
Fixtures use temporary directories and go-git. No test launches a process.
`internal/testutil/sparsegit.go` is shared fixture/counter support, not an SDK API.

| Test | Ordinary control | Explicit slow workload |
| --- | --- | --- |
| `TestRender_MemorySharedOwner/local` | Two fresh render instances, one memory cache owner, two producers/20 objects, identical output and one fill of a shared owner payload | None |
| `TestRender_MemorySharedOwner/home_plex` | Not-run | Pinned local `FLATE_E8_HOME`, `kubernetes/apps`, two fresh full SDK renders followed by public `HelmRelease media/plex` selection, one memory owner; label SDK startup/output encoding separately from the CLI row |
| `TestAcquire_MemoryRetirement` | Memory and disk backing, eight 32-KiB source files; warm reuse, refresh with retained old payload, result release/owner close, and bounded reacquisition | Same lifecycle with 64 one-MiB source files; run separately at default GOGC with `GOMEMLIMIT=128MiB` and `256MiB` |
| `TestAutoResolve_SparseGitMemory` | `/routine`: 24 committed files and a 4-KiB unchanged blob | `/exact`: 12,000 committed files and an identical unchanged 256-MiB blob, one changed consumed input, then disjoint committed growth |
| `TestSelection_SparseGitBodies` | `/routine`: calibrated comparison primitives on the same small fixture | `/exact`: same phases with exactly 12,000 files/256 MiB |
| `TestGitView_SparseBodyReads` | `/routine`: calibrated fresh pool before tree walk/shape | `/exact`: same phases with exactly 12,000 files/256 MiB |
| `TestRender_MemorySparseGit` | `/routine`: actual public AutoResolve inputs into RenderTrees, exact producer admission/terminal/output and disjoint-growth invariance | `/sparse-git`: same checks with exactly 12,000 files/256 MiB; two explicit render invocations before/after disjoint untracked growth |
| `TestAutoResolve_SparseHistoricalBodyUnavailable` | Remove the known unchanged committed blob while retaining checkout bytes; public admission and baseline reads still succeed | This fault injection is distinct from the intact 256-MiB row |
| `TestDetect_SparseDirectoryBodies` | Separate bounded directory comparison: 25 files per side, one same-size edit, 25 regular body opens per side | No large directory comparison substitutes for Git identity evidence |
| `TestRender_MemoryContained` | Not-run | Exactly 1,000 producers (500 Kustomizations and 500 HelmReleases), 10,000 ConfigMaps with 1,024-byte data payloads, shared Kustomize input/local Helm chart, concurrency 16; checks complete unique object and terminal producer membership |

The sparse fixture streams a sparse on-disk zero file through go-git's
`filesystem.Storage.LazyWriter` and `plumbing.NewHasher`, then stages that blob
identity in the Git index. It never builds a 256-MiB Go buffer. Both commits have
exactly the requested file count and the same unrelated blob identity. Fixture
construction/commit validation and Go test startup are harness overhead, outside
render metrics; a collector measuring the entire test child must label that cost
rather than subtract it or call it product-only RSS.

Reader counters wrap encoded objects, counting `Reader` opens separately for
metadata, all blobs and the known large blob. The gittree test installs a fresh
pool factory before any walk/shape operation. A small consumed blob calibrates
the actual pool path; counters reset before walk, shape/list/stat and consumed
read. The baseline test calibrates a separate test-local repository, resets for
baseEntries, candidates, committed comparison and dirty comparison, and never
passes a wrapped Storer to `selectChanges`. The public baseline inputs retain
concrete `*gittree.FS` policy dispatch. These are separate observations: public
AutoResolve admission, checkout hashing, filterChanges and render/fetch Reader
opens are not spanned by those counters and remain explicit instrumentation gaps.
Integration checks sibling exclusion and exact completed output; it does not
invent body counts for those gaps. The missing-historical-blob case supplies
independent public-route evidence without claiming intact large-row metrics.

Retention samples release render/results and close owners before explicit GC.
The cache test also reports a held-payload snapshot with `runtime.KeepAlive`.
Heap samples are process-wide observations, not attributed payload retention or
peak RSS, and have no global-equality assertion. Memory reacquires once after
close; disk source reuse restores the final payload without a new producer fill.
Source archive reuse is separate from persistent render-cache eligibility.

Focused routine proof (race instrumentation is separate from RSS collection):

```fish
go test -race ./pkg/orchestrator ./pkg/baseline ./pkg/source/gittree ./pkg/source/cache ./pkg/change -run 'Memory|Sparse|LazyBaseReads' -count=1 -v
```

Exact synthetic fixture correctness, one fixture at a time without reducing
reconcile concurrency:

```fish
env FLATE_E8_SLOW=1 go test ./pkg/baseline ./pkg/source/gittree ./pkg/orchestrator -run 'TestAutoResolve_SparseGitMemory/exact|TestSelection_SparseGitBodies/exact|TestGitView_SparseBodyReads/exact|TestRender_MemorySparseGit/sparse-git|TestRender_MemoryContained' -count=1 -v -p=1 -parallel=1
env -u GOGC FLATE_E8_SLOW=1 GOMEMLIMIT=128MiB go test ./pkg/source/cache -run '^TestAcquire_MemoryRetirement$' -count=1 -v -parallel=1
env -u GOGC FLATE_E8_SLOW=1 GOMEMLIMIT=256MiB go test ./pkg/source/cache -run '^TestAcquire_MemoryRetirement$' -count=1 -v -parallel=1
```

For the corpus SDK row the collector must supply `FLATE_E8_HOME` as an absolute
local copy at the pin below, set `FLATE_E8_SLOW=1`, and invoke
`go test ./pkg/orchestrator -run '^TestRender_MemorySharedOwner/home_plex$' -count=1 -v`.
Missing input on that explicit invocation fails. Pin/source/chart validation and
network availability are recorded by the external collector; the SDK test does
not infer corpus identity from the path. The SDK workload explicitly matches the verified CLI defaults for Git depth,
retry policy, concurrency, bundled Kubernetes version, test/CRD/Secret omission
and secret wiping. It sets `Config.DisableChartDigestTracking=true`, matching
the home-ops inventory; the ordinary SDK does not read CLI environment flags.

The external row IDs below describe measurement regimes, not collected results.
The inventory source is the installed pack's `assets/corpus/corpus-invocations.json`.
Current inventory pins: home-ops `d2a8c47f99f2614003f037e455b374e7b616edd0`,
angelnu `872f8a1baebedfdcce40c580b509e038d0242ec1`,
Biohazard `4e4bb6180d6417990dc5f195852730ed77a3142f`.
E8-5 must verify these and the historical absolute-target options before verdicts.

| ID | Executable argv after the resolved binary, cwd/input and cache regime | Authority |
| --- | --- | --- |
| MEM-3-COLD | `build hr plex -n media -p kubernetes/apps --log-level debug --no-progress`; cwd pinned home-ops; fresh child, no cache-dir; `FLATE_DISABLE_CHART_DIGEST_TRACKING=true` | CPU <=2 s, peak RSS <=250 MiB after exact workload eligibility |
| MEM-3-WARM | SDK test above; separately the MEM-3-COLD argv plus `--cache-dir` and the explicit absolute persistent directory, reused across fresh children | Approximately 100 MiB objective; report distance, no hard 100-MiB cap; never label a second default-memory process warm |
| MEM-B11-ANGELNU | `build all -p clusters/prod --log-level debug --no-progress --cache-dir` plus explicit absolute persistent directory; cwd pinned angelnu, warm reused directory | <=261 MiB target requires verified exact historical pin/options |
| MEM-B11-GC128 | `TestAcquire_MemoryRetirement` slow invocation above with `GOMEMLIMIT=128MiB`, plus B11 pinned source/all-render invocation under the same pressure; default GOGC | Soft runtime budget, report RSS/held and released heap/reacquisition; synthetic retirement does not stand in for corpus RSS |
| MEM-B11-GC256 | Same as GC128 with `GOMEMLIMIT=256MiB` | Same soft-budget authority |
| MEM-ALL-HOME | `build all -p kubernetes/apps --log-level debug --no-progress`; cwd pinned home-ops, default product concurrency, explicitly record cold/reused cache; inventory chart-digest environment override | Named resource evidence; this small tests-only lane has no extra comparative performance gate |
| MEM-ALL-BIO | `build all -p kube/clusters/biohazard/flux --log-level debug --no-progress`; cwd pinned Biohazard, default product concurrency, explicit cache regime | Same named-evidence obligation |
| MEM-SPARSE | Exact sparse invocations above; measure each clean uninstrumented test child separately and retain phase assertions, dimensions and setup overhead | No unrelated historical blob body on observed commit paths, no unrelated selected producer; uncovered phases remain gaps |
| MEM-CONTAINED | Exact `TestRender_MemoryContained` child, concurrency 16; clean test binary and delegated Linux cgroup, swap off | Proposed process peak 1 GiB/cgroup 2 GiB; report process/cgroup accounting separately; do not shrink fixture or silently promote proposal to gate |

Each collector row resolves concrete binary/test-binary argv arrays, cwd,
environment overrides, cache directory/regime and clean stamped identity.
Default GOGC means remove any inherited override; pressure rows override only
GOMEMLIMIT. SDK binaries carry their own startup cost and cannot be called clean
product CLI measurements. Persistent render-cache claims require an eligible
clean stamped product binary, independently of the unstamped hash-equality proof.
The collector records CPU user/system, peak RSS bytes (normalize platform units),
wall time, exit/signal, stdout digest, available work counts and missing counts,
provenance and calibration. Linux containment records effective memory/swap
limits, cgroup peak/events/OOM and teardown; macOS cannot satisfy that row.
External collector/containment delivery is prerequisite D1, owned by the pack.
`rusage.py` alone is insufficient. This lane does not install or patch a collector;
E8-5 must record missing delivery/coverage rather than treating these invocations
or test skips as measured passes.


## E8 synchronized liveness scenarios

These fixtures are generated under `t.TempDir()` and remain offline. Routine
variants run with bounded task concurrency 2 and 16. Store listeners record
immutable observations; consumers park outside callbacks. Scheduler assertions
read terminal state only after structural completion and task drain. Barriers
hold only the first `min(concurrency, runnableRoots)` active dispatch bodies.

| Family | Routine tests and ordering assertions |
| --- | --- |
| Shared DAG | `TestScheduler_BoundedDAG`, `TestDAG_BoundedPublications`: 160 producers, ten distinct ConfigMaps each, depth 64 and a 64-way diamond. Active body counts use deferred release, reach the bound and never exceed it. Exact producer/child membership, Ready status and drain are asserted. The orchestrator renders two explicit independent outputs and compares their bytes. |
| Publication and replay | `TestStore_AddListenerNoFlush_NoMissedConcurrentWrites` distinguishes pre-registration, overlapping and promised post-registration writes. `TestStore_ListenerSnapshots` captures dispatch under the existing shard lock, unlocks, changes registration and dispatches old/fresh snapshots. `TestRecordEmitted_ParkedSnapshotReplay` holds consumers across replacement and withdrawal, checks retained old objects/conditions and fresh replay/current membership. |
| Keep before visibility | `TestEmit_OwnershipPrecedesArrivals`, `TestFilter_AddExtendsKeepSetAtRuntime`, `TestDAG_KeepBeforeLateDataPublication`: selection precedes transformed child arrivals; the child is observed Blocked before late data release; the provider is selected, the sibling stays excluded, and the child finishes with the expected data and no stale identity. |
| Shared fetch cancellation | `TestAcquire_SharedWaiterCancellation` attaches two consumers to one blocked producer, cancels a waiter and completes the survivor with one fill. `TestRemoteInput_CancelQueuedGitBase` prevents canceled queued work/reacquisition. `TestAcquire_CancelStopsContextIgnoringProducer` and `TestRenderTrees_CancellationJoinsBeforeCacheClose` hold producer cleanup, then join before closing memory/disk owners and reject canceled post-close reacquisition. |
| Failure and panic | `TestScheduler_RegisteredDependencyGraphs`, `TestScheduler_RegisteredFailureRecovery`, `TestDAGDispatcher_RecoveredAndPrimaryFailures` cover cycles, missing dependencies, failed declaration replacement, released pins and exact failure/blocker membership. `TestService_PanicCountedAndRecovered`, `TestYieldSlot_PanicRestoresSlot`, `TestScheduler_RunDispatchPanic` prove counted panic and uncanceled completion. `TestFinalize_DispatchPanic` feeds the same poisoned service into finalization and requires an error. Existing `TestE2E_MissingLocalInputDiagnostics` verifies ordinary `cli.Run` failure translation. |

Exact large variants are `TestScheduler_ScaleDAG` and
`TestDAG_ScalePublications`. Each keeps 1,000 producers, 10,000 publications,
depth 64 and 64-way diamond fan-in at both task bounds; an ordinary test skip
is not a large-campaign pass. Run the large race campaign with:

```sh
env FLATE_E8_SLOW=1 mise exec -- go test -race ./pkg/schedule ./pkg/orchestrator -run '^(TestScheduler_ScaleDAG|TestDAG_ScalePublications)$' -count=1
```

E8-4 runs the routine package race proof. E8-5 owns the exact large race
campaign, full repository gates and the final candidate commit.
