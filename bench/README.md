# Benchmarks

This directory hosts the perf-tracking infrastructure introduced in Phase 0
of the performance refactor (see plan at the repo root).

## Layout

- `baseline.txt` — canonical baseline. Updated only by `mise run bench-baseline`.
- `results/` — timestamped run outputs (gitignored).

## Running

```
mise run bench                 # all benches, single run
BENCH=BenchmarkLoad mise run bench   # filtered
COUNT=10 mise run bench        # 10x for stable measurements
```

Outputs go to stdout and a timestamped file under `results/`.

## Updating the baseline

After a perf-PR lands, regenerate:

```
mise run bench-baseline
```

Commit `baseline.txt`; do not commit `results/` (it's gitignored).

## Comparing runs

Use `benchstat` for noise-aware comparison:

```
go install golang.org/x/perf/cmd/benchstat@latest
benchstat baseline.txt results/<new-run>.txt
```

Every Phase 1+ PR description should paste the `benchstat` output as the
performance delta.

## CLI workloads

`BenchmarkWorkCLI` runs generated workloads through `cli.Run`. It includes
named and namespace builds, eleven edit kinds, both complexity workloads,
full builds, concurrent OCI charts, a large index, shared acquisitions,
two cluster roots, and large JSON output.

Each row has separate `allocations` and `peak` arms. Use the allocations arm
for primary `ns/op`, `B/op`, and `allocs/op` comparisons. The peak arm samples
`HeapAlloc` at start, end, and every millisecond. Its `peak-heap-B` metric is
an observed lower bound above the starting heap. The sampler joins before
the row returns. Its time and allocations belong only to the peak arm.

Cold operations use an empty cache. Warm operations restore the same cache
seed from an unedited accepted build. Every operation creates a fresh
invocation. Fixture preparation and cache restoration stay outside the timer.
`setup-ns` reports fixture and seed preparation. `reset-ns/op` reports cache
restoration separately.

Complexity names distinguish one logical edit across many physical files from
one edited file among many candidates. `total-files`, `edited-files`, and
`changed-owners` describe the generated workload. Changed-file rows also report
`file-visits`, `reader-visits`,
and `raw-key-prepares` report a separate SDK execution with the same inputs
and cache regime. These metrics count actual work. They do not infer work
from elapsed time. Exact scale tests cover 10, 100, 1,000, and 5,000 files.

The acceptance budgets remain setup below one second, operations below
250 milliseconds, and added race-suite time at most 30 seconds.
`budget-overruns/op` reports the fraction of operations over the operation
budget. A nonzero result is an unmet budget. A passing benchmark only shows
that its exit and output assertions pass.

Run all workload arms through the existing entrypoint:

```fish
env BENCH=BenchmarkWorkCLI COUNT=10 mise run bench
```

Use a benchmark name that ends with `/allocations$` for a primary comparison.
Run the matching `/peak$` row separately. Keep both results in the comparison
report. CLI rows run serially because `cli.Run` changes process logging.
