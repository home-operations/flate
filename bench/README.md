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
