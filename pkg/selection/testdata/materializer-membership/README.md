# Materializer membership counts

An overlay replaces source files for one Go command.
This runner instruments the three production collectors without changing their files.
The same Go assertions test scan and keyed implementations.
The runner requires Python, Go, and Git.

From the candidate checkout, run:

```fish
python3 pkg/selection/testdata/materializer-membership/probe.py \
  --worktree (pwd -P) \
  --artifact-root /var/home/buroa/flate/plans/selection-materializer-scan/membership-count/candidate \
  --mode keyed
```

For a scan control, set `--worktree` to the base checkout and use `--mode scan`.
Use a separate artifact directory for that control.
The base control returns a nonzero exit because it fails the same linear bounds.
Its result identifies the expected object comparison counts at 100, 200, and 400 producers.

`inventory.json` records source hashes and each instrumentation site.
The runner rejects missing, repeated, or changed sites.
`result.json` records commands, results, counters, bounds, and positive witnesses.
`Insertions` counts accepted additions to a map.
`Seeds` counts insertions from retained candidates separately.
The operation bound includes probes, insertions, seeds, and scan comparisons.
Map initializations have a separate counter.
The assertions require zero membership work for empty and filtered controls.

The runner selects only the serial count test and enables the race detector.
Go temporary files use a short directory under `/var/tmp`.
The runner removes that directory, generated overlays, and instrumented copies after each run.
This proof does not measure sorting, retention searches, allocations, or elapsed time.
