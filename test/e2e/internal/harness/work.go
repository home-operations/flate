package harness

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
)

// RunWithWork compares the complete fixture results before reading a fresh debug replay.
func (c *Case) RunWithWork(t *testing.T) []Result {
	t.Helper()
	want := c.Want(t)
	for range 2 {
		assert.Diff(t, c.Run(t), want)
	}
	debug := c.Run(t, "--log-level", "debug")
	assert.FatalIf(t, len(debug) != len(want), "debug steps: got %d, want %d", len(debug), len(want))
	for i, result := range debug {
		assert.Equal(t, result.Step, want[i].Step)
		assert.Equal(t, result.Code, want[i].Code)
		assert.Equal(t, result.Stdout, want[i].Stdout)
	}
	return debug
}

var reconcileRecord = regexp.MustCompile(
	`Tree:\{Cluster:[^ ]+ Side:([01])\} Owner:([^ ]+) Source:[^ ]+ Phase:([a-z]+) Input:[^}]*\} Counts:\{Attempts:(\d+) Completed:\d+ Failures:\d+ CacheService:(\d+)`,
)

// ReconcileWork groups paired work by tree side, owner, and phase. Counts hold attempts and cache service.
func (r Result) ReconcileWork(t *testing.T) map[[3]string][2]int {
	t.Helper()
	work := make(map[[3]string][2]int)
	summaries := 0
	for line := range strings.Lines(r.Stderr) {
		if !strings.Contains(line, `msg="orchestrator: reconcile complete"`) {
			continue
		}
		summaries++
		for _, row := range reconcileRecord.FindAllStringSubmatch(line, -1) {
			attempts, err := strconv.Atoi(row[4])
			assert.NoError(t, err)
			service, err := strconv.Atoi(row[5])
			assert.NoError(t, err)
			key := [3]string{row[1], row[2], row[3]}
			current := work[key]
			current[0] += attempts
			current[1] += service
			if current != ([2]int{}) {
				work[key] = current
			}
		}
	}
	const pairedTrees = 2
	assert.Equal(t, summaries, pairedTrees)
	return work
}
