package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_Diff_ProducerMoves(t *testing.T) {
	testScopeCases(t, "producer-moves", []string{
		"resourceset", "kustomizations", "content-edits", "clusters", "partial", "competing", "competing-reverse",
	})
}
