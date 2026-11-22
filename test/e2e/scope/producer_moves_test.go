package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_Diff_ProducerMoves(t *testing.T) {
	for _, name := range []string{
		"resourceset", "kustomizations", "content-edits", "clusters", "partial", "competing", "competing-reverse",
	} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "producer-moves", name)
		})
	}
}
