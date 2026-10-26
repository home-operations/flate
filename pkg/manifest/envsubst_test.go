package manifest

import "testing"

func TestHasEnvsubstReference(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"bare reference", "${REDIS_APP_NAME}-redis", true},
		{"reference with :? error message", "${REQUIRED:?must be set}", true},
		{"literal", "biohazard", false},
		{"reference with default", "${VAR:=default}", true},
		{"no envsubst at all", "redis-server", false},
		{"plain dollar (no brace)", "$HOME", false},
		{"empty", "", false},
		{"escaped dollar", "$$HOME", false},
		{"reference in middle of string", "prefix-${VAR}-suffix", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := HasEnvsubstReference(tc.in); got != tc.want {
				t.Errorf("HasEnvsubstReference(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
