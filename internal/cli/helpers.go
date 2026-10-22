package cli

import "fmt"

func firstArg(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

func noNamedError(kind, name, path string) error {
	return fmt.Errorf("no %s named %q in %s", kind, name, path)
}
