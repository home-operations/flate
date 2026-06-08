package cli

import "fmt"

// firstArg returns the first positional arg, or "" when none was given.
func firstArg(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

// noNamedError is the error build/get/test return when an explicit name
// positional matches nothing of the given kind in --path. Single-sourced
// so the three commands can't drift on the message.
func noNamedError(kind, name string) error {
	return fmt.Errorf("no %s named %q in --path", kind, name)
}
