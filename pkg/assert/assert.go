package assert

import "fmt"

// Assertf panics and prints the appended message if the cond is false.
func Assertf(cond bool, format string, a ...any) {
	if !cond {
		msg := fmt.Sprintf(format, a...)
		panic(msg)
	}
}
