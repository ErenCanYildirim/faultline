// Command loadgen simulates many apps sending events through the SDK.
// It is built in Phase 3 (step 3.1). Until then it only exists so the
// repository layout is final from the start.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "loadgen is not implemented yet (Phase 3, step 3.1)")
	os.Exit(1)
}
