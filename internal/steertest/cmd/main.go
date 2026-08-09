package main

import (
	"fmt"
	"os"

	"github.com/looprig/foreignloops/internal/steertest"
)

func main() {
	if err := steertest.RunProcess(); err != nil {
		// Keep diagnostics bounded and free of script/environment contents. The
		// parent fixture receives transport loss and the ACP client classifies
		// the process exit; stderr is only a local bounded hint.
		if len(err.Error()) > 512 {
			fmt.Fprintln(os.Stderr, err.Error()[:512])
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}
