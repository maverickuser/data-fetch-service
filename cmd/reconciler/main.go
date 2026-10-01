// Command reconciler runs one bounded state-repair pass per scheduled Lambda invocation.
package main

import (
	"log"

	"github.com/maverickuser/data-fetch-service/internal/runtime"
)

var run = runtime.Run

// main fails cold start so an unconfigured reconciliation pass is observable.
func main() {
	if err := run("reconciler"); err != nil {
		log.Print(err)
		panic(err)
	}
}
