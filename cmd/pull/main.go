// Command pull runs the complete-event SQS acquisition Lambda.
package main

import (
	"log"

	"github.com/maverickuser/data-fetch-service/internal/runtime"
)

var run = runtime.Run

// main fails cold start rather than acknowledging an unconfigured pull message.
func main() {
	if err := run("pull"); err != nil {
		log.Print(err)
		panic(err)
	}
}
