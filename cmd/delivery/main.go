// Command delivery runs the processor-admission Lambda from its internal SQS queue.
package main

import (
	"log"

	"github.com/maverickuser/data-fetch-service/internal/runtime"
)

var run = runtime.Run

// main fails cold start rather than acknowledging an unconfigured delivery message.
func main() {
	if err := run("delivery"); err != nil {
		log.Print(err)
		panic(err)
	}
}
