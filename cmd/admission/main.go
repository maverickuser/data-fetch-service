// Command admission runs the ingress SQS admission Lambda.
package main

import (
	"github.com/maverickuser/data-fetch-service/internal/runtime"
	"log"
)

var run = runtime.Run

// main fails cold start explicitly instead of acknowledging unconfigured messages.
func main() {
	if err := run("admission"); err != nil {
		log.Print(err)
		panic(err)
	}
}
