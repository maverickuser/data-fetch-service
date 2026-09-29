// Command api runs the manual admission and read API Lambda.
package main

import (
	"github.com/maverickuser/data-fetch-service/internal/runtime"
	"log"
)

var run = runtime.Run

// main fails cold start explicitly instead of serving with incomplete configuration.
func main() {
	if err := run("api"); err != nil {
		log.Print(err)
		panic(err)
	}
}
