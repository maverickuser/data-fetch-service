// Command configcheck validates the effective YAML configuration and prints its revision.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/maverickuser/data-fetch-service/internal/config"
)

var exitProcess = os.Exit

// main reports configuration validation failures using exit code 2.
func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		exitProcess(2)
		return
	}
}

// run validates the requested configuration and writes a stable diagnostic.
func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("configcheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "config/events.yaml", "base configuration file")
	overlay := flags.String("overlay", "config/environments/prod.yaml", "environment overlay")
	commit := flags.String("commit", "local-uncommitted", "deployment Git commit; default is diagnostic only")
	if err := flags.Parse(args); err != nil {
		return err
	}
	base, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	overlayData, err := os.ReadFile(*overlay)
	if err != nil {
		return err
	}
	cfg, err := config.LoadEffective(base, overlayData)
	if err != nil {
		return err
	}
	cfg.DeploymentCommit = *commit
	_, err = fmt.Fprintf(stdout, "valid schema_version=%d events=%d revision=%s\n", cfg.SchemaVersion, len(cfg.Events), cfg.Revision())
	return err
}
