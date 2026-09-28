package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunConfigcheck(t *testing.T) {
	var out, errOut bytes.Buffer
	path := filepath.Join("..", "..", "config", "events.yaml")
	if err := run([]string{"-config", path, "-overlay", "../../config/environments/prod.yaml"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "valid schema_version=1 events=2 revision=sha256:") {
		t.Fatalf("unexpected output %q", out.String())
	}
	if err := run([]string{"-config", "missing.yaml"}, &out, &errOut); err == nil {
		t.Fatal("expected missing file error")
	}
	if err := run([]string{"-bad"}, &out, &errOut); err == nil {
		t.Fatal("expected flag error")
	}
}

func TestMainSuccess(t *testing.T) {
	original := os.Args
	defer func() { os.Args = original }()
	os.Args = []string{"configcheck", "-config", filepath.Join("..", "..", "config", "events.yaml"), "-overlay", "../../config/environments/prod.yaml"}
	main()
}

func TestMainError(t *testing.T) {
	originalArgs, originalExit := os.Args, exitProcess
	defer func() { os.Args, exitProcess = originalArgs, originalExit }()
	exitProcess = func(code int) {
		if code != 2 {
			t.Fatalf("exit code=%d", code)
		}
	}
	os.Args = []string{"configcheck", "-config", "missing.yaml"}
	main()
}
