package main

import (
	"errors"
	"testing"
)

func TestReconcilerEntrypoint(t *testing.T) {
	old := run
	defer func() { run = old }()
	run = func(kind string) error {
		if kind != "reconciler" {
			t.Fatal(kind)
		}
		return nil
	}
	main()
}

func TestReconcilerColdStartFailure(t *testing.T) {
	old := run
	defer func() { run = old }()
	failure := errors.New("cold start failed")
	run = func(string) error { return failure }
	defer func() {
		if recover() != failure {
			t.Fatal("missing panic")
		}
	}()
	main()
}
