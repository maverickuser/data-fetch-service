package main

import (
	"errors"
	"testing"
)

func TestMainStartsConfiguredKind(t *testing.T) {
	original := run
	defer func() { run = original }()
	called := false
	run = func(kind string) error {
		called = true
		if kind != "api" {
			t.Fatal(kind)
		}
		return nil
	}
	main()
	if !called {
		t.Fatal("runtime not started")
	}
}
func TestMainFailsColdStart(t *testing.T) {
	original := run
	defer func() { run = original }()
	failure := errors.New("cold start failed")
	run = func(string) error { return failure }
	defer func() {
		if recover() != failure {
			t.Fatal("cold start did not fail")
		}
	}()
	main()
}
