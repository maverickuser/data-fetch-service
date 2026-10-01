package main

import (
	"errors"
	"testing"
)

func TestDeliveryEntrypoint(t *testing.T) {
	old := run
	defer func() { run = old }()
	run = func(kind string) error {
		if kind != "delivery" {
			t.Fatal(kind)
		}
		return nil
	}
	main()
}

func TestDeliveryEntrypointFailsColdStart(t *testing.T) {
	old := run
	defer func() { run = old }()
	failure := errors.New("cold start failed")
	run = func(string) error { return failure }
	defer func() {
		if recover() != failure {
			t.Fatal("cold start did not fail")
		}
	}()
	main()
}
