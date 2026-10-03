// Package awsverify holds tests that run only against disposable real AWS resources.
//
// They are built with the `aws` tag and run by the manual AWS integration workflow,
// never by `make check`, so they add nothing to unit coverage.
package awsverify
