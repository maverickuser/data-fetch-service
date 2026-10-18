// Package architecture holds the structural tests that keep this module's package layering intact.
//
// Its tests parse the imports of every production package under cmd/ and internal/ and fail when a package
// imports a module package it is not allowed to, or uses an AWS SDK where the design keeps AWS out. The
// package has no production code, so it adds nothing to unit coverage.
package architecture
