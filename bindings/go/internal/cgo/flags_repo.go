//go:build !pkgconfig

package cgobridge

/*
#cgo CFLAGS: -I${SRCDIR}/../../../../include
#cgo darwin LDFLAGS: -L${SRCDIR}/../../../../zig-out/lib -llattice -Wl,-rpath,${SRCDIR}/../../../../zig-out/lib
#cgo linux LDFLAGS: -L${SRCDIR}/../../../../zig-out/lib -llattice -Wl,-rpath,${SRCDIR}/../../../../zig-out/lib
#cgo windows LDFLAGS: -L${SRCDIR}/../../../../zig-out/lib -llattice
*/
import "C"
