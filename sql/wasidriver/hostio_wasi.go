// Copyright 2026 Elliot Shepherd. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

//go:build wasip1

package wasidriver

import "unsafe"

// Host-imported functions. The host must provide these when
// instantiating the WASI module.
//
// atlas_sql: Execute SQL on the host database.
//   - reqPtr/reqLen: pointer and length of JSON-encoded Request
//   - respPtr/respCap: pointer and capacity of response buffer
//   - returns: actual response length, or negative value if buffer too small
//     (absolute value is the required size)
//
//go:wasmimport env atlas_sql
func atlasSql(reqPtr unsafe.Pointer, reqLen uint32, respPtr unsafe.Pointer, respCap uint32) int64

func init() {
	SetExecFunc(wasiExec)
}

// wasiExec implements ExecFunc by calling the host-imported atlas_sql function.
func wasiExec(request []byte) ([]byte, error) {
	// Start with a 64KB response buffer.
	buf := make([]byte, 65536)
	for {
		n := atlasSql(
			unsafe.Pointer(unsafe.SliceData(request)),
			uint32(len(request)),
			unsafe.Pointer(unsafe.SliceData(buf)),
			uint32(len(buf)),
		)
		if n >= 0 {
			return buf[:n], nil
		}
		// Negative means buffer too small; absolute value is needed size.
		needed := int(-n)
		buf = make([]byte, needed)
	}
}
