// Copyright (c) 2025 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build !unix

package ffldb

import "os"

// adviseSequential is a no-op on non-Unix platforms (e.g. Windows).
func adviseSequential(f *os.File) {}
