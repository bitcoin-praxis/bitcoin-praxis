// Copyright (c) 2025 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build unix

package ffldb

import (
	"os"

	"golang.org/x/sys/unix"
)

// adviseSequential hints the kernel that the block file will be read/written
// mostly sequentially (IBD and compaction). Failures are ignored — hints are
// best-effort and not available on every filesystem.
func adviseSequential(f *os.File) {
	if f == nil {
		return
	}
	fd := int(f.Fd())
	_ = unix.Fadvise(fd, 0, 0, unix.FADV_SEQUENTIAL)
}
