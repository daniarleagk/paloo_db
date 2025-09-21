// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory
package wal

import (
	"io"
	"iter"
)

type LogSequenceNumber int64

// WriteAheadLogger is an interface that wraps the io.Writer interface
// and adds a Flush method for flushing the log.
type WriteAheadLogger interface {
	io.Writer // extends io.Writer
	Flush(lsn LogSequenceNumber) error
	All() iter.Seq[[]byte] // iterator over all log entries
}
