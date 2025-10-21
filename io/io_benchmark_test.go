package io

import (
	"encoding/binary"
	"testing"
)

func TestBlockNextIterator(t *testing.T) {
	for i := 0; i < 2; i++ {
		block := NewFixedSizeRecordBlock(4096, 8) // block size 4096 bytes, record size 8 bytes
		// 4096 - 12 = 4084 / 8 = 511 records
		for i := 0; i < 510; i++ {
			var record int64 = int64(i)
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(record))
			if ok := block.Append(buf); !ok {
				t.Fatalf("failed to append record %d", i)
			}
		}

		for {
			record, ok, err := block.Next()
			if err != nil {
				t.Fatalf("failed to get next record: %v", err)
				break
			}
			if !ok {
				break
			}
			v := binary.BigEndian.Uint64(record) // simulate processing
			t.Logf("Got record: %d", v)
		}

		t.Logf("Completed iteration %d", i)
	}
}

func BenchmarkBlockNextIterator(b *testing.B) {
	b.Logf("Start benchmark")
	for i := 0; i < b.N; i++ {
		block := NewFixedSizeRecordBlock(4096, 8) // block size 4096 bytes, record size 8 bytes
		// 4096 - 12 = 4084 / 8 = 511 records
		for i := 0; i < 510; i++ {
			var record int64 = int64(i)
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(record))
			if ok := block.Append(buf); !ok {
				b.Fatalf("failed to append record %d", i)
			}
		}
		b.StartTimer()
		for {
			record, ok, err := block.Next()
			if err != nil {
				b.Fatalf("failed to get next record: %v", err)
				break
			}
			if !ok {
				break
			}
			_ = binary.BigEndian.Uint64(record) // simulate processing
		}
		b.StopTimer()
	}
}

func BenchmarkBlockFuncIterator(b *testing.B) {
	for i := 0; i < b.N; i++ {
		block := NewFixedSizeRecordBlock(4096, 8) // block size 4096 bytes, record size 8 bytes
		// 4096 - 12 = 4084 / 8 = 511 records
		for i := 0; i < 510; i++ {
			var record int64 = int64(i)
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(record))
			if ok := block.Append(buf); !ok {
				b.Fatalf("failed to append record %d", i)
			}
		}
		b.StartTimer()
		for blockIter := range block.All() {
			_ = binary.BigEndian.Uint64(blockIter) // simulate processing
		}
		b.StopTimer()
	}

}
