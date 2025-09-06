package paloo_db

import (
	"bytes"
	"encoding/binary"
	"os"
	"slices"
	"testing"
)

type FixedSizeTempFileWriterFactory[T any] struct {
	recordSize int
}

func (f *FixedSizeTempFileWriterFactory[T]) CreateTempFileWriter(file *os.File, bufferSize int, serialize func(item T) ([]byte, error)) TempFileWriter[T] {
	return NewFixedSizeTempFileWriter[T](file, bufferSize, f.recordSize, serialize)
}

func TestStandardGoSortRunGenerator_GenerateRuns(t *testing.T) {
	directory := t.TempDir()
	int32Slice := make([]int32, 1000)
	for i := range int32Slice {
		int32Slice = append(int32Slice, int32(i))
	}
	serialize := func(item int32) ([]byte, error) {
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, item); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	comparator := func(a, b int32) int {
		return int(a - b)
	}
	getByteSize := func(item int32) int {
		return 4 // size of int32
	}
	runGenerator := NewStandardGoSortRunGenerator(
		comparator,
		getByteSize,
		serialize,
		64,    // buffer size
		256,   // run size
		128/4, // initial run size
		directory,
		"run",
		".tmp",
		&FixedSizeTempFileWriterFactory[int32]{recordSize: 4},
		4, // parallelism
	)
	err := runGenerator.GenerateRuns(slices.Values(int32Slice))
	if err != nil {
		t.Fatalf("failed to generate runs: %v", err)
	}
	// read back the files and verify sorting

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("failed to read directory: %v", err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	slices.Sort(files)
	deserialize := func(data []byte) (int32, error) {
		var record int32
		buf := bytes.NewReader(data)
		if err := binary.Read(buf, binary.BigEndian, &record); err != nil {
			return 0, err
		}
		return record, nil
	}
	for _, file := range files {
		f, err := os.Open(directory + "/" + file)
		if err != nil {
			t.Fatalf("failed to open file %s: %v", file, err)
		}
		reader := NewFixedSizeTempFileReader(f, 64, 4, deserialize)
		for r := range reader.All() { // just to ensure we can read all records
			if r.Error != nil {
				t.Errorf("read failed: %v", r.Error)
			}
			t.Logf("Read record: %v", r.Record)
		}
		f.Close()
	}

}
