package operators

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/daniar-achakeev/paloo_db/utils"
)

type ItInt32 struct {
	s   []int32
	idx int
}

func NewSliceIterator(s []int32) *ItInt32 {
	return &ItInt32{
		s:   s,
		idx: 0,
	}
}

func (s *ItInt32) Next() (int32, bool, error) {
	if s.idx < len(s.s) {
		next := s.s[s.idx]
		s.idx++
		return next, true, nil
	}
	return 0, false, nil
}

func (s *ItInt32) Close() error {
	s.idx = 0
	s.s = nil
	return nil
}

func TestFixedSizeRecordBlock(t *testing.T) {
	block := NewFixedSizeRecordBlock(54, 4)
	// 12 bytes is a header now 40 Bytes is for data
	// block does not have sufficient space to accommodate 12 records
	for i := range 12 {
		record := int32(i)
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.BigEndian, record)

		if err != nil {
			t.Errorf("binary.Write failed: %v", err)
		}
		if ok := block.Append(buf.Bytes()); !ok {
			if i < 10 {
				t.Fatalf("expected success for first 10 records")
			}
			if block.numRecords > 10 {
				t.Fatalf("expected max 10 records got %d", block.numRecords)
			}
		}
		t.Logf("Wrote record %d: %v : %v", i, record, block)
	}
	//Test block All() function
	allRecords := block.All()
	i := 0
	for bSlice := range allRecords {
		var record int32
		buf := bytes.NewReader(bSlice)
		if err := binary.Read(buf, binary.BigEndian, &record); err != nil {
			t.Errorf("binary.Read failed: %v", err)
		}
		if record != int32(i) {
			t.Errorf("expected record %d got %d", i, record)
		}
		t.Logf("Read record %d: %v", i, record)
		i++
	}
	// now we Reset the block
	t.Logf("Resetting block")
	block.Reset()
	// and append only 4 values
	for i := range 4 {
		record := int32(i + 100)
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.BigEndian, record)

		if err != nil {
			t.Errorf("binary.Write failed: %v", err)
		}
		if ok := block.Append(buf.Bytes()); !ok {
			t.Fatalf("expected success for record %d", i)
		}
		t.Logf("Wrote record %d: %v", i, record)
	}
	// now we read with All this should give us only 4 element
	allRecords = block.All()
	i = 0
	for bSlice := range allRecords {
		var record int32
		buf := bytes.NewReader(bSlice)
		if err := binary.Read(buf, binary.BigEndian, &record); err != nil {
			t.Errorf("binary.Read failed: %v", err)
		}
		if record != int32(i+100) {
			t.Errorf("expected record %d got %d", i, record)
		}
		if i >= 4 {
			t.Errorf("un expected record %d got %d", i, record)
		}
		t.Logf("Read record %d: %v", i, record)
		i++
	}
}

type Int32DeSerializer struct{}

func (Int32DeSerializer) Deserialize(data []byte) (int32, error) {
	return int32(binary.BigEndian.Uint32(data)), nil
}

// Serialize
func (Int32DeSerializer) Serialize(item int32, buf []byte) error {
	binary.BigEndian.PutUint32(buf, uint32(item))
	return nil
}

func TestFixedSizeRecordWriterReader(t *testing.T) {
	tmpDir := t.TempDir()
	fp := filepath.Join(tmpDir, "test.tmp")
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("cannot open file %s: %v", fp, err)
	}
	defer f.Close()
	// Create a FixedSizeTempFileWriter
	deSerialize := Int32DeSerializer{}
	// NOTE the header per buffer is 12 bytes
	// current capacity is 64 - 12 = 52
	// each record is 4 bytes
	// we should have 13 Records per buffer
	recordsPerBuffer := 13
	writer := NewFixedSizeTmpFileWriter(f, 64, 4, deSerialize)
	// now we will insert 4 buffers
	// 3 full buffers and 1 partial buffer
	// each buffer can hold 13 records
	maxRecords := recordsPerBuffer*3 + 2
	slice := make([]int32, 0, maxRecords)
	for i := range maxRecords {
		slice = append(slice, int32(i))
	}
	// Write all records
	if err := writer.WriteSeq(slices.Values(slice)); err != nil {
		t.Errorf("write batch failed: %v", err)
	}
	// Flush the writer
	if err := writer.Flush(); err != nil {
		t.Errorf("flush failed: %v", err)
	}
	// reset offset from a file handler
	f.Seek(0, io.SeekStart)
	// Create a FixedSizeTempFileReader
	reader := NewFixedSizeTmpFileReader(f, 64, 4, deSerialize)
	// Read all records
	allRecords := reader.All()
	i := 0
	shouldHaveData := false
	for r, err := range allRecords {
		if err != nil {
			t.Errorf("read failed: %v", err)
		}
		if r != int32(i) {
			t.Errorf("expected record %d got %d", i, r)
		}
		t.Logf("Read record %d: %v", i, r)
		i++
		shouldHaveData = true
	}
	if !shouldHaveData {
		t.Errorf("expected data but got none")
	}
	if i != maxRecords {
		t.Errorf("less then expected %d", maxRecords)
	}
}

func TestFixedSizeRecordReaderIterator(t *testing.T) {
	tmpDir := t.TempDir()
	fp := filepath.Join(tmpDir, "test.tmp")
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("cannot open file %s: %v", fp, err)
	}
	defer f.Close()
	// Create a FixedSizeTempFileWriter
	deSerialize := Int32DeSerializer{}
	// NOTE the header per buffer is 12 bytes
	// current capacity is 64 - 12 = 52
	// each record is 4 bytes
	// we should have 13 Records per buffer
	recordsPerBuffer := 13
	writer := NewFixedSizeTmpFileWriter(f, 64, 4, deSerialize)
	// now we will insert 4 buffers
	// 3 full buffers and 1 partial buffer
	// each buffer can hold 13 records
	maxRecords := recordsPerBuffer*3 + 2
	slice := make([]int32, 0, maxRecords)
	for i := range maxRecords {
		slice = append(slice, int32(i))
	}
	// Write all records
	if err := writer.Write(NewSliceIterator(slice)); err != nil {
		t.Errorf("write batch failed: %v", err)
	}
	// Flush the writer
	if err := writer.Flush(); err != nil {
		t.Errorf("flush failed: %v", err)
	}
	// reset offset from a file handler
	f.Seek(0, io.SeekStart)
	// Create a FixedSizeTempFileReader
	reader, err := NewFixedSizeTmpFileIterator(f, 64, 4, deSerialize)
	if err != nil {
		t.Fatalf("should be no error")
	}
	// Read all records
	allRecords := utils.IteratorToSeq(reader)
	defer reader.Close()
	i := 0
	shouldHaveData := false
	for r := range allRecords {
		if err != nil {
			t.Errorf("read failed: %v", err)
		}
		if r != int32(i) {
			t.Errorf("expected record %d got %d", i, r)
		}
		t.Logf("Read record %d: %v", i, r)
		i++
		shouldHaveData = true
	}
	if !shouldHaveData {
		t.Errorf("expected data but got none")
	}
	if i != maxRecords {
		t.Errorf("less then expected %d", maxRecords)
	}
}
