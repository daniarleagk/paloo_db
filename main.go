package main

import (
	"iter"
	"slices"
)

type CacheBlock interface {
	put(record string)
	All() iter.Seq[string]
}

type CacheBlockSimple struct {
	records []string
}

func (c *CacheBlockSimple) put(record string) {
	c.records = append(c.records, record)
}

func (c *CacheBlockSimple) All() iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, record := range c.records {
			if !yield(record) {
				return
			}
		}
	}
}

type CacheBlockStandard struct {
	records []string
}

func (c *CacheBlockStandard) put(record string) {
	c.records = append(c.records, record)
}

func (c *CacheBlockStandard) All() iter.Seq[string] {
	return slices.Values(c.records)
}

type SlicePrinter[T any, C CacheBlock] struct {
	storage       []T
	printerFunc   func(T) string
	internalBlock C
}

func NewSlicePrinter[T any](printerFunc func(T) string, block *CacheBlockStandard) *SlicePrinter[T, *CacheBlockStandard] {
	return &SlicePrinter[T, *CacheBlockStandard]{
		storage:       make([]T, 0),
		printerFunc:   printerFunc,
		internalBlock: block,
	}
}

func (sp *SlicePrinter[T, C]) Add(item T) {
	sp.storage = append(sp.storage, item)
	record := sp.printerFunc(item)
	sp.internalBlock.put(record)
}

func (sp *SlicePrinter[T, C]) AllRecords() iter.Seq[string] {
	return sp.internalBlock.All()
}

func main() {
	// Example usage
	block := &CacheBlockStandard{}
	printer := NewSlicePrinter(func(i int) string {
		return "Record: " + string(rune(i+'0'))
	}, block)

	printer.Add(1)
	printer.Add(2)
	printer.Add(3)
	for i := range printer.AllRecords() {
		println(i)
	}

}
