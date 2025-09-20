// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory.
package spatial

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/daniarleagk/paloo_db/utils"
)

type DoublePointRectangle struct {
	dimension uint16
	lowLeft   []float64
	upRight   []float64
}

func getDoublePointRectangleDeSerFunc() utils.DeSerFunc[DoublePointRectangle] {
	return func(data []byte) (st DoublePointRectangle, err error) {
		// read the dimension
		// slice first 2 bytes
		dimension := binary.BigEndian.Uint16(data[:2])
		if capacity := 2 + 2*int(dimension)*8; len(data) < capacity {
			return DoublePointRectangle{}, errors.New("not enough capacity to build ")
		}
		// read the lower left point
		lowLeft := make([]float64, dimension)
		// start from 2 and read 8 bytes at a time
		startIndex := 2
		for i := 0; i < int(dimension); i++ {
			i64Bits := binary.BigEndian.Uint64(data[startIndex+i*8 : startIndex+(i+1)*8])
			lowLeft[i] = math.Float64frombits(i64Bits)
		}
		// read the upper right point at position 2 + dimension * 8
		startIndex = 2 + int(dimension)*8
		upRight := make([]float64, dimension)
		for i := 0; i < int(dimension); i++ {
			i64Bits := binary.BigEndian.Uint64(data[startIndex+i*8 : startIndex+(i+1)*8])
			upRight[i] = math.Float64frombits(i64Bits)
		}
		return DoublePointRectangle{
			dimension: dimension,
			lowLeft:   lowLeft,
			upRight:   upRight,
		}, nil
	}
}

func getDoublePointRectangleSerFunc() utils.SerFunc[DoublePointRectangle] {
	return func(s DoublePointRectangle, data []byte) (n int, err error) {
		// length and capacity of the slice is 2 + 2 * dimension * 8
		capacity := 2 + 2*int(s.dimension)*8
		if cap(data) < capacity {
			return 0, errors.New("not enough capacity")
		}
		// write the dimension
		binary.BigEndian.PutUint16(data[:2], s.dimension)
		// write the lower left point
		startIndex := 2
		for i := 0; i < int(s.dimension); i++ {
			i64Bits := math.Float64bits(s.lowLeft[i])
			binary.BigEndian.PutUint64(data[startIndex+i*8:startIndex+(i+1)*8], i64Bits)
		}
		// write the upper right point
		startIndex = 2 + int(s.dimension)*8
		for i := 0; i < int(s.dimension); i++ {
			i64Bits := math.Float64bits(s.upRight[i])
			binary.BigEndian.PutUint64(data[startIndex+i*8:startIndex+(i+1)*8], i64Bits)
		}
		return capacity, nil
	}
}
