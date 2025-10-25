// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory.
package spatial

import "fmt"

// DoublePoint represents a point in N-dimensional space using double-precision floating-point numbers.
type DoublePoint []float64

func (d DoublePoint) Dimension() int {
	return len(d)
}

// DoublePointRectangle represents a rectangle in N-dimensional space using double-precision floating-point numbers.
type DoublePointRectangle struct {
	lowLeft DoublePoint
	upRight DoublePoint
}

// returns  a pointer to a new DoublePointRectangle
func NewDoublePointRectangle(lowLeft, upRight DoublePoint) (*DoublePointRectangle, error) {
	if lowLeft.Dimension() != upRight.Dimension() {
		return nil, fmt.Errorf("length of lowLeft and upRight must match")
	}
	return &DoublePointRectangle{
		lowLeft: lowLeft,
		upRight: upRight,
	}, nil
}

func (r *DoublePointRectangle) Dimension() int {
	return r.lowLeft.Dimension()
}

// Union returns a new DoublePointRectangle that is the union of the current rectangle and another rectangle.
func (r *DoublePointRectangle) Union(other *DoublePointRectangle) *DoublePointRectangle {
	if r.Dimension() != other.Dimension() {
		panic("Dimensions must match for union operation")
	}
	newLowLeft := make(DoublePoint, r.Dimension())
	newUpRight := make(DoublePoint, r.Dimension())
	for i := 0; i < r.Dimension(); i++ {
		if r.lowLeft[i] < other.lowLeft[i] {
			newLowLeft[i] = r.lowLeft[i]
		} else {
			newLowLeft[i] = other.lowLeft[i]
		}
		if r.upRight[i] > other.upRight[i] {
			newUpRight[i] = r.upRight[i]
		} else {
			newUpRight[i] = other.upRight[i]
		}
	}
	return &DoublePointRectangle{
		lowLeft: newLowLeft,
		upRight: newUpRight,
	}
}

func (r *DoublePointRectangle) UnionInPlace(other *DoublePointRectangle) {
	if r.Dimension() != other.Dimension() {
		panic("Dimensions must match for union operation")
	}
	for i := 0; i < r.Dimension(); i++ {
		if r.lowLeft[i] > other.lowLeft[i] {
			r.lowLeft[i] = other.lowLeft[i]
		}
		if r.upRight[i] < other.upRight[i] {
			r.upRight[i] = other.upRight[i]
		}
	}
}

// Intersects checks if the current rectangle intersects with another rectangle.
func (r *DoublePointRectangle) Intersects(other *DoublePointRectangle) bool {
	if len(r.lowLeft) != len(other.lowLeft) {
		return false
	}
	// negate interval overlap condition
	// ovrelap other.lowLeft[i] <= r.upRight[i] && other.upRight[i] >= r.lowLeft[i]
	// negate   !(other.lowLeft[i] <= r.upRight[i]) || !( other.upRight[i] >= r.lowLeft[i])
	// which is equivalent to
	// other.lowLeft[i] > r.upRight[i] || other.upRight[i] < r.lowLeft[i]
	for i := 0; i < len(r.lowLeft); i++ {
		if other.lowLeft[i] > r.upRight[i] || other.upRight[i] < r.lowLeft[i] {
			return false
		}
	}
	return true
}

// Area calculates the area (or hypervolume) of the rectangle.
func (r *DoublePointRectangle) Area() float64 {
	area := 1.0
	dimension := r.Dimension()
	for i := 0; i < dimension; i++ {
		area *= r.upRight[i] - r.lowLeft[i]
	}
	return area
}

// Clone creates a deep copy of the DoublePointRectangle.
func (r *DoublePointRectangle) Clone() *DoublePointRectangle {
	newLowLeft := make([]float64, r.Dimension())
	newUpRight := make([]float64, r.Dimension())
	copy(newLowLeft, r.lowLeft)
	copy(newUpRight, r.upRight)
	return &DoublePointRectangle{
		lowLeft: newLowLeft,
		upRight: newUpRight,
	}
}

func (r *DoublePointRectangle) Equals(other *DoublePointRectangle) bool {
	if r.Dimension() != other.Dimension() {
		return false
	}
	for i := 0; i < r.Dimension(); i++ {
		if r.lowLeft[i] != other.lowLeft[i] || r.upRight[i] != other.upRight[i] {
			return false
		}
	}
	return true
}

func (r *DoublePointRectangle) String() string {
	result := "DoublePointRectangle{"
	result += "lowLeft: ["
	for i, val := range r.lowLeft {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%f", val)
	}
	result += "], "
	result += "upRight: ["
	for i, val := range r.upRight {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%f", val)
	}
	result += "]"
	result += "}"
	return result
}
