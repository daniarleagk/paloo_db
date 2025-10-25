package spatial

import "testing"

func TestDoublePointRectangleUnionTest(t *testing.T) {
	r1, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{5, 5})
	r2, _ := NewDoublePointRectangle([]float64{8, 7}, []float64{10, 9})
	r3, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{10, 9})
	r4, _ := NewDoublePointRectangle([]float64{8, 2}, []float64{10, 4})
	r5, _ := NewDoublePointRectangle([]float64{2, 7}, []float64{4, 9})
	r6, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{10, 9})
	r7, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{5, 9})
	r8, _ := NewDoublePointRectangle([]float64{3, 4}, []float64{4, 8})
	r9, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{5, 9})
	tests := []struct {
		name     string
		rect1    *DoublePointRectangle
		rect2    *DoublePointRectangle
		expected *DoublePointRectangle
	}{
		{"left_lower", r1, r2, r3},
		{"upper_lower", r4, r5, r6},
		{"inside", r7, r8, r9},
	}
	for _, tt := range tests {
		t.Logf("Running test: %s", tt.name)
		t.Run(tt.name, func(t *testing.T) {
			result := tt.rect1.Union(tt.rect2)
			if !result.Equals(tt.expected) {
				t.Errorf("Expected %v, but got %v", tt.expected, result)
			}
			tt.rect1.UnionInPlace(tt.rect2)
			if !tt.rect1.Equals(tt.expected) {
				t.Errorf("Expected %v, but got %v", tt.expected, tt.rect1)
			}
		})
	}
}

func TestDoublePointRectangleIntersectTest(t *testing.T) {
	r1, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{6, 6})
	r2, _ := NewDoublePointRectangle([]float64{4, 4}, []float64{10, 10})
	r3, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{10, 10})
	r4, _ := NewDoublePointRectangle([]float64{4, 4}, []float64{6, 6})
	r5, _ := NewDoublePointRectangle([]float64{5, 5}, []float64{8, 8})
	r6, _ := NewDoublePointRectangle([]float64{2, 10}, []float64{6, 12})

	tests := []struct {
		name     string
		rect1    *DoublePointRectangle
		rect2    *DoublePointRectangle
		expected bool
	}{
		{"left_lower",
			r1, r2,
			true,
		},
		{"inside",
			r3, r4,
			true,
		},
		{"inside",
			r5, r6,
			false,
		},
	}
	for _, tt := range tests {
		t.Logf("Running test: %s", tt.name)
		t.Run(tt.name, func(t *testing.T) {
			result := tt.rect1.Intersects(tt.rect2)
			if result != tt.expected {
				t.Errorf("Expected %v, but got %v", tt.expected, result)
			}
		})
	}
}
