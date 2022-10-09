package main

import (
	"fmt"
	"math"
)

const dev = 0.000000000000000001

type ErrNegativeSqrt float64

func (e ErrNegativeSqrt) Error() string {
	return fmt.Sprintf("cannot Sqrt negative number: %v", float64(e))
}

func Sqrt(x float64) (float64, error) {
	if x < 0 {
		return -1, ErrNegativeSqrt(x)
	}

	prev := float64(1)
	z := prev - (prev * prev - x) / (2 * prev)
	
	for math.Abs(z - prev) > dev {
		prev = z
		z -= (z * z - x) / (2 * x)
	}

	return z, nil
}

func main() {
	fmt.Println(Sqrt(2))
	fmt.Println(Sqrt(-2))
}
