package main

import "golang.org/x/tour/pic"

func Pic(dx, dy int) [][]uint8 {
	v := make([][]uint8, dx)
	
	for i := range v {
		v[i] = make([]uint8, dy)
	}
	
	for i := range v {
		for j:= range v[i] {
			if (i + j) % 3 == 0 {
				v[i][j] = 27
			} else if (i + j) % 2 == 0 {
				v[i][j] = 120
			}
		}
	}
	
	return v
}

func main() {
	pic.Show(Pic)
}
