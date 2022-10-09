package main

import (
	"image"
	"image/color"

	"golang.org/x/tour/pic"
)

type Image struct{
	w, h int
}

func (im Image) ColorModel() color.Model {
	return color.RGBAModel
}

func (im Image) Bounds() image.Rectangle {
	return image.Rect(0, 0, im.w, im.h)
}

func (im Image) At(x, y int) color.Color {
	return color.RGBA{uint8(x*y), uint8((x+y)/2), uint8(x^y), 255}
}
	
func main() {
	m := Image{500, 500}
	pic.ShowImage(m)
}
