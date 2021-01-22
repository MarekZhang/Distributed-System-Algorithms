package main

import (
	"fmt"
	"io/ioutil"
)

func main() {
	m := make(map[string]int)
	m["b"] = 1
	fmt.Println(m["a"])
	fmt.Println(m["b"])
	files, _ := ioutil.ReadDir("./")
	for _, f := range files {
		fmt.Println(f.Name())
	}

	arr := make([]int, 10)
	arr[0] = 1
	fmt.Println(cap(arr))

}
