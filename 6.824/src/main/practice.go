package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"time"
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

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 20; i++ {
		fmt.Println(rand.Intn(100))
	}

}
