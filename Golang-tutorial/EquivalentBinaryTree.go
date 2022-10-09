package main

import (
	"fmt"

	"golang.org/x/tour/tree"
)

// Walk walks the tree t sending all values
// from the tree to the channel ch.
func Walk(t *tree.Tree, ch chan int) {
	if t == nil {
		return
	}
	
	Walk(t.Left, ch)
	ch <- t.Value
	Walk(t.Right, ch)
}

// Same determines whether the trees
// t1 and t2 contain the same values.
func Same(t1, t2 *tree.Tree) bool {
	ch1 := make(chan int, 10)	
	ch2 := make(chan int, 10)
	
	go Walk(t1, ch1)
	go Walk(t2, ch2)
	
	for i := 0; i < cap(ch1); i++ {
		v1, ok1 := <- ch1
		v2, ok2 := <- ch2

		if v1 != v2 {
			return false
		} else if !ok1 || !ok2 {
			return false
		}
	}
	
	return true;
}

func main() {
	fmt.Println("Expect true: ", Same(tree.New(2), tree.New(2)))
	fmt.Println("Expect false: ", Same(tree.New(3), tree.New(4)))
}
