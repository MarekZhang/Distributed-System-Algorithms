package main

import (
	"strings"

	"golang.org/x/tour/wc"
)


func WordCount(s string) map[string]int {
	m := make(map[string]int)
	wordArr := strings.Split(s, " ")
	
	for _, word := range wordArr {
		v, ok := m[word]
		
		if ok {
			m[word] = v + 1
		} else {
			m[word] = 1
		}
	}

	return m
}

func main() {
	wc.Test(WordCount)
}
