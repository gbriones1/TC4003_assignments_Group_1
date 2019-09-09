package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

func main() {
	fmt.Printf("Searching for word: \"%v\" in %v\n", os.Args[1], os.Args[2:])
	var count int
	for _, file := range os.Args[2:] {
		text, _ := ioutil.ReadFile(file)
		words := strings.Fields(string(text))
		reg, _ := regexp.Compile("[^0-9a-zA-Z]+")
		for _, word := range words {
			validWord := reg.ReplaceAllString(word, "")
			if validWord == os.Args[1] {
				// fmt.Printf("%v\n", word)
				count++
			}
		}
	}
	fmt.Printf("%v: %v\n", os.Args[1], count)
}