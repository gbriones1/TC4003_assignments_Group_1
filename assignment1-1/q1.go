package cos418_hw1_1

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
)

func topWords(path string, numWords int, charThreshold int) []WordCount {
	// Reads the document
	text, err := ioutil.ReadFile(path)
	checkError(err)
	// Gets all the words
	words := strings.Fields(string(text))
	// An empty map to store the words and their counts
	counts := map[string]int{}
	// Regex to say we only want letters and numbers
	reg, err := regexp.Compile("[^0-9a-zA-Z]+")
	checkError(err)
	// Loop through all the words
	for _, word := range words {
		// Remove all non-alpha-numeric chars from the word
		validWord := reg.ReplaceAllString(word, "")
		// If condition to validate that the charThreshold condition is meet
		if len(validWord) >= charThreshold{
			counts[strings.ToLower(validWord)]++
		}
	}
	// Convert map to flattened slice of wordCounts
	flat := []WordCount{}
	for word, count := range counts {
		obj := WordCount{word, count}
		flat = append(flat, obj)
	}
	// Sort the slice
	sortWordCounts(flat)
	return flat[:numWords]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
