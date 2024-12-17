package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
)

func processChunk(chunk []string, idIndex int, wg *sync.WaitGroup, writer *csv.Writer) {
	defer wg.Done()
	seen := make(map[string]bool)

	for _, record := range chunk {
		fields := strings.Split(record, ",") // Assuming comma-separated values

		id := fields[idIndex]
		if seen[id] {
			writer.Write([]string{id})
		} else {
			seen[id] = true
		}
	}

	writer.Flush() // Flush the writer after processing the chunk
}

func main() {
	createCSVOutput(22000000, 10000000)
	checkForDupes()
	exclusionMap := readDupeToMap()
	parseFile(exclusionMap)
}

func parseFile(exclusionMap map[string]struct{}) {

	file, err := os.Open("large-file-with-dupe.csv")
	if err != nil {

		fmt.Println("Creating File with dupes run again:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		record := scanner.Text()
		fields := strings.Split(record, ",")
		id := fields[0]
		if _, ok := exclusionMap[id]; ok {
			fmt.Printf("Key '%s' exists in the map\n", id)
		}
	}

}

func readDupeToMap() map[string]struct{} {
	file, err := os.Open("duplicates.csv")
	if err != nil {

		fmt.Println("Creating File with dupes run again:", err)
		return nil
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	numberString := make(map[string]struct{})
	for scanner.Scan() {
		line := scanner.Text()
		numberString[line] = struct{}{}
	}

	return numberString
}

func checkForDupes() {
	// Open the CSV file
	file, err := os.Open("large-file-with-dupe.csv")
	if err != nil {

		fmt.Println("Creating File with dupes run again:", err)
		return
	}
	defer file.Close()

	outputFile, err := os.Create("duplicates.csv")
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	scanner := bufio.NewScanner(file)

	var wg sync.WaitGroup
	chunkSize := 1000 // Adjust chunk size as needed
	idIndex := 0      // Assuming ID is in the first column (adjust as needed)

	chunk := make([]string, 0, chunkSize) // Pre-allocate a chunk slice
	for scanner.Scan() {
		line := scanner.Text()
		chunk = append(chunk, line)

		if len(chunk) == chunkSize {
			wg.Add(1)
			go processChunk(chunk, idIndex, &wg, writer)
			chunk = chunk[:0] // Clear the chunk for the next iteration
		}
	}

	// Process the remaining records in the last chunk
	if len(chunk) > 0 {
		wg.Add(1)
		go processChunk(chunk, idIndex, &wg, writer)
	}

	wg.Wait()

}

func createCSVOutput(recordsNo int, duplicatesEvery int) {
	outputFile, err := os.Create("large-file-with-dupe.csv")
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)
	end := 99999999999999999
	start := end - recordsNo

	previous_no := 0
	for i := 0; i <= recordsNo; i++ {
		no := start + rand.Intn(end-start+1)

		if i%duplicatesEvery == 0 {
			no = previous_no
		}
		numberString := fmt.Sprintf("%d", no)

		writer.Write([]string{numberString})
		previous_no = no
	}
	writer.Flush() // Flush the writer after processing the chunk
}
