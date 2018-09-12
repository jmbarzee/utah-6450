package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outName string, // write the output here *** Renamed for my convience ***
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	keyValuesMap := make(map[string][]string)

	// Read all Reduce files that were prepared for this reduce
	for i := 0; i < nMap; i++ {

		// Open the inFile
		inName := reduceName(jobName, i, reduceTaskNumber)
		inFile, err := os.OpenFile(inName, os.O_RDONLY, 0666)
		defer inFile.Close()
		if err != nil {
			panic(err)
		}

		// Decode the inFile into keyValueList
		var keyValueList []KeyValue
		inDecoder := json.NewDecoder(inFile)
		err = inDecoder.Decode(&keyValueList)
		if err != nil {
			panic(err)
		}

		//  For each kv in keyValueList, insert/append to the overall keyValuesMap
		for _, kv := range keyValueList {
			if keyValuesMap[kv.Key] == nil {
				keyValuesMap[kv.Key] = []string{kv.Value}
			} else {
				keyValuesMap[kv.Key] = append(keyValuesMap[kv.Key], kv.Value)
			}
		}
	}

	// Condense keyValuesMap into keyValueList by reducing all the keys with reduceF
	keyValueList := make([]KeyValue, len(keyValuesMap))
	j := 0
	for key, values := range keyValuesMap {
		keyValueList[j] = KeyValue{key, reduceF(key, values)}
		j++
	}

	// Sort by the Value of the KeyValue Pairs
	sort.Sort(ByValue(keyValueList))

	// Dump to output
	{
		// Open the outFile
		outFile, err := os.OpenFile(outName, os.O_WRONLY|os.O_CREATE, 0666)
		defer outFile.Close()
		if err != nil {
			panic(err)
		}

		// Setup the outEncoder
		outEncoder := json.NewEncoder(outFile)
		if err != nil {
			panic(err)
		}

		// Dump KeyValues, one at a time
		for _, keyValue := range keyValueList {
			err = outEncoder.Encode(keyValue)
			if err != nil {
				panic(err)
			}
		}
	}
}

type ByValue []KeyValue

func (a ByValue) Len() int           { return len(a) }
func (a ByValue) Less(i, j int) bool { return a[i].Value > a[j].Value }
func (a ByValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
