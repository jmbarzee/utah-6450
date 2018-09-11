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

	for i := 0; i < nMap; i++ {
		inName := reduceName(jobName, i, reduceTaskNumber)

		inFile, err := os.OpenFile(inName, os.O_RDONLY, 0666)
		defer inFile.Close()
		if err != nil {
			panic(err)
		}

		var keyValueList []KeyValue

		inDecoder := json.NewDecoder(inFile)
		err = inDecoder.Decode(&keyValueList)
		if err != nil {
			panic(err)
		}

		for _, kv := range keyValueList {
			if keyValuesMap[kv.Key] == nil {
				keyValuesMap[kv.Key] = []string{kv.Value}
			} else {
				keyValuesMap[kv.Key] = append(keyValuesMap[kv.Key], kv.Value)
			}
		}
	}

	keyValueList := make([]KeyValue, len(keyValuesMap))
	j := 0
	for key, values := range keyValuesMap {
		keyValueList[j] = KeyValue{key, reduceF(key, values)}
		j++
	}

	sort.Sort(ByValue(keyValueList))

	outFile, err := os.OpenFile(outName, os.O_WRONLY|os.O_CREATE, 0666)
	defer outFile.Close()
	if err != nil {
		panic(err)
	}

	outEncoder := json.NewEncoder(outFile)
	if err != nil {
		panic(err)
	}

	for _, keyValue := range keyValueList {
		err = outEncoder.Encode(keyValue)
		if err != nil {
			panic(err)
		}
	}
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}

type ByValue []KeyValue

func (a ByValue) Len() int           { return len(a) }
func (a ByValue) Less(i, j int) bool { return a[i].Value < a[j].Value }
func (a ByValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
