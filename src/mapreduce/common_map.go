package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inName string, // *** Renamed for my convience ***
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	inFile, err := os.OpenFile(inName, os.O_RDONLY, 0666)
	defer inFile.Close()
	if err != nil {
		panic(err)
	}

	inBytes, err := ioutil.ReadAll(inFile)
	if err != nil {
		panic(err)
	}

	keyValueSet := mapF(inName, string(inBytes))

	for i := 0; i < nReduce; i++ {
		outName := reduceName(jobName, mapTaskNumber, i)

		outFile, err := os.OpenFile(outName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		defer outFile.Close()
		if err != nil {
			panic(err)
		}

		outEncoder := json.NewEncoder(outFile)
		err = outEncoder.Encode(keyValueSet)
		if err != nil {
			panic(err)
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
