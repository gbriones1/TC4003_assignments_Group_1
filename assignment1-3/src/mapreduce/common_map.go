package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// debug("Start doMap function: %v %v %v %v\n",jobName,mapTaskNumber,inFile,nReduce)
	for r := 0; r < nReduce; r++ {
		outFile := reduceName(jobName, mapTaskNumber, r)
		// debug("outFile: %v\n", outFile)
		value, err := ioutil.ReadFile(inFile)
		checkError(err)
		result := mapF(inFile, string(value))
		fileWriter, err := os.Create(outFile)
		checkError(err)
		defer fileWriter.Close()
		enc := json.NewEncoder(fileWriter)
		for i := range result {
			err := enc.Encode(&result[i])
			checkError(err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
