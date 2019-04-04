package mapreduce

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
)

type KVs []KeyValue
func (a KVs) Len() int { return len(a) }
func (a KVs) Swap(i, j int) { a[i],a[j] = a[j],a[i] }
func (a KVs) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
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
	// Your code here (Part I).
	//

	var kvSlice []KeyValue
	for i:=0;i<nMap;i++{
		file,err:=os.Open(reduceName(jobName,i,reduceTask))
		if err!=nil{
			log.Fatal("doReduce:open",err)
		}
		var kv KeyValue
		dec:=json.NewDecoder(file)
		i:=0
		for{
			 err:=dec.Decode(&kv)
			 kvSlice=append(kvSlice,kv)
			 if err==io.EOF{
			 	break
			 }
			 i=i+1
		}
		file.Close()
	}
	sort.Sort(KVs(kvSlice))
	var reduceFValues []string
	var outputKVs []KeyValue
	preKey:=kvSlice[0].Key
	for i,kv:=range kvSlice{
		if i==(len(kvSlice)-1){
			reduceFValues=append(reduceFValues,kv.Value)
			outputKVs=append(outputKVs,KeyValue{preKey,reduceF(preKey,reduceFValues)})
		}else{
			if kv.Key!=preKey{
				outputKVs=append(outputKVs,KeyValue{preKey,reduceF(preKey,reduceFValues)})
				reduceFValues=make([]string,0)
				preKey=kv.Key
			}
			reduceFValues=append(reduceFValues,kv.Value)
		}
	}

	file,err:=os.Create(outFile)
	if err!=nil{
		log.Fatal("doReduce:create",err)
	}
	defer file.Close()

	enc:=json.NewEncoder(file)
	for _,kv:=range outputKVs{
		err:=enc.Encode(&kv)
		if err!=nil{
			log.Fatal("doReduce:json encode ",err)
		}
	}
}
