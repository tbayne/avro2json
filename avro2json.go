
package main

import (
	"encoding/json"
	"fmt"
    "os"

    "github.com/linkedin/goavro/v2"
    "github.com/pkg/errors"
    "gopkg.in/alecthomas/kingpin.v2"
)
var (
	app = kingpin.New("avro2json","A command line utility to convert AVRO data to JSON")
	inputFile = kingpin.Arg("inputFile","Input AVRO formatted file").Required().String()
	outputFile = kingpin.Flag("outputfile","Sets the output filename").Short('o').Default("<STDOUT>").String()
	showSchemaFlag = kingpin.Flag("showschema","Displays the schema").Short('s').Default("true").Bool()
)


func main(){
	
	kingpin.Version("1.0.0")
	kingpin.Parse()
	fmt.Printf("Input File: %s\n",*inputFile)
	fmt.Printf("Output File: %s\n",*outputFile)


    r, err := os.Open(*inputFile)
    if err != nil {
        fmt.Println(errors.Wrapf(err,"Error opening file: %s",*inputFile))
        os.Exit(1)
    }
    defer r.Close()

	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
	    fmt.Println(errors.Wrapf(err,"error opening ocf: %s",*inputFile))
        os.Exit(1)
    }
    if *showSchemaFlag {
        fmt.Printf("Compression Algorithm (avro.codec): %s\n", ocfr.CompressionName())
    	fmt.Printf("Schema (avro.schema):\n%s\n", ocfr.Codec().Schema())
    }
    // Now read out the data
    for ocfr.Scan() {
		data, err := ocfr.Read()
		if err != nil {
			fmt.Println(errors.Wrapf(err, "error reading from ocf"))
			os.Exit(1)
		}
		j, err := json.MarshalIndent(data,"","  ")
		fmt.Println(string(j), err)
		fmt.Println("-----------------------------------------")
	}



}

