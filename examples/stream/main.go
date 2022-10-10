package main

import (
	"fmt"
	"log"
	"os"

	"github.com/edwsel/go-dag"
	"github.com/olekukonko/tablewriter"
)

func main() {
	stream := dag.NewDefaultStream()

	err := stream.SetAppend("test1", []any{1, 2, 3, 4, 5, 6, 7, 8, 9})

	if err != nil {
		log.Fatalln(err)
	}

	stream1 := dag.NewDefaultStream()

	err = stream1.SetAppend("test2", []any{1, 2, 3, 4, 5, 6, 7, 8, 9})

	if err != nil {
		log.Fatalln(err)
	}

	stream2 := dag.NewDefaultStream()

	err = stream2.SetAppend("test1", []any{10, 12, 13, 14, 15, 16, 17, 18, 19})

	if err != nil {
		log.Fatalln(err)
	}

	//fmt.Println(stream.Keys())
	//fmt.Println(stream.Get("test1"))
	//fmt.Println(stream.GetElement("test1", 2))
	dag.StreamMerge(stream, stream1, stream2)

	//fmt.Println("a")

	//fmt.Println(stream.Keys())
	//fmt.Println(stream.Get("test1"))
	//fmt.Println(stream.Get("test2"))

	keys := stream.Keys()
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(keys)

	tableData := make([][]string, 0)

	rowIndex := 0
	for _, key := range keys {
		for _, rowData := range stream.Get(key) {
			if rowIndex >= len(tableData) {
				tableData = append(tableData, []string{fmt.Sprintf("%v", rowData)})
			} else {
				tableData[rowIndex] = append(tableData[rowIndex], fmt.Sprintf("%v", rowData))
			}

			rowIndex++
		}

		rowIndex = 0
	}

	table.AppendBulk(tableData)

	table.Render()

}
