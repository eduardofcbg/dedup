package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/gwenn/gosqlite"

	"dedup/file"
	"dedup/filedb"
)

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func createHashFunctions(db *sqlite.Conn) {
	type hash func(string) (string, error)

	createHashFunction := func(f hash) sqlite.ScalarFunction {
		return func(ctx *sqlite.ScalarContext, nArg int) {
			path := ctx.Text(0)
			hash, err := f(path)
			checkError(err)

			ctx.ResultText(hash)
		}
	}

	err := db.CreateScalarFunction("hash", 1, true, nil, createHashFunction(file.Hash), nil)
	checkError(err)

	err = db.CreateScalarFunction("hash_first_bytes", 1, true, nil, createHashFunction(file.HashFirstBytes), nil)
	checkError(err)
}

func createFileSearchModule(db *sqlite.Conn) {
	module := filedb.FileSearchModule{}
	err := db.CreateModule("file_search", module)
	checkError(err)
}

func main() {
	path := os.Args[1]
	log.Print("Searching for duplicates at ", path)

	db, err := sqlite.Open("dedup.db")
	checkError(err)

	defer db.Close()

	createFileSearchModule(db)
	createHashFunctions(db)

	err = filedb.Create(db, path)
	checkError(err)

	err = filedb.ExcludeUniqueSize(db)
	checkError(err)

	err = filedb.ExcludeUniqueFirstBytes(db)
	checkError(err)

	err = filedb.HashFiles(db)
	checkError(err)

	err = filedb.PrintDuplicates(db)
	checkError(err)
}
