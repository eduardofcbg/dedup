package filedb

import (
	"errors"

	"github.com/gwenn/gosqlite"

	"dedup/file"
)

type foundFilesCursor struct {
	table     *foundFilesTable
	cursorPos int
}

func (c *foundFilesCursor) Close() error {
	return nil
}

func (c *foundFilesCursor) Filter() error {
	c.cursorPos = 0
	return nil
}

func (c *foundFilesCursor) Next() error {
	c.cursorPos++
	return nil
}

func (c *foundFilesCursor) EOF() bool {
	return c.cursorPos >= len(*c.table.filesFound)
}

func (c *foundFilesCursor) Column(ctx *sqlite.Context, col int) error {
	file := (*c.table.filesFound)[c.cursorPos]

	if col == 0 {
		ctx.ResultText(file.Path)
	} else if col == 1 {
		ctx.ResultInt64(file.Size)
	} else if col == 2 {
		time := file.ModTime
		ctx.ResultInt64(time.Unix())
	} else {
		panic(errors.New("Column index out of bounds"))
	}

	return nil
}

func (c *foundFilesCursor) Rowid() (int64, error) {
	return int64(c.cursorPos), nil
}

type foundFilesTable struct {
	filesFound *[]file.File
}

func (v *foundFilesTable) BestIndex() error {
	return nil
}

func (v *foundFilesTable) Disconnect() error {
	return nil
}

func (v *foundFilesTable) Destroy() error {
	return nil
}

func (v *foundFilesTable) Open() (sqlite.VTabCursor, error) {
	return &foundFilesCursor{v, 0}, nil
}

type FileSearchModule struct {
}

func (m FileSearchModule) Create(c *sqlite.Conn, args []string) (sqlite.VTab, error) {
	if len(args) != 4 {
		panic(errors.New("Single argument is required for search root path"))
	}

	path := args[3]
	files, error := file.Search(path)
	if error != nil {
		return nil, error
	}

	schema := "create table found_files(path string, size int, modtime datetime)"
	error = c.DeclareVTab(schema)
	if error != nil {
		return nil, error
	}

	table := &foundFilesTable{&files}

	return table, nil
}

func (m FileSearchModule) Connect(c *sqlite.Conn, args []string) (sqlite.VTab, error) {
	return m.Create(c, args)
}

func (m FileSearchModule) DestroyModule() {

}
