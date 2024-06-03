package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

// insert into: put a new tuple into the table(indexes)
// (?, ?, ?, ?) are placeholders for the values of the tuple
const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	//panic("todo")
	statement, err = db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	// The table has 4 columns which are fileName, version, hashIndex, hashValue.
	// Their types are TEXT, INT, INT, and TEXT respectively
	for fileName, filemeta := range fileMetas {
		for hashIndex, hashValue := range filemeta.BlockHashList { // Index should start from 0
			statement.Exec(fileName, filemeta.Version, hashIndex, hashValue)
		}
	}
	statement.Close()
	db.Close()
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName, version from indexes;`

// asc: ascending order
const getTuplesByFileName string = `select fileName, version, hashIndex, hashValue from indexes where fileName=? AND version=? order by hashIndex ASC
`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	//fmt.Println("Loading Meta From Meta File")
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	//fmt.Println("Meta File Path: ", metaFilePath)
	fileMetaMap = make(map[string]*FileMetaData)
	//fmt.Println("File Meta Map: ", fileMetaMap)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	db = db // remove warning
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	//panic("todo")

	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatalf("Error create Table: %v", err)
	}

	if _, err = statement.Exec(); err != nil {
		log.Fatalf("Error create Table: %v", err)
	}
	statement.Close()

	rows, err := db.Query(getDistinctFileName) // get all distinct file names
	if err != nil {
		log.Fatal("Error while querying distinct file names", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fileName string
		var version int32
		if err := rows.Scan(&fileName, &version); err != nil {
			log.Fatal("Error while scanning distinct file names1", err)
		}
		hashValues := []string{}
		hashRows, err := db.Query(getTuplesByFileName, fileName, version)
		defer hashRows.Close()
		if err != nil {
			log.Fatal("Error while scanning distinct file names2", err)
		}
		for hashRows.Next() {
			var fileName string
			var version int
			var hashIndex int
			var hashValue string
			if err := hashRows.Scan(&fileName, &version, &hashIndex, &hashValue); err != nil {
				log.Fatal("Error while scanning distinct file names3", err)
			}
			hashValues = append(hashValues, hashValue)
		}

		fileMetaMap[fileName] = &FileMetaData{
			Filename:      fileName,
			Version:       version,
			BlockHashList: hashValues,
		}
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
