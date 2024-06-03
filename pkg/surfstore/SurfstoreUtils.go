package surfstore

import (
	"log"
	"os"
	"path/filepath"
)

func ClientSync(client RPCClient) {
	baseDir, localFileInfoMap, err := getLocalInfo(client)             // get localIndex
	err = updateLocalIndexFile(client, err, baseDir, localFileInfoMap) // update localIndex (new, delete, change)
	log.Println("Local index updated")
	remoteIndex, err := getRemoteIndexFile(client, err)
	log.Println("Remote index updated")
	for remoteFilename, remoteFileMetaData := range remoteIndex {
		log.Println(">>>>>>>>>>>Syncing file: ", remoteFilename)
		log.Println("Remote file version: ", remoteFileMetaData.Version)
		if _, ok := localFileInfoMap[remoteFilename]; !ok { // local index no file -> download file

			if remoteFileMetaData.BlockHashList[0] != "0" { // remote file is not deleted, download file
				log.Println("Downloading file: ", remoteFilename)
				downloadFile(client, remoteFileMetaData, err, remoteFilename, localFileInfoMap)
			} else { // remote file is deleted, update local index
				log.Println("Deleting file: ", remoteFilename)
				localFileInfoMap[remoteFilename] = remoteFileMetaData
			}
		} else { // local index has file, remote index has file -> compare version
			log.Println("Local file version: ", localFileInfoMap[remoteFilename].Version)
			localFileMetaData := localFileInfoMap[remoteFilename]
			if localFileMetaData.Version > remoteFileMetaData.Version {
				if localFileMetaData.BlockHashList[0] == "0" { // - local hash[0] == "0" -> delete remote file
					log.Println("Deleting remote file: ", remoteFilename)
					returnedVersion, _ := updateRemoteFile(client, remoteFilename, localFileMetaData.Version, []string{"0"})
					if returnedVersion == -1 { // conflict
						log.Println("Conflict: ", remoteFilename)
						coflictReturnHandle(client, remoteIndex, err, remoteFileMetaData, remoteFilename, baseDir, localFileInfoMap)
					}
				} else { // upload file
					log.Println("Uploading file: ", remoteFilename)
					returnedVersion, err := uploadFile(client, remoteFilename, localFileMetaData)
					if returnedVersion == -1 { // conflict
						log.Println("Conflict: ", remoteFilename)
						coflictReturnHandle(client, remoteIndex, err, remoteFileMetaData, remoteFilename, baseDir, localFileInfoMap)
					}
				}

			} else if localFileMetaData.Version < remoteFileMetaData.Version {
				log.Println("Syncing with remote: ", remoteFilename)
				syncWithRemote(client, remoteFileMetaData, baseDir, remoteFilename, localFileInfoMap, err)
			} else if localFileMetaData.Version == remoteFileMetaData.Version {
				if !CompareBlockHashList(localFileMetaData.BlockHashList, remoteFileMetaData.BlockHashList) {
					log.Println("conflict, syncing with remote: ", remoteFilename)
					syncWithRemote(client, remoteFileMetaData, baseDir, remoteFilename, localFileInfoMap, err)
				}
			}
		}
	}

	// - local index has file, remote index no file -> upload file (done)
	for localFilename, localFileMetaData := range localFileInfoMap {
		if _, ok := remoteIndex[localFilename]; !ok {
			if localFileMetaData.BlockHashList[0] != "0" { // local file is not deleted, upload file
				log.Println("Uploading file: ", localFilename)
				returnedVersion, err := uploadFile(client, localFilename, localFileMetaData)
				if returnedVersion == -1 { // conflict
					log.Println("Conflict: ", localFilename)
					coflictReturnHandle(client, remoteIndex, err, localFileMetaData, localFilename, baseDir, localFileInfoMap)
				}
			}
		}
	}
	err = WriteMetaFile(localFileInfoMap, baseDir)
	log.Println("Local index updated, done")
}

func coflictReturnHandle(client RPCClient, remoteIndex map[string]*FileMetaData, err error, remoteFileMetaData *FileMetaData, remoteFilename string, baseDir string, localFileInfoMap map[string]*FileMetaData) {
	remoteIndex, _ = getRemoteIndexFile(client, err) // get new remote index
	remoteFileMetaData = remoteIndex[remoteFilename]
	syncWithRemote(client, remoteFileMetaData, baseDir, remoteFilename, localFileInfoMap, err)
}

func syncWithRemote(client RPCClient, remoteFileMetaData *FileMetaData, baseDir string, remoteFilename string, localFileInfoMap map[string]*FileMetaData, err error) {
	if remoteFileMetaData.BlockHashList[0] == "0" { // delete local file
		log.Println("Deleting local file: ", remoteFilename)
		os.Remove(ConcatPath(baseDir, remoteFilename))
		localFileInfoMap[remoteFilename] = remoteFileMetaData
	} else { // download file
		log.Println("Downloading file: ", remoteFilename)
		downloadFile(client, remoteFileMetaData, err, remoteFilename, localFileInfoMap)
	}
}

func uploadFile(client RPCClient, remoteFilename string, localFileMetaData *FileMetaData) (returnedVersion int32, err error) {
	blockStoreMap := map[string][]string{}
	err = client.GetBlockStoreMap(localFileMetaData.BlockHashList, &blockStoreMap)
	if err != nil {
		log.Fatalf("Error while getting block store map from the server: %v", err)
	}
	// change map to block hash -> server address
	hashToServer := map[string]string{}
	for serverAddr, blockHashes := range blockStoreMap {
		for _, blockHash := range blockHashes {
			hashToServer[blockHash] = serverAddr
		}
	}
	localPath := filepath.Join(client.BaseDir, remoteFilename)
	file, err := os.Open(localPath)
	if err != nil {
		log.Fatalf("Cannot open file %s: %v", localPath, err)
	}
	defer file.Close()
	filestats, err := file.Stat()
	if err != nil {
		log.Fatalf("Cannot get file stats %s: %v", localPath, err)
	}
	blocknum := filestats.Size() / int64(client.BlockSize)
	if filestats.Size()%int64(client.BlockSize) != 0 {
		blocknum++
	}

	if blocknum > 0 {
		blockData := make([]byte, client.BlockSize)
		for i := int64(0); i < blocknum; i++ {
			n, err := file.Read(blockData)
			if err != nil {
				log.Fatalf("Cannot read block %d from file %s: %v", i, localPath, err)
			}
			var block Block
			block.BlockData = blockData[:n]
			block.BlockSize = int32(n)

			// get block store address
			blockStoreAddr := hashToServer[GetBlockHashString(block.BlockData)]
			var success bool
			err = client.PutBlock(&block, blockStoreAddr, &success)
			if err != nil || !success {
				log.Fatalf("Error while putting block %d to the server: %v", i, err)
			}
		}
	}
	returnedVersion, err = updateRemoteFile(client, remoteFilename, localFileMetaData.Version, localFileMetaData.BlockHashList)
	return returnedVersion, err
}

func updateRemoteFile(client RPCClient, name string, version int32, blockHashList []string) (returnedVersion int32, err error) {
	remoteFileupdate := &FileMetaData{
		Filename:      name,
		Version:       version,
		BlockHashList: blockHashList,
	}
	err = client.UpdateFile(remoteFileupdate, &returnedVersion)
	if err != nil {
		log.Fatalf("Error while updating file %s: %v", name, err)
	}
	return returnedVersion, err
}

func downloadFile(client RPCClient, remoteFileMetaData *FileMetaData, err error, remoteFilename string, localFileInfoMap map[string]*FileMetaData) {
	if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "-1" { //empty file, no need to download,  only open local path and exit
		localPath := ConcatPath(client.BaseDir, remoteFilename)
		localFile, err := os.Create(localPath)
		if err != nil {
			log.Fatalf("Cannot create file %s: %v", localPath, err)
		}
		defer localFile.Close()
		localFileInfoMap[remoteFilename] = remoteFileMetaData
		return
	}
	blockStoreMap := map[string][]string{}
	err = client.GetBlockStoreMap(remoteFileMetaData.BlockHashList, &blockStoreMap)
	if err != nil {
		log.Fatalf("Error while getting block store map from the server: %v", err)
	}
	hashToServer := map[string]string{} // change map to block hash -> server address
	for serverAddr, blockHashes := range blockStoreMap {
		for _, blockHash := range blockHashes {
			hashToServer[blockHash] = serverAddr
		}
	}
	localPath := ConcatPath(client.BaseDir, remoteFilename)
	localFile, err := os.Create(localPath)
	if err != nil {
		log.Fatalf("Cannot create file %s: %v", localPath, err)
	}
	defer localFile.Close()
	for _, blockHash := range remoteFileMetaData.BlockHashList {
		var block Block // get block store address
		blockStoreAddr := hashToServer[blockHash]
		err = client.GetBlock(blockHash, blockStoreAddr, &block)
		if err != nil {
			log.Fatalf("Error while getting block %s from the server: %v", blockHash, err)
		}
		_, err = localFile.Write(block.BlockData) // sync write block to file
		if err != nil {
			log.Fatalf("Error while writing block to file %s: %v", localPath, err)
		}
	}
	localFileInfoMap[remoteFilename] = remoteFileMetaData
}

func getRemoteIndexFile(client RPCClient, err error) (map[string]*FileMetaData, error) {
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Fatalf("Error while getting FileInfoMap from the server: %v", err)
	}
	if remoteIndex == nil {
		remoteIndex = make(map[string]*FileMetaData)
	}
	return remoteIndex, err
}

func CompareBlockHashList(h1, h2 []string) bool {
	if len(h1) != len(h2) {
		return false
	}
	for i := range h1 {
		if h1[i] != h2[i] {
			return false
		}
	}
	return true
}

func getLocalInfo(client RPCClient) (string, map[string]*FileMetaData, error) {
	baseDir := client.BaseDir
	localFileInfoMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Fatalf("Error while loading metadata from index.db: %v", err)
	}
	if localFileInfoMap == nil {
		localFileInfoMap = make(map[string]*FileMetaData)
	}
	return baseDir, localFileInfoMap, err
}

func blockToHash(path string, blocknum int64, client RPCClient, file *os.File, blockHashList []string) (error, bool) {
	for i := int64(0); i < blocknum; i++ {
		block := make([]byte, client.BlockSize)
		n, err := file.Read(block)
		if err != nil {
			log.Printf("Cannot read block %d from file %s: %v", i, path, err)
			return nil, true
		}
		blockHashList[i] = GetBlockHashString(block[:n])
	}
	return nil, false
}

func updateLocalIndexFile(client RPCClient, err error, baseDir string, localFileInfoMap map[string]*FileMetaData) error {
	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && info.Name() != ".DS_Store" && info.Name() != "index.db" {
			file, err := os.Open(path)
			if err != nil {
				log.Printf("Cannot open file %s: %v", path, err)
				return nil
			}
			defer file.Close()
			blocknum := info.Size() / int64(client.BlockSize)
			if info.Size()%int64(client.BlockSize) != 0 {
				blocknum++
			}
			blockHashList := make([]string, blocknum)
			if blocknum > 0 {
				err2, done := blockToHash(path, blocknum, client, file, blockHashList)
				if done {
					return err2
				}
			} else {
				blockHashList = make([]string, 1)
				blockHashList[0] = "-1"
			}
			compareLocalIndexFile(localFileInfoMap, info, blockHashList, baseDir) // compare with local index file
		}
		return nil
	})
	checkLocalDelete(localFileInfoMap, baseDir) // mark deleted files
	return err
}

func compareLocalIndexFile(localFileInfoMap map[string]*FileMetaData, info os.FileInfo, blockHashList []string, baseDir string) {
	if localFileMetaData, ok := localFileInfoMap[info.Name()]; ok {
		if !CompareBlockHashList(localFileMetaData.BlockHashList, blockHashList) { // file has changed -> update local index file
			localFileInfoMap[info.Name()] = &FileMetaData{
				Filename:      info.Name(),
				Version:       localFileMetaData.Version + 1,
				BlockHashList: blockHashList,
			}
		}
	} else { // new file -> update local index file
		localFileInfoMap[info.Name()] = &FileMetaData{
			Filename:      info.Name(),
			Version:       1,
			BlockHashList: blockHashList,
		}
	}
}

func checkLocalDelete(localFileInfoMap map[string]*FileMetaData, baseDir string) {
	for filename, localFileMetaData := range localFileInfoMap {
		filePath := filepath.Join(baseDir, filename)
		_, err := os.Stat(filePath)
		if err != nil {
			if localFileMetaData.BlockHashList[0] != "0" { // file not exist -> mark as deleted
				localFileInfoMap[filename] = &FileMetaData{
					Filename:      filename,
					Version:       localFileMetaData.Version + 1,
					BlockHashList: []string{"0"},
				}
			}
		}
	}
}
