package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cosmossdk.io/math"
	"github.com/bnb-chain/greenfield-go-sdk/client"
	"github.com/bnb-chain/greenfield/sdk/types"

	sdktypes "github.com/bnb-chain/greenfield-go-sdk/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/urfave/cli/v2"
)

// cmdPutObj return the command to finish uploading payload of the object
func cmdPutObj() *cli.Command {
	return &cli.Command{
		Name:      "put",
		Action:    putObject,
		Usage:     "create object on chain and upload payload of object to SP",
		ArgsUsage: "[filePath]...  OBJECT-URL",
		Description: `
Send createObject txn to chain and upload the payload of object to the storage provider.
The command need to pass the file path inorder to compute hash roots on client.
Note that the  uploading with recursive flag only support folder.

Examples:
# create object and upload file to storage provider, the corresponding object is gnfd-object
$ gnfd-cmd object put file.txt gnfd://gnfd-bucket/gnfd-object,
# upload the files inside the folders
$ gnfd-cmd object put --recursive folderName gnfd://bucket-name`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  secondarySPFlag,
				Value: "",
				Usage: "indicate the Secondary SP addr string list, input like addr1,addr2,addr3",
			},
			&cli.StringFlag{
				Name:  contentTypeFlag,
				Value: "",
				Usage: "indicate object content-type",
			},
			&cli.GenericFlag{
				Name: visibilityFlag,
				Value: &CmdEnumValue{
					Enum:    []string{publicReadType, privateType, inheritType},
					Default: inheritType,
				},
				Usage: "set visibility of the object",
			},
			&cli.Uint64Flag{
				Name: partSizeFlag,
				// the default part size is 32M
				Value: 32 * 1024 * 1024,
				Usage: "indicate the resumable upload 's part size, uploading a large file in multiple parts. " +
					"The part size is an integer multiple of the segment size.",
			},
			&cli.BoolFlag{
				Name:  resumableFlag,
				Value: false,
				Usage: "indicate whether need to enable resumeable upload. Resumable upload refers to the process of uploading " +
					"a file in multiple parts, where each part is uploaded separately.This allows the upload to be resumed from " +
					"where it left off in case of interruptions or failures, rather than starting the entire upload process from the beginning.",
			},
			&cli.BoolFlag{
				Name:  recursiveFlag,
				Value: false,
				Usage: "performed on all files or objects under the specified directory or prefix in a recursive way",
			},
		},
	}
}

// cmdGetObj return the command to finish downloading object payload
func cmdGetObj() *cli.Command {
	return &cli.Command{
		Name:      "get",
		Action:    getObject,
		Usage:     "download an object",
		ArgsUsage: "[filePath] OBJECT-URL",
		Description: `
Download a specific object from storage provider

Examples:
# download an object payload to file
$ gnfd-cmd object get gnfd://gnfd-bucket/gnfd-object  file.txt `,
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  startOffsetFlag,
				Value: 0,
				Usage: "start offset info of the download body",
			},
			&cli.Int64Flag{
				Name:  endOffsetFlag,
				Value: 0,
				Usage: "end offset info of the download body",
			},
			&cli.Uint64Flag{
				Name: partSizeFlag,
				// the default part size is 32M
				Value: 32 * 1024 * 1024,
				Usage: "indicate the resumable upload 's part size, uploading a large file in multiple parts. " +
					"The part size is an integer multiple of the segment size.",
			},
			&cli.BoolFlag{
				Name:  resumableFlag,
				Value: false,
				Usage: "indicate whether need to enable resumeable download. Resumable download refers to the process of download " +
					"a file in multiple parts, where each part is downloaded separately.This allows the download to be resumed from " +
					"where it left off in case of interruptions or failures, rather than starting the entire download process from the beginning.",
			},
		},
	}
}

// cmdCancelObjects cancel the object which has been created
func cmdCancelObjects() *cli.Command {
	return &cli.Command{
		Name:      "cancel",
		Action:    cancelCreateObject,
		Usage:     "cancel the created object",
		ArgsUsage: "OBJECT-URL",
		Description: `
Cancel the created object 

Examples:
$ gnfd-cmd object cancel  gnfd://gnfd-bucket/gnfd-object`,
	}
}

// cmdListObjects list the objects of the bucket
func cmdListObjects() *cli.Command {
	return &cli.Command{
		Name:      "ls",
		Action:    listObjects,
		Usage:     "list objects of the bucket",
		ArgsUsage: "BUCKET-URL",
		Description: `
List Objects of the bucket, including object name, object id, object status

Examples:
$ gnfd-cmd object ls gnfd://gnfd-bucket`,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  recursiveFlag,
				Value: false,
				Usage: "performed on all files or objects under the specified directory or prefix in a recursive way",
			},
		},
	}
}

// cmdUpdateObject update the visibility of the object
func cmdUpdateObject() *cli.Command {
	return &cli.Command{
		Name:      "replicate",
		Action:    replicateObject,
		Usage:     "update object visibility",
		ArgsUsage: "OBJECT-URL",
		Description: `
Update the visibility of the object.
The visibility value can be public-read, private or inherit.

Examples:
update visibility of the gnfd-object
$ gnfd-cmd object update --visibility=public-read  gnfd://gnfd-bucket/gnfd-object`,
		Flags: []cli.Flag{
			&cli.GenericFlag{
				Name: visibilityFlag,
				Value: &CmdEnumValue{
					Enum:    []string{publicReadType, privateType, inheritType},
					Default: privateType,
				},
				Usage: "set visibility of the bucket",
			},
		},
	}
}

// cmdGetUploadProgress return the uploading progress info of the object
func cmdGetUploadProgress() *cli.Command {
	return &cli.Command{
		Name:      "get-progress",
		Action:    getUploadInfo,
		Usage:     "get the uploading progress info of object",
		ArgsUsage: "OBJECT-URL",
		Description: `
The command is used to get the uploading progress info. 
you can use this command to view the progress information during the process of uploading a file to a Storage Provider.

Examples:
$ gnfd-cmd object get-progress gnfd://gnfd-bucket/gnfd-object`,
	}
}

func cmdMirrorObject() *cli.Command {
	return &cli.Command{
		Name:      "mirror",
		Action:    mirrorObject,
		Usage:     "mirror object to BSC",
		ArgsUsage: "",
		Description: `
Mirror a object as NFT to BSC

Examples:
# Mirror a object using object id
$ gnfd-cmd object mirror --destChainId 97 --id 1

# Mirror a object using bucket and object name
$ gnfd-cmd object mirror --destChainId 97 --bucketName yourBucketName --objectName yourObjectName
`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     DestChainIdFlag,
				Value:    "",
				Usage:    "target chain id",
				Required: true,
			},
			&cli.StringFlag{
				Name:     IdFlag,
				Value:    "",
				Usage:    "object id",
				Required: false,
			},
			&cli.StringFlag{
				Name:     bucketNameFlag,
				Value:    "",
				Usage:    "bucket name",
				Required: false,
			},
			&cli.StringFlag{
				Name:     objectNameFlag,
				Value:    "",
				Usage:    "object name",
				Required: false,
			},
		},
	}
}

// putObject upload the payload of file, finish the third stage of putObject
func putObject(ctx *cli.Context) error {
	if ctx.NArg() < 1 {
		return toCmdErr(fmt.Errorf("args number error"))
	}

	var (
		isUploadSingleFolder             bool
		bucketName, objectName, filePath string
		objectSize                       int64
		err                              error
		urlInfo                          string
	)

	gnfdClient, err := NewClient(ctx, false)
	if err != nil {
		return err
	}

	supportRecursive := ctx.Bool(recursiveFlag)

	if ctx.NArg() == 1 {
		// upload an empty folder
		urlInfo = ctx.Args().Get(0)
		bucketName, objectName, err = getObjAndBucketNames(urlInfo)
		if err != nil {
			return toCmdErr(err)
		}
		if strings.HasSuffix(objectName, "/") {
			isUploadSingleFolder = true
		} else {
			return toCmdErr(errors.New("no file path to upload, if you need create a folder, the folder name should be end with /"))
		}

		if err = uploadFile(bucketName, objectName, filePath, urlInfo, ctx, gnfdClient, isUploadSingleFolder, true, 0); err != nil {
			return toCmdErr(err)
		}

	} else {
		// upload files in folder in a recursive way
		if supportRecursive {
			urlInfo = ctx.Args().Get(1)
			if err = uploadFolder(urlInfo, ctx, gnfdClient); err != nil {
				return toCmdErr(err)
			}
			return nil
		}

		filePathList := make([]string, 0)
		argNum := ctx.Args().Len()
		for i := 0; i < argNum-1; i++ {
			filePathList = append(filePathList, ctx.Args().Get(i))
		}

		var needUploadMutiFiles bool
		if len(filePathList) > 1 {
			needUploadMutiFiles = true
		}

		// upload multiple files
		if needUploadMutiFiles {
			urlInfo = ctx.Args().Get(argNum - 1)
			bucketName = ParseBucket(urlInfo)
			if bucketName == "" {
				return toCmdErr(errors.New("fail to parse bucket name"))
			}

			for idx, fileName := range filePathList {
				nameList := strings.Split(fileName, "/")
				objectName = nameList[len(nameList)-1]
				objectSize, err = parseFileByArg(ctx, idx)
				if err != nil {
					return toCmdErr(err)
				}

				if err = uploadFile(bucketName, objectName, fileName, urlInfo, ctx, gnfdClient, false, false, objectSize); err != nil {
					fmt.Println("upload object:", objectName, "err", err)
				}
			}
		} else {
			// upload single file
			objectSize, err = parseFileByArg(ctx, 0)
			if err != nil {
				return toCmdErr(err)
			}
			urlInfo = ctx.Args().Get(1)
			bucketName, objectName, err = getObjAndBucketNames(urlInfo)
			if err != nil {
				bucketName = ParseBucket(urlInfo)
				if bucketName == "" {
					return toCmdErr(errors.New("fail to parse bucket name"))
				}
				// if the object name has not been set, set the file name as object name
				objectName = filepath.Base(filePathList[0])
			}
			if err = uploadFile(bucketName, objectName, filePathList[0], urlInfo, ctx, gnfdClient, false, true, objectSize); err != nil {
				return toCmdErr(err)
			}
		}
	}

	return nil
}

// uploadFolder upload folder and the files inside to bucket in a recursive way
func uploadFolder(urlInfo string, ctx *cli.Context,
	gnfdClient client.Client) error {
	// upload folder with recursive flag
	bucketName := ParseBucket(urlInfo)
	if bucketName == "" {
		return errors.New("fail to parse bucket name")
	}

	folderName := ctx.Args().Get(0)
	fileInfo, err := os.Stat(folderName)
	if err != nil {
		return err
	}

	if !fileInfo.IsDir() {
		return errors.New("failed to parse folder path with recursive flag")
	}
	fileInfos := make([]os.FileInfo, 0)
	filePaths := make([]string, 0)
	listFolderErr := filepath.Walk(folderName, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			fileInfos = append(fileInfos, info)
			filePaths = append(filePaths, path)
		}
		return nil
	})

	if listFolderErr != nil {
		return listFolderErr
	}
	// upload folder
	for id, info := range fileInfos {
		//	pathList := strings.Split(info.Name(), "/")
		objectName := filePaths[id]
		if uploadErr := uploadFile(bucketName, objectName, filePaths[id], urlInfo, ctx, gnfdClient, false, false, info.Size()); uploadErr != nil {
			fmt.Printf("failed to upload object: %s, error:%v \n", objectName, uploadErr)
		}
	}

	return nil
}

func uploadFile(bucketName, objectName, filePath, urlInfo string, ctx *cli.Context,
	gnfdClient client.Client, uploadSigleFolder, printTxnHash bool, objectSize int64) error {

	contentType := ctx.String(contentTypeFlag)
	secondarySPAccs := ctx.String(secondarySPFlag)
	partSize := ctx.Uint64(partSizeFlag)
	resumableUpload := ctx.Bool(resumableFlag)

	opts := sdktypes.CreateObjectOptions{}
	if contentType != "" {
		opts.ContentType = contentType
	}

	visibity := ctx.Generic(visibilityFlag)
	if visibity != "" {
		visibityTypeVal, typeErr := getVisibilityType(fmt.Sprintf("%s", visibity))
		if typeErr != nil {
			return typeErr
		}
		opts.Visibility = visibityTypeVal
	}

	// set second sp address if provided by user
	if secondarySPAccs != "" {
		secondarySplist := strings.Split(secondarySPAccs, ",")
		addrList := make([]sdk.AccAddress, len(secondarySplist))
		for idx, addr := range secondarySplist {
			addrList[idx] = sdk.MustAccAddressFromHex(addr)
		}
		opts.SecondarySPAccs = addrList
	}

	c, cancelPutObject := context.WithCancel(globalContext)
	defer cancelPutObject()

	_, err := gnfdClient.HeadObject(c, bucketName, objectName)
	var txnHash string
	// if err==nil, object exist on chain, no need to createObject
	if err != nil {
		if uploadSigleFolder {
			txnHash, err = gnfdClient.CreateFolder(c, bucketName, objectName, opts)
			if err != nil {
				return toCmdErr(err)
			}
		} else {
			// Open the referenced file.
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer file.Close()
			txnHash, err = gnfdClient.CreateObject(c, bucketName, objectName, file, opts)
			if err != nil {
				return toCmdErr(err)
			}
		}
		if printTxnHash {
			fmt.Printf("object %s created on chain \n", objectName)
			fmt.Println("transaction hash: ", txnHash)
		}
	} else {
		fmt.Printf("object %s already exist \n", objectName)
	}

	if objectSize == 0 {
		return nil
	}

	opt := sdktypes.PutObjectOptions{}
	if contentType != "" {
		opt.ContentType = contentType
	}

	opt.DisableResumable = !resumableUpload
	opt.PartSize = partSize

	// Open the referenced file.
	reader, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	if err = gnfdClient.PutObject(c, bucketName, objectName,
		objectSize, reader, opt); err != nil {
		return toCmdErr(err)
	}

	// Check if object is sealed
	timeout := time.After(15 * time.Second)
	ticker := time.NewTicker(2 * time.Second)

	for {
		select {
		case <-timeout:
			return toCmdErr(errors.New("object not sealed after 15 seconds"))
		case <-ticker.C:
			headObjOutput, queryErr := gnfdClient.HeadObject(c, bucketName, objectName)
			if queryErr != nil {
				return queryErr
			}

			if headObjOutput.ObjectInfo.GetObjectStatus().String() == "OBJECT_STATUS_SEALED" {
				ticker.Stop()
				fmt.Printf("upload %s to %s \n", objectName, urlInfo)
				return nil
			}
		}
	}
}

// getObject download the object payload from sp
func getObject(ctx *cli.Context) error {
	if ctx.NArg() < 1 {
		return toCmdErr(fmt.Errorf("args number less than one"))
	}

	urlInfo := ctx.Args().Get(0)
	bucketName, objectName, err := ParseBucketAndObject(urlInfo)
	if err != nil {
		return toCmdErr(err)
	}

	gnfdClient, err := NewClient(ctx, false)
	if err != nil {
		return toCmdErr(err)
	}

	c, cancelGetObject := context.WithCancel(globalContext)
	defer cancelGetObject()

	_, err = gnfdClient.HeadObject(c, bucketName, objectName)
	if err != nil {
		return toCmdErr(ErrObjectNotExist)
	}

	var filePath string
	if ctx.Args().Len() == 1 {
		filePath = objectName
	} else if ctx.Args().Len() == 2 {
		filePath = ctx.Args().Get(1)
		stat, err := os.Stat(filePath)
		if err == nil {
			if stat.IsDir() {
				if strings.HasSuffix(filePath, "/") {
					filePath += objectName
				} else {
					filePath = filePath + "/" + objectName
				}
			}
		}
	}

	opt := sdktypes.GetObjectOptions{}
	startOffset := ctx.Int64(startOffsetFlag)
	endOffset := ctx.Int64(endOffsetFlag)
	partSize := ctx.Uint64(partSizeFlag)
	resumableDownload := ctx.Bool(resumableFlag)

	// flag has been set
	if startOffset != 0 || endOffset != 0 {
		if err = opt.SetRange(startOffset, endOffset); err != nil {
			return toCmdErr(err)
		}
	}

	if resumableDownload {
		opt.PartSize = partSize
		err = gnfdClient.FGetObjectResumable(c, bucketName, objectName, filePath, opt)
		if err != nil {
			return toCmdErr(err)
		}
		fmt.Printf("resumable download object %s, the file path is %s \n", objectName, filePath)
	} else {
		// If file exist, open it in append mode
		fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			return err
		}

		defer fd.Close()

		body, info, err := gnfdClient.GetObject(c, bucketName, objectName, opt)
		if err != nil {
			return toCmdErr(err)
		}

		_, err = io.Copy(fd, body)
		if err != nil {
			return toCmdErr(err)
		}
		fmt.Printf("download object %s, the file path is %s, content length:%d \n", objectName, filePath, uint64(info.Size))
	}

	return nil
}

// cancelCreateObject cancel the created object on chain
func cancelCreateObject(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return toCmdErr(fmt.Errorf("args number should be one"))
	}

	urlInfo := ctx.Args().Get(0)
	bucketName, objectName, err := ParseBucketAndObject(urlInfo)
	if err != nil {
		return toCmdErr(err)
	}

	cli, err := NewClient(ctx, false)
	if err != nil {
		return toCmdErr(err)
	}

	c, cancelCancelCreate := context.WithCancel(globalContext)
	defer cancelCancelCreate()

	_, err = cli.HeadObject(c, bucketName, objectName)
	if err != nil {
		return toCmdErr(ErrObjectNotCreated)
	}

	_, err = cli.CancelCreateObject(c, bucketName, objectName, sdktypes.CancelCreateOption{TxOpts: &TxnOptionWithSyncMode})
	if err != nil {
		return toCmdErr(err)
	}

	fmt.Println("cancel create object:", objectName)
	return nil
}

func listObjects(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return toCmdErr(fmt.Errorf("args number should be one"))
	}

	bucketName, prefixName, err := ParseBucketAndPrefix(ctx.Args().Get(0))
	if err != nil {
		return toCmdErr(err)
	}

	client, err := NewClient(ctx, false)
	if err != nil {
		return toCmdErr(err)
	}
	c, cancelList := context.WithCancel(globalContext)
	defer cancelList()

	_, err = client.HeadBucket(c, bucketName)
	if err != nil {
		return toCmdErr(ErrBucketNotExist)
	}

	supportRecursive := ctx.Bool(recursiveFlag)
	err = listObjectByPage(client, c, bucketName, prefixName, supportRecursive)
	if err != nil {
		return toCmdErr(err)
	}

	return nil
}

func listObjectByPage(cli client.Client, c context.Context, bucketName, prefixName string, isRecursive bool) error {
	var (
		listResult        sdktypes.ListObjectsResult
		continuationToken string
		err               error
	)

	for {
		if isRecursive {
			listResult, err = cli.ListObjects(c, bucketName, sdktypes.ListObjectsOptions{ShowRemovedObject: false,
				MaxKeys:           defaultMaxKey,
				ContinuationToken: continuationToken,
				Prefix:            prefixName})
		} else {
			listResult, err = cli.ListObjects(c, bucketName, sdktypes.ListObjectsOptions{ShowRemovedObject: false,
				Delimiter:         "/",
				MaxKeys:           defaultMaxKey,
				ContinuationToken: continuationToken,
				Prefix:            prefixName})
		}
		if err != nil {
			return toCmdErr(err)
		}

		printListResult(listResult)
		if !listResult.IsTruncated {
			break
		}

		continuationToken = listResult.NextContinuationToken
	}
	return nil
}

func printListResult(listResult sdktypes.ListObjectsResult) {
	for _, object := range listResult.Objects {
		info := object.ObjectInfo
		location, _ := time.LoadLocation("Asia/Shanghai")
		t := time.Unix(info.CreateAt, 0).In(location)

		fmt.Printf("%s %15d %s \n", t.Format(iso8601DateFormat), info.PayloadSize, info.ObjectName)
	}
	// list the folders
	for _, prefix := range listResult.CommonPrefixes {
		fmt.Printf("%s %15s %s \n", strings.Repeat(" ", len(iso8601DateFormat)), "PRE", prefix)
	}

}

func updateObject(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return toCmdErr(fmt.Errorf("args number should be one"))
	}

	urlInfo := ctx.Args().First()
	bucketName, objectName, err := ParseBucketAndObject(urlInfo)
	if err != nil {
		return toCmdErr(err)
	}

	client, err := NewClient(ctx, false)
	if err != nil {
		return toCmdErr(err)
	}

	c, cancelUpdateObject := context.WithCancel(globalContext)
	defer cancelUpdateObject()

	visibility := ctx.Generic(visibilityFlag)
	if visibility == "" {
		return toCmdErr(fmt.Errorf("visibity must set to be updated"))
	}

	visibilityType, typeErr := getVisibilityType(fmt.Sprintf("%s", visibility))
	if typeErr != nil {
		return typeErr
	}

	txnHash, err := client.UpdateObjectVisibility(c, bucketName, objectName, visibilityType, sdktypes.UpdateObjectOption{TxOpts: &TxnOptionWithSyncMode})
	if err != nil {
		fmt.Println("update object visibility error:", err.Error())
		return nil
	}

	err = waitTxnStatus(client, c, txnHash, "UpdateObject")
	if err != nil {
		return toCmdErr(err)
	}

	objectDetail, err := client.HeadObject(c, bucketName, objectName)
	if err != nil {
		// head fail, no need to print the error
		return nil
	}

	fmt.Printf("update object visibility finished, latest object visibility:%s\n", objectDetail.ObjectInfo.GetVisibility().String())
	fmt.Println("transaction hash: ", txnHash)
	return nil
}

func replicateObject(ctx *cli.Context) error {

	GnfdReceiveMsgHeader := "X-Gnfd-Receive-Msg"
	ReplicateObjectPiecePath := "/greenfield/receiver/v1/replicate-piece"
	headerContent := "7b227461736b223a7b226372656174655f74696d65223a313639353039363732372c227570646174655f74696d65223a313639353039363732372c227461736b5f7072696f72697479223a3235357d2c226f626a6563745f696e666f223a7b226f776e6572223a22307845323233344145373863443933353034443566333833636538364437333238373245374366313536222c226275636b65745f6e616d65223a22746573746275636b65743233222c226f626a6563745f6e616d65223a22647373356473313131317831222c226964223a2231222c227061796c6f61645f73697a65223a313437363339353030382c227669736962696c697479223a332c22636f6e74656e745f74797065223a226170706c69636174696f6e2f6f637465742d73747265616d222c226372656174655f6174223a313639353039363731382c22636865636b73756d73223a5b224637477076434750795148515a633342447a2f6f6b6237426b2f6458644b73674444504154395578506d673d222c224c7352624951534d434f536b336b3243396f705134534a495271374c2b684a384139747334477a303578633d222c224c7352624951534d434f536b336b3243396f705134534a495271374c2b684a384139747334477a303578633d222c224c7352624951534d434f536b336b3243396f705134534a495271374c2b684a384139747334477a303578633d222c224c7352624951534d434f536b336b3243396f705134534a495271374c2b684a384139747334477a303578633d222c224c7352624951534d434f536b336b3243396f705134534a495271374c2b684a384139747334477a303578633d222c224c7352624951534d434f536b336b3243396f705134534a495271374c2b684a384139747334477a303578633d225d7d2c2273746f726167655f706172616d73223a7b2276657273696f6e65645f706172616d73223a7b226d61785f7365676d656e745f73697a65223a31363737373231362c22726564756e64616e745f646174615f6368756e6b5f6e756d223a342c22726564756e64616e745f7061726974795f6368756e6b5f6e756d223a322c226d696e5f6368617267655f73697a65223a313034383537367d2c226d61785f7061796c6f61645f73697a65223a36383731393437363733362c226273635f6d6972726f725f6275636b65745f72656c617965725f666565223a2231333030303030303030303030303030222c226273635f6d6972726f725f6275636b65745f61636b5f72656c617965725f666565223a22323530303030303030303030303030222c226273635f6d6972726f725f6f626a6563745f72656c617965725f666565223a2231333030303030303030303030303030222c226273635f6d6972726f725f6f626a6563745f61636b5f72656c617965725f666565223a22323530303030303030303030303030222c226273635f6d6972726f725f67726f75705f72656c617965725f666565223a2231333030303030303030303030303030222c226273635f6d6972726f725f67726f75705f61636b5f72656c617965725f666565223a22323530303030303030303030303030222c226d61785f6275636b6574735f7065725f6163636f756e74223a3130302c22646973636f6e74696e75655f636f756e74696e675f77696e646f77223a31303030302c22646973636f6e74696e75655f6f626a6563745f6d6178223a31383434363734343037333730393535313631352c22646973636f6e74696e75655f6275636b65745f6d6178223a31383434363734343037333730393535313631352c22646973636f6e74696e75655f636f6e6669726d5f706572696f64223a352c22646973636f6e74696e75655f64656c6574696f6e5f6d6178223a322c227374616c655f706f6c6963795f636c65616e75705f6d6178223a3230302c226d696e5f71756f74615f7570646174655f696e74657276616c223a323539323030302c226d61785f6c6f63616c5f7669727475616c5f67726f75705f6e756d5f7065725f6275636b6574223a31302c226f705f6d6972726f725f6275636b65745f72656c617965725f666565223a22313330303030303030303030303030222c226f705f6d6972726f725f6275636b65745f61636b5f72656c617965725f666565223a223235303030303030303030303030222c226f705f6d6972726f725f6f626a6563745f72656c617965725f666565223a22313330303030303030303030303030222c226f705f6d6972726f725f6f626a6563745f61636b5f72656c617965725f666565223a223235303030303030303030303030222c226f705f6d6972726f725f67726f75705f72656c617965725f666565223a22313330303030303030303030303030222c226f705f6d6972726f725f67726f75705f61636b5f72656c617965725f666565223a223235303030303030303030303030227d2c22726564756e64616e63795f696478223a342c2270696563655f73697a65223a343139343330342c2270696563655f636865636b73756d223a2275352b4e39685230306c35782b67427949786a4e4f484f57796863325946345353494963774e34394f76673d222c227369676e6174757265223a226d4d724e54667761322b6a7377384553336461794b396c5449734b707174546e466942477757583270767767636e684d71595137564344484d596d762b6963543866345a78594f7542626a7433535a496b7632315667413d222c22676c6f62616c5f7669727475616c5f67726f75705f6964223a317d"

	// sp addr could be an endpoint or sp operator address
	spAddressInfo := ctx.Args().Get(0)
	filePath := "test.file"

	fReader, err := os.Open(filePath)
	// If any error fail quickly here.
	if err != nil {
		return err
	}
	defer fReader.Close()

	// Save the file stat.
	_, err = fReader.Stat()
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, spAddressInfo+ReplicateObjectPiecePath, fReader)
	if err != nil {
		return toCmdErr(err)
		return err
	}

	req.Header.Add(GnfdReceiveMsgHeader, headerContent)

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		}}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to replicate piece, status_code(%d) endpoint(%s)", resp.StatusCode, spAddressInfo)
	}

	// construct err responses and messages
	err = ConstructErrResponse(resp, "bucket", "objecrt")
	if err != nil {
		// dump error msg
		return err
	}

	return nil
}

func getUploadInfo(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return toCmdErr(fmt.Errorf("args number should be 1"))
	}

	urlInfo := ctx.Args().Get(0)
	bucketName, objectName, err := getObjAndBucketNames(urlInfo)
	if err != nil {
		return toCmdErr(err)
	}

	client, err := NewClient(ctx, false)
	if err != nil {
		return toCmdErr(err)
	}

	c, cancelGetUploadInfo := context.WithCancel(globalContext)
	defer cancelGetUploadInfo()

	uploadInfo, err := client.GetObjectUploadProgress(c, bucketName, objectName)
	if err != nil {
		return toCmdErr(err)
	}

	fmt.Println("uploading progress:", uploadInfo)
	return nil
}

func pathExists(path string) (bool, int64, error) {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, 0, nil
	}

	if err == nil {
		if stat.IsDir() {
			return false, 0, fmt.Errorf("not support upload dir without recursive flag")
		}
		return true, stat.Size(), nil
	}

	return false, 0, err
}

func getObjAndBucketNames(urlInfo string) (string, string, error) {
	bucketName, objectName, err := ParseBucketAndObject(urlInfo)
	if bucketName == "" || objectName == "" || err != nil {
		return "", "", fmt.Errorf("fail to parse bucket name or object name")
	}
	return bucketName, objectName, nil
}

func mirrorObject(ctx *cli.Context) error {
	client, err := NewClient(ctx, false)
	if err != nil {
		return toCmdErr(err)
	}
	id := math.NewUint(0)
	if ctx.String(IdFlag) != "" {
		id = math.NewUintFromString(ctx.String(IdFlag))
	}
	destChainId := ctx.Int64(DestChainIdFlag)
	bucketName := ctx.String(bucketNameFlag)
	objectName := ctx.String(objectNameFlag)
	c, cancelContext := context.WithCancel(globalContext)
	defer cancelContext()

	txResp, err := client.MirrorObject(c, sdk.ChainID(destChainId), id, bucketName, objectName, types.TxOption{})
	if err != nil {
		return toCmdErr(err)
	}
	fmt.Printf("mirror object succ, txHash: %s\n", txResp.TxHash)
	return nil
}

// ErrResponse define the information of the error response
type ErrResponse struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	StatusCode int
}

// Error returns the error msg
func (r ErrResponse) Error() string {
	return fmt.Sprintf("statusCode %v : code : %s  (Message: %s)",
		r.StatusCode, r.Code, r.Message)
}

// ConstructErrResponse  checks the response is an error response
func ConstructErrResponse(r *http.Response, bucketName, objectName string) error {
	if c := r.StatusCode; 200 <= c && c <= 299 {
		return nil
	}

	if r == nil {
		return ErrResponse{
			StatusCode: r.StatusCode,
			Code:       "dss",
			Message:    "Response is empty ",
		}
	}

	errResp := ErrResponse{}
	errResp.StatusCode = r.StatusCode

	// read err body of max 10M size
	const maxBodySize = 10 * 1024 * 1024
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
	if err != nil {
		return ErrResponse{
			StatusCode: r.StatusCode,
			Code:       "InternalError",
			Message:    err.Error(),
		}
	}
	// decode the xml content from response body
	decodeErr := xml.NewDecoder(bytes.NewReader(body)).Decode(&errResp)
	if decodeErr != nil {
		switch r.StatusCode {
		case http.StatusNotFound:
			if bucketName != "" {
				if objectName == "" {
					errResp = ErrResponse{
						StatusCode: r.StatusCode,
						Code:       "NoSuchBucket",
						Message:    "The specified bucket does not exist.",
					}
				} else {
					errResp = ErrResponse{
						StatusCode: r.StatusCode,
						Code:       "NoSuchObject",
						Message:    "The specified object does not exist.",
					}
				}
			}
		case http.StatusForbidden:
			errResp = ErrResponse{
				StatusCode: r.StatusCode,
				Code:       "AccessDenied",
				Message:    "no permission to access the resource",
			}
		default:
			errBody := bytes.TrimSpace(body)
			msg := "dss"
			if len(errBody) > 0 {
				msg = string(errBody)
			}
			fmt.Println("default error msg :", msg)
			errResp = ErrResponse{
				StatusCode: r.StatusCode,
				Code:       "dss",
				Message:    msg,
			}
		}
	}

	return errResp
}
