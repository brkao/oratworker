package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/gocql/gocql"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	region              = "us-east-1"
	bucketCompressed    = "obuck"
	bucketUncompressed  = "obuck1"
	keyspacesHost       = "cassandra.us-east-1.amazonaws.com:9142"
	keyspacesCRTUrl     = "https://certs.secureserver.net/repository/sf-class2-root.crt"
	keyspacesCRTName    = "/tmp/sf-class2-root.crt"
	mostRecentTSFile    = "mostRecent.time"
	timeLayout          = time.RFC3339
	tmpLocalFileGZName  = "snapshot.csv.gz"
	tmpLocalFileCSVName = "snapshot.csv"
	keyspaceName        = "orats"
	snapshotTableName   = "snapshots"
	errorRetrySeconds   = 10
)

var AccessKeyId string
var SecretAccessKey string
var KeyspacesUser string
var KeyspacesPass string

var columns = [...]string{"\"ticker\"",
	"\"tradeDate\"",
	"\"expirDate\"",
	"\"dte\"",
	"\"strike\"",
	"\"stockPrice\"",
	"\"callVolume\"",
	"\"callOpenInterest\"",
	"\"callBidSize\"",
	"\"callAskSize\"",
	"\"putVolume\"",
	"\"putOpenInterest\"",
	"\"putBidSize\"",
	"\"putAskSize\"",
	"\"callBidPrice\"",
	"\"callValue\"",
	"\"callAskPrice\"",
	"\"putBidPrice\"",
	"\"putValue\"",
	"\"putAskPrice\"",
	"\"callBidIv\"",
	"\"callMidIv\"",
	"\"callAskIv\"",
	"\"smvVol\"",
	"\"putBidIv\"",
	"\"putMidIv\"",
	"\"putAskIv\"",
	"\"residualRate\"",
	"\"delta\"",
	"\"gamma\"",
	"\"theta\"",
	"\"vega\"",
	"\"rho\"",
	"\"phi\"",
	"\"driftlessTheta\"",
	"\"callSmvVol\"",
	"\"putSmvVol\"",
	"\"extSmvVol\"",
	"\"extCallValue\"",
	"\"extPutValue\"",
	"\"spotPrice\"",
	"\"quoteDate\"",
	"\"updatedAt\"",
	"\"snapShotEstTime\"",
	"\"snapShotDate\"",
	"\"expiryTod\"",
}
var columnsTypes = [...]string{"'%s'", //ticker
	"'%s'", //tradeDate
	"'%s'", //expirDate
	"%s",   //dte
	"%s",   //strike
	"%s",   //stockPrice
	"%s",   //callVolume
	"%s",   //callOpenInterest
	"%s",   //callBidSize
	"%s",   //callAskSize
	"%s",   //putVolume
	"%s",   //putOpenInterest
	"%s",   //putBidSize
	"%s",   //putAskSize
	"%s",   //callBidPrice
	"%s",   //callValue
	"%s",   //callAskPrice
	"%s",   //putBidPrice
	"%s",   //putValue
	"%s",   //putAskPrice
	"%s",   //callBidIv
	"%s",   //callMidIv
	"%s",   //callAskIv
	"%s",   //smvVol
	"%s",   //putBidIv
	"%s",   //putMidIv
	"%s",   //putAskIv
	"%s",   //residualRate
	"%s",   //delta
	"%s",   //gamma
	"%s",   //theta
	"%s",   //vega
	"%s",   //rho
	"%s",   //phi
	"%s",   //driftlessTheta
	"%s",   //callSmvVol
	"%s",   //putSmvVol
	"%s",   //extSmvVol
	"%s",   //extCallValue
	"%s",   //extPutValue
	"%s",   //spotPrice
	"'%s'", //quoteDate
	"'%s'", //updatedAt
	"'%s'", //snapShotEstTime
	"'%s'", //snapShotDate
	"'%s'", //expiryTod
}

//this is the format string to build the INSERT query
var insertQuery string

func buildInsertQuery() {
	insertQuery = insertQuery + "INSERT INTO " + keyspaceName + "." + snapshotTableName
	insertQuery = insertQuery + " ("
	for _, c := range columns {
		insertQuery = insertQuery + c + ", "
	}
	insertQuery = insertQuery[:len(insertQuery)-2]
	insertQuery = insertQuery + ")" + " values ("
	for _, c := range columnsTypes {
		insertQuery = insertQuery + c + ", "
	}
	insertQuery = insertQuery[:len(insertQuery)-2]
	insertQuery = insertQuery + ")"
}

func DownloadFile(filepath string, url string) error {
	log.Printf("Downloading %s\n", url)
	_, err := os.Stat(filepath)
	if err == nil {
		log.Printf("Deleting file %s\n", filepath)
		os.Remove(filepath)
	}

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	log.Printf("Download Done - file %s\n", filepath)
	return nil
}

func DownloadS3File(bucket, s3file string) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	//remove existing downloaded file if present
	os.Remove(tmpLocalFileGZName)
	//create local tmp file to store the snapshot
	file, err := os.Create(tmpLocalFileGZName)
	if err != nil {
		return err
	}

	//download the new snapshot to local tmp file
	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3file),
	})
	if err != nil {
		return err
	}
	log.Println("Downloaded", s3file, numBytes, "bytes to ", file.Name())
	return nil
}

func unzip(srcFile, dstFile string) error {
	gzipFile, err := os.Open(srcFile)
	if err != nil {
		return err
	}

	gzipReader, err := gzip.NewReader(gzipFile)
	if err != nil {
		fmt.Printf("Errr\n")
		return err
	}
	defer gzipReader.Close()

	outfileWriter, err := os.Create(dstFile)
	if err != nil {
		return err
	}
	defer outfileWriter.Close()

	// Copy contents of gzipped file to output file
	_, err = io.Copy(outfileWriter, gzipReader)
	if err != nil {
		return err
	}
	return nil
}

func InsertCSVtoKeyspaces(csvFile string) error {
	// add the Amazon Keyspaces service endpoint
	cluster := gocql.NewCluster(keyspacesHost)
	// add your service specific credentials
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: KeyspacesUser,
		Password: KeyspacesPass}
	// provide the path to the sf-class2-root.crt
	cluster.SslOpts = &gocql.SslOptions{
		CaPath: keyspacesCRTName,
	}

	// Override default Consistency to LocalQuorum
	cluster.Consistency = gocql.LocalQuorum

	// Disable initial host lookup
	cluster.DisableInitialHostLookup = true
	cqlSession, err := cluster.CreateSession()
	if err != nil {
		log.Printf("Unable to create sql session: %v\n", err)
		return err
	}
	defer cqlSession.Close()

	//read the CSV file, skip first line then read line by line and insert
	file, err := os.OpenFile(csvFile, os.O_RDWR, 0755)
	if err != nil {
		log.Printf("Unable to open CSV file %s", csvFile)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	//skip first line
	scanner.Scan()
	for scanner.Scan() {
		log.Printf(scanner.Text() + "\n")
		s := strings.Split(scanner.Text(), ",")

		is := make([]interface{}, len(s), len(s))
		for i := range s {
			is[i] = s[i]
		}

		queryString := fmt.Sprintf(insertQuery, is...)

		log.Printf(queryString)
		err = cqlSession.Query(queryString).Exec()
		if err != nil {
			log.Printf("gocal query error %v\n", err)
			return err
		}
	}
	return nil
}

func Init() error {
	AccessKeyId = os.Getenv("ACCESS_KEY_ID")
	SecretAccessKey = os.Getenv("SECRET_ACCESS_KEY")
	KeyspacesUser = os.Getenv("KEYSPACES_USER")
	KeyspacesPass = os.Getenv("KEYSPACES_PASS")
	fmt.Println(len(KeyspacesUser), len(KeyspacesPass))

	if len(AccessKeyId) == 0 || len(SecretAccessKey) == 0 {
		fmt.Printf("Invalid S3 credentials\n")
		return errors.New("Invalid S3 Credentials")
	}
	if len(KeyspacesUser) == 0 || len(KeyspacesPass) == 0 {
		fmt.Printf("Invalid Keyspaces credentials\n")
		return errors.New("Invalid Keyspaces credentials")
	}

	os.Setenv("AWS_ACCESS_KEY_ID", AccessKeyId)
	os.Setenv("AWS_SECRET_ACCESS_KEY", SecretAccessKey)
	os.Setenv("AWS_SESSION_TOKEN", "")

	log.Printf("Downloading keyspaces cert: %s", keyspacesCRTUrl)
	err := DownloadFile(keyspacesCRTName, keyspacesCRTUrl)
	if err != nil {
		return err
	}

	log.Printf("Building Insert Query...")
	buildInsertQuery()
	log.Println("QUERY : ", insertQuery)
	return nil
}

func GetMostRecentTimestamp() (time.Time, error) {
	var mostRecent time.Time
	//read the time stamp of the most recently processed file
	file, err := os.OpenFile(mostRecentTSFile, os.O_RDWR, 0755)
	if err != nil {
		log.Printf("Most recent file doesn't exist")
		return mostRecent, nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan()
	mostR := scanner.Text()
	if mostR == "" {
		log.Printf("No Most Recent Time Stamp\n")
	} else {
		mostRecent, err = time.Parse(timeLayout, mostR)
		if err != nil {
			log.Printf("Error parsing most recent timestamp: %v\n", err)
			return mostRecent, err
		}
		log.Printf("Most Recent Time Stamp %v\n", mostR)
	}
	return mostRecent, nil
}

func handler(ctx context.Context, s3Event events.S3Event) {
	log.Printf("Lambda Handler Called\n")
	for _, record := range s3Event.Records {
		s3record := record.S3
		log.Printf("[%s - %s] Bucket = %s, Key = %s \n",
			record.EventSource, record.EventTime, s3record.Bucket.Name, s3record.Object.Key)
		log.Printf("New file from bucket %s file %s", s3record.Bucket.Name, s3record.Object.Key)

		//check file path to make sure it's csv
		fileExtension := filepath.Ext(s3record.Object.Key)
		if fileExtension != ".csv" {
			log.Printf("No a CSV file, Done")
			return
		}

		log.Printf("Creating s3 session  ...")
		//get the item from bucket
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String(region)},
		)
		svc := s3.New(sess)

		log.Printf("Reading file from s3 ...")
		rawObject, err := svc.GetObject(
			&s3.GetObjectInput{
				Bucket: aws.String(s3record.Bucket.Name),
				Key:    aws.String(s3record.Object.Key),
			})
		if err != nil {
			log.Printf("Error GetObject: %v\n", err)
			return
		}
		buf := new(bytes.Buffer)
		buf.ReadFrom(rawObject.Body)

		log.Printf("Connecting to keyspaces ...")
		// add the Amazon Keyspaces service endpoint
		cluster := gocql.NewCluster(keyspacesHost)
		// add your service specific credentials
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: KeyspacesUser,
			Password: KeyspacesPass}
		// provide the path to the sf-class2-root.crt
		cluster.SslOpts = &gocql.SslOptions{
			CaPath: keyspacesCRTName,
		}
		// Override default Consistency to LocalQuorum
		cluster.Consistency = gocql.LocalQuorum
		// Disable initial host lookup
		cluster.DisableInitialHostLookup = true
		cqlSession, err := cluster.CreateSession()
		if err != nil {
			log.Printf("Unable to create sql session: %v\n", err)
			return
		}
		defer cqlSession.Close()

		log.Printf("Inserting CSV into keyspaces...")
		scanner := bufio.NewScanner(buf)
		//skip first line
		scanner.Scan()
		for scanner.Scan() {
			log.Printf(scanner.Text() + "\n")
			s := strings.Split(scanner.Text(), ",")

			is := make([]interface{}, len(s), len(s))
			for i := range s {
				is[i] = s[i]
			}

			queryString := fmt.Sprintf(insertQuery, is...)

			log.Printf(queryString)
			err = cqlSession.Query(queryString).Exec()
			if err != nil {
				log.Printf("gocal query error %v\n", err)
				return
			}
		}
		return
	}
	log.Printf("Lambda Handler Done\n")
}

func main() {
	err := Init()
	if err != nil {
		log.Printf("Init() error: %v", err)
		return
	}

	//this environment is set when called from labmda
	isLambda := os.Getenv("CALLED_BY_LAMBDA")
	if len(isLambda) != 0 {
		log.Printf("Called by Lambda")
		lambda.Start(handler)
		return
	}
	//if this is not called by lambda, check s3 bucket and process every new file

	var mostRecent time.Time
	//this is the most recent time stamp that was processed from the bucket
	mostRecent, _ = GetMostRecentTimestamp()

	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	// Create S3 service client
	svc := s3.New(sess)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketCompressed)})
	for err != nil {
		log.Printf("Unable to list items in bucket %q, %v", bucketCompressed, err)
		log.Printf("Retrying after %d seconds...", errorRetrySeconds)
		resp, err = svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketCompressed)})
	}

	log.Printf("Listing bucket items")
	for _, item := range resp.Contents {
		//fmt.Println("Name:         ", *item.Key)
		//fmt.Println("Last modified:", *item.LastModified)
		//fmt.Println("Size:         ", *item.Size)
		//fmt.Println("Storage class:", *item.StorageClass)

		if mostRecent.Before(*item.LastModified) {
			log.Printf("Downloading New Un-read file %s %v\n", *item.Key, *item.LastModified)

			//download the file from s3 bucket
			err = DownloadS3File(bucketCompressed, *item.Key)
			for err != nil {
				log.Printf("Error downloading from bucket %v\n", err)
				log.Printf("Retrying after %d seconds...", errorRetrySeconds)
				time.Sleep(5 * time.Second)
				err = DownloadS3File(bucketCompressed, *item.Key)
			}

			//unzip and file locally
			log.Printf("Unzippping...")
			err = unzip(tmpLocalFileGZName, tmpLocalFileCSVName)
			for err != nil {
				log.Printf("Error unzippping %v", err)
				log.Printf("Retrying after %d seconds...", errorRetrySeconds)
				os.Remove(tmpLocalFileCSVName)
				time.Sleep(errorRetrySeconds * time.Second)
				err = unzip(tmpLocalFileGZName, tmpLocalFileCSVName)
			}

			log.Printf("Inserting CSV into keyspaces...")
			err = InsertCSVtoKeyspaces(tmpLocalFileCSVName)
			for err != nil {
				log.Printf("Error inserting CSV to keyspaces %v", err)
				log.Printf("Retrying after %d seconds...", errorRetrySeconds)
				time.Sleep(errorRetrySeconds * time.Second)
				err = InsertCSVtoKeyspaces(tmpLocalFileCSVName)
			}

			//delete tmpLocalFileCSVName
			os.Remove(tmpLocalFileCSVName)

			//store the most recent time stamp for future reference
			log.Printf("Saving most recent time stamp %v", mostRecent)
			mostRecent = *item.LastModified
			//save the most recent timestamp in file
			file, err := os.OpenFile(mostRecentTSFile, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
			if err != nil {
				log.Fatalf("Failed recording most recent time to file: %s %s",
					mostRecentTSFile, err)
			}
			file.Write([]byte(mostRecent.Format(timeLayout)))
			file.Close()
		} else {
			log.Printf("File previously processed %s %v\n", *item.Key, *item.LastModified)
		}
	}
	log.Printf("Done checking bucket\n")
}
