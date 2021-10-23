package main

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
    "github.com/gocql/gocql"
    "fmt"
    "os"
    "time"
    "log"
    "bufio"
    "net/http"
    "io"
    "compress/gzip"
    //"strings"
)

const (
    region              = "us-east-1"
    bucket              = "orats-snapshots"
    keyspacesHost       = "cassandra.us-east-1.amazonaws.com:9142"
    keyspacesCRTUrl     = "https://certs.secureserver.net/repository/sf-class2-root.crt"
    keyspacesCRTName    = "sf-class2-root.crt"
    mostRecentTSFile    = "mostRecent.time"
    timeLayout          = time.RFC3339
    tmpLocalFileGZName  = "snapshot.csv.gz"
    tmpLocalFileCSVName = "snapshot.csv"
    keyspacesUser       = "kryptek-aws-api-at-439416760204"
    keyspacesPass       = "hlnWFtG3FIUrgcnoI/vYStzdwl9Y6nQbPrrbZIKQEqk="
    keyspaceName        = "orats"
    snapshotTableName   = "snapshots"
    errorRetrySeconds   = 10
)

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
var columnsTypes = [...]string{"'%s'",    //ticker
                                "'%s'",   //tradeDate
                                "'%s'",   //expirDate
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
                                "'%s'",   //quoteDate
                                "'%s'",   //updatedAt
                                "'%s'",   //snapShotEstTime
                                "'%s'",   //snapShotDate
                                "'%s'",   //expiryTod
                            }
//this is the format string to build the INSERT query
var insertQuery string
func buildInsertQuery() {
    insertQuery = insertQuery + "COPY " + keyspaceName + "." + snapshotTableName
    insertQuery = insertQuery + " ("
    for _, c := range columns {
        insertQuery = insertQuery + c + ", "
    }
    insertQuery = insertQuery[:len(insertQuery)-2]
    insertQuery = insertQuery + ")"

    // values ("
    // for _, c := range columnsTypes {
    //     insertQuery = insertQuery + c + ", "
    // }   
    // insertQuery = insertQuery[:len(insertQuery)-2]
    // insertQuery = insertQuery + ")"
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
                                Username: keyspacesUser,
                                Password: keyspacesPass}
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
            return err
    }
    defer cqlSession.Close()

    // //read the CSV file, skip first line then read line by line and insert
    // file, err := os.OpenFile(csvFile, os.O_RDWR, 0755)
    // if err != nil {
    //     log.Printf("Unable to open CSV file %s", csvFile)
    //     return err
    // }
    // defer file.Close()

    // scanner := bufio.NewScanner(file)
    // //skip first line
    // scanner.Scan()
    // for scanner.Scan() {

    //     log.Printf(scanner.Text() + "\n")
    //     s := strings.Split(scanner.Text(), ",")

    //     is := make([]interface{}, len(s), len(s))
    //     for i := range s {
    //         is[i] = s[i]
    //     }

    //     queryString := fmt.Sprintf(insertQuery, is...)
        
    //     log.Printf(queryString)
    //     os.Exit(0)
    // }

    //run a sample query from the system keyspace
    insertQuery = insertQuery + " FROM '" + csvFile + "' WITH delimiter=',' AND header=TRUE;"
    log.Printf(insertQuery)
    var text string
    iter := cqlSession.Query("SELECT * from orats.snapshots;").Iter()
    for iter.Scan(&text) {
            log.Printf("Row:", text)
    }
    fmt.Println("Done")
    if err := iter.Close(); err != nil {
        return err
    }
    cqlSession.Close()
    return nil
}


func Init() error {
    log.Printf("Downloading keyspaces cert: keyspacesCRTUrl")
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

func main() {
    var mostRecent time.Time
    Init()

    //this is the most recent time stamp that was processed from the bucket
    mostRecent, _ = GetMostRecentTimestamp()

    // Initialize a session that the SDK will use to load
    // credentials from the shared credentials file ~/.aws/credentials.
    sess, err := session.NewSession(&aws.Config{
        Region: aws.String(region)},
    )
    // Create S3 service client
    svc := s3.New(sess)
    resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
    if err != nil {
	    exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
    }
    log.Printf("Listing bucket items")
    for i, item := range resp.Contents {
        if i > 1 {
            os.Exit(0)
        }
	    //fmt.Println("Name:         ", *item.Key)
	    //fmt.Println("Last modified:", *item.LastModified)
	    //fmt.Println("Size:         ", *item.Size)
	    //fmt.Println("Storage class:", *item.StorageClass)

	    if mostRecent.Before(*item.LastModified) {
            log.Printf("Downloading New Un-read file %s %v\n", *item.Key, *item.LastModified)

            //download the file from s3 bucket
            // err = DownloadS3File(bucket, *item.Key)
            // for err != nil {
            //     log.Printf("Error downloading from bucket %v\n", err)
            //     log.Printf("Retrying after 5 seconds...")
            //     time.Sleep(5 * time.Second)
            //     err = DownloadS3File(bucket, *item.Key)
            // }

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

            //store the most recent time stamp for future reference
            log.Printf("Saving most recent time stamp %v", mostRecent)
            mostRecent = *item.LastModified
            //save the most recent timestamp in file   
            file, err := os.OpenFile(mostRecentTSFile, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
            if err != nil {
                log.Fatalf("failed Trunc'ing file: %:s", err)

            }
            file.Write([]byte(mostRecent.Format(timeLayout)))
            file.Close()
        }
    }
}

func exitErrorf(msg string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, msg+"\n", args...)
    os.Exit(1)
}
