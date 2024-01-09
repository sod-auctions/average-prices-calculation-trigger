package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

type PriceAverage struct {
	RowCount    int64
	QuantitySum int64
	P05Sum      int64
	P10Sum      int64
	P25Sum      int64
	P50Sum      int64
	P75Sum      int64
	P90Sum      int64
}

func init() {
	log.SetFlags(0)
}

func download(client s3.S3, ctx context.Context, bucket string, path string) (*s3.GetObjectOutput, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(path),
	}

	objects, err := client.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}

	// Filter for .csv files and find the most recent one
	var latestKey string
	var latestTime time.Time
	for _, object := range objects.Contents {
		key := *object.Key
		if strings.HasSuffix(key, ".csv") && object.LastModified.After(latestTime) {
			latestKey = key
			latestTime = *object.LastModified
		}
	}

	// If no .csv file was found, return an error
	if latestKey == "" {
		return nil, fmt.Errorf("no .csv file found in path")
	}

	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(latestKey),
	}

	return client.GetObjectWithContext(ctx, getObjectInput)
}

func upload(client *s3.S3, bucket string, key string, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size and read the file content into a buffer
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	_, err = file.Read(buffer)
	if err != nil {
		return err
	}

	// Create a PutObjectInput struct
	input := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(buffer),
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(http.DetectContentType(buffer)),
	}

	// Upload the file
	_, err = client.PutObject(input)
	if err != nil {
		return err
	}

	return nil
}

func read(closer io.ReadCloser) (map[string]*PriceAverage, error) {
	priceAverages := make(map[string]*PriceAverage)
	r := csv.NewReader(closer)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		realmId, _ := strconv.Atoi(row[0])
		auctionHouseId, _ := strconv.Atoi(row[1])
		itemId, _ := strconv.Atoi(row[2])
		rowCount, _ := strconv.Atoi(row[3])
		quantitySum, _ := strconv.Atoi(row[4])
		p05Sum, _ := strconv.Atoi(row[5])
		p10Sum, _ := strconv.Atoi(row[6])
		p25Sum, _ := strconv.Atoi(row[7])
		p50Sum, _ := strconv.Atoi(row[8])
		p75Sum, _ := strconv.Atoi(row[9])
		p90Sum, _ := strconv.Atoi(row[10])

		priceAverage := &PriceAverage{
			RowCount:    int64(rowCount),
			QuantitySum: int64(quantitySum),
			P05Sum:      int64(p05Sum),
			P10Sum:      int64(p10Sum),
			P25Sum:      int64(p25Sum),
			P50Sum:      int64(p50Sum),
			P75Sum:      int64(p75Sum),
			P90Sum:      int64(p90Sum),
		}

		key := fmt.Sprintf("%d-%d-%d", realmId, auctionHouseId, itemId)
		priceAverages[key] = priceAverage
	}
	return priceAverages, nil
}

func write(priceAverages map[string]*PriceAverage) error {
	file, err := os.Create("/tmp/price_averages.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write([]string{
		"realmId",
		"auctionHouseId",
		"itemId",
		"rowCount",
		"quantitySum",
		"p05Sum",
	})
	if err != nil {
		return err
	}

	for key, value := range priceAverages {
		parts := strings.Split(key, "-")
		realmId, _ := strconv.Atoi(parts[0])
		auctionHouseId, _ := strconv.Atoi(parts[1])
		itemId, _ := strconv.Atoi(parts[2])

		row := []string{
			strconv.FormatInt(int64(realmId), 10),
			strconv.FormatInt(int64(auctionHouseId), 10),
			strconv.FormatInt(int64(itemId), 10),
			strconv.FormatInt(value.RowCount, 10),
			strconv.FormatInt(value.QuantitySum, 10),
			strconv.FormatInt(value.P05Sum, 10),
			strconv.FormatInt(value.P10Sum, 10),
			strconv.FormatInt(value.P25Sum, 10),
			strconv.FormatInt(value.P50Sum, 10),
			strconv.FormatInt(value.P75Sum, 10),
			strconv.FormatInt(value.P90Sum, 10),
		}

		err := writer.Write(row)
		if err != nil {
			return err
		}
	}

	if writer.Error() != nil {
		return writer.Error()
	}

	return nil
}

func removeOld(closer io.ReadCloser, priceAverages map[string]*PriceAverage) (map[string]*PriceAverage, error) {
	r := csv.NewReader(closer)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		realmId, _ := strconv.Atoi(row[1])
		auctionHouseId, _ := strconv.Atoi(row[2])
		itemId, _ := strconv.Atoi(row[3])
		quantity, _ := strconv.Atoi(row[4])
		p05, _ := strconv.Atoi(row[7])
		p10, _ := strconv.Atoi(row[8])
		p25, _ := strconv.Atoi(row[9])
		p50, _ := strconv.Atoi(row[10])
		p75, _ := strconv.Atoi(row[11])
		p90, _ := strconv.Atoi(row[12])

		key := fmt.Sprintf("%d-%d-%d", realmId, auctionHouseId, itemId)
		priceAverage, ok := priceAverages[key]
		if !ok {
			// This should not happen, but if it does, we can just ignore it
			continue
		} else {
			priceAverage.RowCount -= 1
			priceAverage.QuantitySum -= int64(quantity)
			priceAverage.P05Sum -= int64(p05)
			priceAverage.P10Sum -= int64(p10)
			priceAverage.P25Sum -= int64(p25)
			priceAverage.P50Sum -= int64(p50)
			priceAverage.P75Sum -= int64(p75)
			priceAverage.P90Sum -= int64(p90)
		}
	}
	return priceAverages, nil
}

func addNew(closer io.ReadCloser, priceAverages map[string]*PriceAverage) (map[string]*PriceAverage, error) {
	r := csv.NewReader(closer)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		realmId, _ := strconv.Atoi(row[1])
		auctionHouseId, _ := strconv.Atoi(row[2])
		itemId, _ := strconv.Atoi(row[3])
		quantity, _ := strconv.Atoi(row[4])
		p05, _ := strconv.Atoi(row[7])
		p10, _ := strconv.Atoi(row[8])
		p25, _ := strconv.Atoi(row[9])
		p50, _ := strconv.Atoi(row[10])
		p75, _ := strconv.Atoi(row[11])
		p90, _ := strconv.Atoi(row[12])

		key := fmt.Sprintf("%d-%d-%d", realmId, auctionHouseId, itemId)
		priceAverage, ok := priceAverages[key]
		if !ok {
			priceAverage = &PriceAverage{
				RowCount:    1,
				QuantitySum: int64(quantity),
				P05Sum:      int64(p05),
				P10Sum:      int64(p10),
				P25Sum:      int64(p25),
				P50Sum:      int64(p50),
				P75Sum:      int64(p75),
				P90Sum:      int64(p90),
			}
			priceAverages[key] = priceAverage
		} else {
			priceAverage.RowCount += 1
			priceAverage.QuantitySum += int64(quantity)
			priceAverage.P05Sum += int64(p05)
			priceAverage.P10Sum += int64(p10)
			priceAverage.P25Sum += int64(p25)
			priceAverage.P50Sum += int64(p50)
			priceAverage.P75Sum += int64(p75)
			priceAverage.P90Sum += int64(p90)
		}
	}
	return priceAverages, nil
}

func runAthenaQuery(svc *athena.Athena, query string, outputLocation string) (*athena.StartQueryExecutionOutput, error) {
	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String("default"),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(fmt.Sprintf("s3://sod-auctions/%s", outputLocation)),
		},
	}

	return svc.StartQueryExecution(input)
}

func waitForQueryToComplete(svc *athena.Athena, queryExecutionId string) error {
	for {
		input := &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryExecutionId),
		}
		output, err := svc.GetQueryExecution(input)
		if err != nil {
			return err
		}
		switch aws.StringValue(output.QueryExecution.Status.State) {
		case athena.QueryExecutionStateSucceeded:
			return nil
		case athena.QueryExecutionStateFailed:
			if strings.Contains(*output.QueryExecution.Status.StateChangeReason, "AlreadyExistsException") {
				log.Printf("query execution error: already exists")
				return nil
			}
			return errors.New("query execution failed")
		case athena.QueryExecutionStateCancelled:
			return errors.New("query execution cancelled")
		default:
			time.Sleep(time.Second)
		}
	}
}

func handler(ctx context.Context, snsEvent events.SNSEvent) error {
	snsRecord := snsEvent.Records[0]
	var event *events.S3Event
	err := json.Unmarshal([]byte(snsRecord.SNS.Message), &event)
	if err != nil {
		return fmt.Errorf("error decoding S3 event: %v", err)
	}

	record := event.Records[0]
	key, err := url.QueryUnescape(record.S3.Object.Key)
	if err != nil {
		return fmt.Errorf("error decoding S3 object key: %v", err)
	}

	components := strings.Split(key, "/")
	dateInfo := make(map[string]string)
	for _, component := range components {
		parts := strings.Split(component, "=")
		if len(parts) == 2 {
			dateInfo[parts[0]] = parts[1]
		}
	}

	layout := "2006-01-02 15"
	dateStr := fmt.Sprintf("%s-%s-%s %s", dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])
	date, err := time.Parse(layout, dateStr)
	if err != nil {
		return fmt.Errorf("error parsing date: %v", err)
	}

	sess := session.Must(session.NewSession())
	s3Client := s3.New(sess)

	oneHourAgo := date.Add(-1 * time.Hour)
	bucketName := fmt.Sprintf("results/percentile_averages/year=%s/month=%s/day=%s/hour=%s",
		oneHourAgo.Format("2006"), oneHourAgo.Format("01"), oneHourAgo.Format("02"), oneHourAgo.Format("15"))
	log.Printf("downloading file %s\n", bucketName)
	file, err := download(*s3Client, ctx, "sod-auctions", bucketName)
	if err != nil {
		// TODO: File probably doesn't exist. Fallback to the heavy query
		return fmt.Errorf("error downloading file: %v", err)
	}

	priceAverages, err := read(file.Body)
	if err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}

	bucketName = fmt.Sprintf("results/aggregates/interval=1/year=%s/month=%s/day=%s/hour=%s",
		dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])
	log.Printf("downloading file %s\n", bucketName)
	file, err = download(*s3Client, ctx, "sod-auctions", bucketName)
	if err != nil {
		// TODO: This shouldn't happen, but if it does then fallback to the heavy query
		return fmt.Errorf("error downloading file: %v", err)
	}

	priceAverages, err = addNew(file.Body, priceAverages)
	if err != nil {
		return fmt.Errorf("error adding new: %v", err)
	}

	overOneWeekAgo := date.Add(-169 * time.Hour)
	bucketName = fmt.Sprintf("results/aggregates/interval=1/year=%s/month=%s/day=%s/hour=%s",
		overOneWeekAgo.Format("2006"), overOneWeekAgo.Format("01"), overOneWeekAgo.Format("02"), overOneWeekAgo.Format("15"))
	log.Printf("downloading file %s\n", bucketName)
	file, err = download(*s3Client, ctx, "sod-auctions", bucketName)
	if err != nil {
		// TODO: File probably doesn't exist. Fallback to the heavy query
		return fmt.Errorf("error downloading file: %v", err)
	}

	priceAverages, err = removeOld(file.Body, priceAverages)
	if err != nil {
		return fmt.Errorf("error removing old: %v", err)
	}

	err = write(priceAverages)
	if err != nil {
		return fmt.Errorf("error writing file: %v", err)
	}

	fileKey := fmt.Sprintf("results/percentile_averages/year=%s/month=%s/day=%s/hour=%s/price_averages.csv",
		dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])
	err = upload(s3Client, "sod-auctions", fileKey, "/tmp/price_averages.csv")

	return nil
}

func main() {
	lambda.Start(handler)
}

func partitionTable() {
	now := time.Now().UTC()
	startTime := now.Add(-168 * time.Hour)
	sess := session.Must(session.NewSession())
	svc := athena.New(sess)

	for t := startTime; t.Before(now); t = t.Add(time.Hour) {
		year, month, day, hour := t.Format("2006"), t.Format("01"), t.Format("02"), t.Format("15")
		location := fmt.Sprintf("s3://sod-auctions/results/aggregates/interval=1/year=%s/month=%s/day=%s/hour=%s", year, month, day, hour)
		query := fmt.Sprintf("ALTER TABLE aggregate_results ADD PARTITION(year='%s', month='%s', day='%s', hour='%s') LOCATION '%s'", year, month, day, hour, location)

		log.Printf("running query: %s\n", query)
		result, err := runAthenaQuery(svc, query, "results/manual-queries/")
		if err != nil {
			log.Fatalf("error running query: %v", err)
		}

		err = waitForQueryToComplete(svc, *result.QueryExecutionId)
		if err != nil {
			log.Fatalf("error waiting for query to complete: %v", err)
		}
	}
}
