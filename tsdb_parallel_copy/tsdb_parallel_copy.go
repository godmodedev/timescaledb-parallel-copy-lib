package main

/*
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/godmodedev/timescaledb-parallel-copy-lib/internal/batch"
	"github.com/godmodedev/timescaledb-parallel-copy-lib/internal/db"
)

const (
	tabCharStr = "\\t"
)

func main() {}

func getFullTableName(schemaName *string, tableName *string) string {
	return fmt.Sprintf(`"%s"."%s"`, *schemaName, *tableName)
}

//export parallelCopy
func parallelCopy(
	fromFile *C.char,
	postgresConnect *C.char,
	dbName *C.char,
	schemaName *C.char,
	tableName *C.char,
	truncate C.int,
	copyOptions *C.char,
	splitCharacter *C.char,
	quoteCharacter *C.char,
	escapeCharacter *C.char,
	columns *C.char,
	skipHeader C.int,
	headerLinesCnt C.int,
	workers C.int,
	limit C.long,
	batchSize C.int,
	logBatches C.int,
	reportingPeriod C.int,
	verbose C.int,
	rowCount C.long,
) {
	fromFile_ := C.GoString(fromFile)
	postgresConnect_ := C.GoString(postgresConnect)
	dbName_ := C.GoString(dbName)
	schemaName_ := C.GoString(schemaName)
	tableName_ := C.GoString(tableName)
	copyOptions_ := C.GoString(copyOptions)
	splitCharacter_ := C.GoString(splitCharacter)
	quoteCharacter_ := C.GoString(quoteCharacter)
	escapeCharacter_ := C.GoString(escapeCharacter)
	columns_ := C.GoString(columns)
	rowCount_ := int64(rowCount)

	var overrides []db.Overrideable
	reportingPeriod_ := time.Duration(int(reportingPeriod)) * time.Second
	if dbName_ != "" {
		overrides = append(overrides, db.OverrideDBName(dbName_))
	}

	if len(quoteCharacter_) > 1 {
		fmt.Println("ERROR: provided --quote must be a single-byte character")
		os.Exit(1)
	}

	if len(escapeCharacter_) > 1 {
		fmt.Println("ERROR: provided --escape must be a single-byte character")
		os.Exit(1)
	}

	if truncate == 1 { // Remove existing data from the table
		dbx, err := db.Connect(postgresConnect_, overrides...)
		if err != nil {
			panic(err)
		}
		_, err = dbx.Exec(fmt.Sprintf("TRUNCATE %s", getFullTableName(&schemaName_, &tableName_)))
		if err != nil {
			panic(err)
		}

		err = dbx.Close()
		if err != nil {
			panic(err)
		}
	}

	var reader io.Reader
	file, err := os.Open(fromFile_)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader = file

	if int(headerLinesCnt) <= 0 {
		fmt.Printf("WARNING: provided --header-line-count (%d) must be greater than 0\n", int(headerLinesCnt))
		os.Exit(1)
	}

	var skip int
	if skipHeader == 1 {
		skip = int(headerLinesCnt)

		if verbose == 1 {
			fmt.Printf("Skipping the first %d lines of the input.\n", int(headerLinesCnt))
		}
	}

	var wg sync.WaitGroup
	batchChan := make(chan net.Buffers, int(workers)*2)

	// Generate COPY workers
	for i := 0; i < int(workers); i++ {
		wg.Add(1)
		go processBatches(
			&wg, batchChan, &overrides, &rowCount_, &schemaName_, &tableName_, &postgresConnect_, &copyOptions_, &splitCharacter_,
			&quoteCharacter_, &escapeCharacter_, &columns_, int(logBatches), int(batchSize),
		)
	}

	// Reporting thread
	if reportingPeriod_ > (0 * time.Second) {
		go report(reportingPeriod_, &rowCount_)
	}

	opts := batch.Options{
		Size:  int(batchSize),
		Skip:  skip,
		Limit: int64(limit),
	}

	if quoteCharacter_ != "" {
		// we already verified the length above
		opts.Quote = quoteCharacter_[0]
	}
	if escapeCharacter_ != "" {
		// we already verified the length above
		opts.Escape = escapeCharacter_[0]
	}

	start := time.Now()
	if err := batch.Scan(reader, batchChan, opts); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	close(batchChan)
	wg.Wait()
	end := time.Now()
	took := end.Sub(start)

	rowsRead := atomic.LoadInt64(&rowCount_)
	rowRate := float64(rowsRead) / float64(took.Seconds())

	res := fmt.Sprintf("COPY %d", rowsRead)
	if verbose == 1 {
		res += fmt.Sprintf(", took %v with %d worker(s) (mean rate %f/sec)", took, int(workers), rowRate)
	}
	fmt.Println(res)
}

// report periodically prints the write rate in number of rows per second
func report(reportingPeriod time.Duration, rowCount *int64) {
	start := time.Now()
	prevTime := start
	prevRowCount := int64(0)

	for now := range time.NewTicker(reportingPeriod).C {
		rCount := atomic.LoadInt64(rowCount)

		took := now.Sub(prevTime)
		rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
		overallRowrate := float64(rCount) / float64(now.Sub(start).Seconds())
		totalTook := now.Sub(start)

		fmt.Printf("at %v, row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %E total rows\n", totalTook-(totalTook%time.Second), rowrate, overallRowrate, float64(rCount))

		prevRowCount = rCount
		prevTime = now
	}

}

// processBatches reads batches from channel c and copies them to the target
// server while tracking stats on the write.
func processBatches(
	wg *sync.WaitGroup, c chan net.Buffers, overrides *[]db.Overrideable, rowCount *int64, schemaName *string,
	tableName *string, postgresConnect *string, copyOptions *string, splitCharacter *string, quoteCharacter *string,
	escapeCharacter *string, columns *string, logBatches int, batchSize int,
) {
	dbx, err := db.Connect(*postgresConnect, *overrides...)
	if err != nil {
		panic(err)
	}
	defer dbx.Close()

	delimStr := "'" + *splitCharacter + "'"
	if *splitCharacter == tabCharStr {
		delimStr = "E" + delimStr
	}

	var quotes string
	if *quoteCharacter != "" {
		quotes = fmt.Sprintf("QUOTE '%s'",
			strings.ReplaceAll(*quoteCharacter, "'", "''"))
	}
	if *escapeCharacter != "" {
		quotes = fmt.Sprintf("%s ESCAPE '%s'",
			quotes, strings.ReplaceAll(*escapeCharacter, "'", "''"))
	}

	var copyCmd string
	if *columns != "" {
		copyCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s %s", getFullTableName(schemaName, tableName), *columns, delimStr, quotes, *copyOptions)
	} else {
		copyCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s %s", getFullTableName(schemaName, tableName), delimStr, quotes, *copyOptions)
	}

	for batch := range c {
		start := time.Now()
		rows, err := db.CopyFromLines(dbx, &batch, copyCmd)
		if err != nil {
			panic(err)
		}
		atomic.AddInt64(rowCount, rows)

		if logBatches == 1 {
			took := time.Since(start)
			fmt.Printf("[BATCH] took %v, batch size %d, row rate %f/sec\n", took, batchSize, float64(batchSize)/float64(took.Seconds()))
		}
	}
	wg.Done()
}
