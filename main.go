package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/fasthttp/router"
	"github.com/gocql/gocql"
	"github.com/klauspost/compress/gzip"
	"github.com/robfig/cron"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/valyala/fasthttp"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

var (
	scdbHosts                  = flag.String("scdbHosts", "localhost", "TCP address to listen to")
	addr                       = flag.String("addr", ":8446", "TCP address to listen to")
	cluster                    *gocql.ClusterConfig
	session                    *gocql.Session
	err                        error
	todayStamp, yesterdayStamp = GetDateStamps()
	getOpinionDataForThread    *gocqlx.Queryx
	getThreadData              *gocqlx.Queryx
	getPollIds                 *gocqlx.Queryx
	updateOpinions             *gocqlx.Queryx
	updateThread               *gocqlx.Queryx
	// https://blog.klauspost.com/gzip-performance-for-go-webservers/
	gzippers = sync.Pool{New: func() interface{} {
		return gzip.NewWriter(nil)
	}}
	gunzippers = sync.Pool{New: func() interface{} {
		reader, _ := gzip.NewReader(nil)

		return reader
	}}

	//compress = flag.Bool("compress", false, "Whether to enable transparent response compression")
)

type Opinion struct {
	OpinionId uint64
	PollId    uint64
	Date      string
	UserId    uint64
	//CreateDt time.Time
	CreateEs  int64
	Data      []byte
	Processed bool
}

type Poll struct {
	PollId   uint64
	UserId   uint64
	CreateEs int64
	Data     []byte
}

type PollKey struct {
	PollId   uint64
	UserId   uint64
	CreateEs int64
	BatchId  int
}

type Thread struct {
	PollId            uint64
	UserId            uint64
	CreateEs          int64
	Data              []byte
	LastProcessedDate string
}

func AlterConfig(ctx *fasthttp.RequestCtx) {
	testVar := ctx.UserValue("testVar")
	fmt.Fprintf(ctx, "Hello, %s!\n", testVar)
}

func GetDateStamps() (string, string) {
	now := time.Now()
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")

	return today, yesterday
}

func Daily() {
	todayStamp, yesterdayStamp = GetDateStamps()

	// TODO: make this code multi-threaded

	var pollKeys []PollKey

	if err := getPollIds.Select(&pollKeys); err != nil {
		log.Fatal(err)
	}

threadLoop:
	for _, pollKey := range pollKeys {
		var threads []Thread
		threadDataQuery := getThreadData.BindMap(qb.M{
			"poll_id": pollKey.PollId,
		})
		if err := threadDataQuery.Select(&threads); err != nil {
			log.Print(err)
			continue
		}

		previousCompressedData := threads[0].Data
		var buf bytes.Buffer

		// Get a Writer from the Pool
		// https://blog.klauspost.com/gzip-performance-for-go-webservers/
		gz := gzippers.Get().(*gzip.Writer)
		gz.Reset(&buf)

		if previousCompressedData != nil {
			threadDataReader := bytes.NewReader(threads[0].Data)
			gunz := gunzippers.Get().(*gzip.Reader)
			gunz.Reset(threadDataReader)

			if _, err := io.Copy(gz, gunz); err != nil {
				log.Print(err)

				gunz.Close()
				gunzippers.Put(gunz)
				gz.Close()
				gzippers.Put(gz)

				continue
			}

			gunz.Close()
			gunzippers.Put(gunz)
		}

		var opinions []Opinion
		opinionDataQuery := getOpinionDataForThread.BindMap(qb.M{
			"poll_id": pollKey.PollId,
			"date":    yesterdayStamp,
		})
		if err := opinionDataQuery.Select(&opinions); err != nil {
			log.Print(err)
			continue
		}

		prefixComma := previousCompressedData != nil
		numUnprocessedOpinions := 0
		for _, opinion := range opinions {
			if opinion.Processed {
				continue
			}

			opinionDataReader := bytes.NewReader(opinion.Data)
			gunz := gunzippers.Get().(*gzip.Reader)
			gunz.Reset(opinionDataReader)

			if prefixComma {
				gz.Write([]byte(","))
			}
			prefixComma = true

			if _, err := io.Copy(gz, gunz); err != nil {
				log.Print(err)

				gunz.Close()
				gunzippers.Put(gunz)
				gz.Close()
				gzippers.Put(gz)

				continue threadLoop
			}

			gunz.Close()
			gunzippers.Put(gunz)

			numUnprocessedOpinions++
		}

		gz.Close()
		// When done, put the Writer back in to the Pool
		gzippers.Put(gz)

		if numUnprocessedOpinions == 0 {
			continue
		}

		thread := Thread{
			PollId:            pollKey.PollId,
			Data:              buf.Bytes(),
			LastProcessedDate: yesterdayStamp,
		}

		updateThreadCommand := updateThread.BindStruct(thread)
		if err := updateThreadCommand.Exec(); err != nil {
			log.Print(err)

			continue
		}

		opinion := Opinion{
			PollId:    pollKey.PollId,
			Date:      yesterdayStamp,
			Processed: true,
		}

		updateOpinionsCommand := updateOpinions.BindStruct(opinion)
		if err := updateOpinionsCommand.Exec(); err != nil {
			log.Print(err)

			continue
		}
	}

}

func main() {
	flag.Parse()

	// connect to the ScyllaDB cluster
	cluster = gocql.NewCluster(strings.SplitN(*scdbHosts, ",", -1)...)

	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	//cluster.Compressor = &gocql.SnappyCompressor{}
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 3}
	cluster.Consistency = gocql.Any

	cluster.Keyspace = "votecube"

	session, err = cluster.CreateSession()

	if err != nil {
		// unable to connect
		panic(err)
	}
	defer session.Close()

	stmt, names := qb.Select("poll_keys").Columns("poll_id").ToCql()
	getPollIds = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("threads").Columns("data").Where(qb.Eq("poll_id")).ToCql()
	getThreadData = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("opinions").Columns("data", "processed").Where(qb.Eq("poll_id"), qb.Eq("date")).ToCql()
	getOpinionDataForThread = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("threads").Set("data", "last_processed_date").Where(qb.Eq("poll_id")).ToCql()
	updateThread = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("opinions").Set("processed").Where(qb.Eq("poll_id"), qb.Eq("date")).ToCql()
	updateOpinions = gocqlx.Query(session.Query(stmt), names)

	cron.New(
		cron.WithLocation(time.UTC)).AddFunc("0 4 * * *", Daily)

	r := router.New()
	r.GET("/alterConfig", AlterConfig)
	log.Fatal(fasthttp.ListenAndServe(*addr, r.Handler))
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	/*
		User create request
		  Verify that the userId is not yet taken in the database
			Get a new user Seq Id
			Create new user in ScyllaDB
			Create a JWT token and send it back to the client
	*/
	fmt.Fprintf(ctx, "Hello, world!\n\n")

	fmt.Fprintf(ctx, "Request method is %q\n", ctx.Method())
	fmt.Fprintf(ctx, "RequestURI is %q\n", ctx.RequestURI())
	fmt.Fprintf(ctx, "Requested path is %q\n", ctx.Path())
	fmt.Fprintf(ctx, "Host is %q\n", ctx.Host())
	fmt.Fprintf(ctx, "Query string is %q\n", ctx.QueryArgs())
	fmt.Fprintf(ctx, "User-Agent is %q\n", ctx.UserAgent())
	fmt.Fprintf(ctx, "Connection has been established at %s\n", ctx.ConnTime())
	fmt.Fprintf(ctx, "Request has been started at %s\n", ctx.Time())
	fmt.Fprintf(ctx, "Serial request number for the current connection is %d\n", ctx.ConnRequestNum())
	fmt.Fprintf(ctx, "Your ip is %q\n\n", ctx.RemoteIP())

	fmt.Fprintf(ctx, "Raw request is:\n---CUT---\n%s\n---CUT---", &ctx.Request)

	//ctx.SetContentType("text/plain; charset=utf8")

	// Set arbitrary headers
	//ctx.Response.Header.Set("X-My-Header", "my-header-value")

	// Set cookies
	//var c fasthttp.Cookie
	//c.SetKey("cookie-name")
	//c.SetValue("cookie-value")
	//ctx.Response.Header.SetCookie(&c)
}