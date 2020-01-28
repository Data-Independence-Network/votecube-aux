package main

import (
	"bitbucket.org/votecube/votecube-go-lib/utils"
	"bitbucket.org/votecube/votecube-ui-aux/ingest"
	"bitbucket.org/votecube/votecube-ui-aux/mod"
	"flag"
	"fmt"
	"github.com/fasthttp/router"
	"github.com/gocql/gocql"
	"github.com/robfig/cron"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/valyala/fasthttp"
	"log"
	"strings"
	"sync"
	"time"
)

var (
	scdbHosts = flag.String("scdbHosts", "localhost", "TCP address to listen to")
	addr      = flag.String("addr", ":8446", "TCP address to listen to")
	cluster   *gocql.ClusterConfig
	session   *gocql.Session
	err       error

	// TODO: think through for how ingest jobs would catch up if they are behind more
	// than one period (probably using state tables persisted in CRDB)
	currentPeriod, previousPeriod = utils.GetCurrentAndPreviousParitionPeriods(5)

	getAddedToRootOpinionIds     *gocqlx.Queryx
	getAddedOpinionIds           *gocqlx.Queryx
	getOpinionData               *gocqlx.Queryx
	getPollData                  *gocqlx.Queryx
	getPollIds                   *gocqlx.Queryx
	getRootOpinion               *gocqlx.Queryx
	getUpdatedOpinionIds         *gocqlx.Queryx
	getUpdatedRootOpinionIds     *gocqlx.Queryx
	updateOpinion                *gocqlx.Queryx
	updateOpinionUpdate          *gocqlx.Queryx
	updateOpinionUpdatePartition *gocqlx.Queryx
	updatePoll                   *gocqlx.Queryx
	updateRootOpinion            *gocqlx.Queryx

	rootOpinionIdModFactor int16 = 2
	pollIdModFactor        int16 = 2

	//compress = flag.Bool("compress", false, "Whether to enable transparent response compression")
)

func main() {
	flag.Parse()

	// connect to the ScyllaDB cluster
	cluster = gocql.NewCluster(strings.SplitN(*scdbHosts, ",", -1)...)

	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	//cluster.Compressor = &gocql.SnappyCompressor{}
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 3}
	cluster.Consistency = gocql.One

	cluster.Keyspace = "votecube"

	session, err = cluster.CreateSession()

	if err != nil {
		// unable to connect
		panic(err)
	}
	defer session.Close()

	stmt, names := qb.Select("period_opinion_ids").
		Columns("opinion_id").
		Where(
			qb.Eq("partition_period"),
			qb.Eq("root_opinion_id"),
		).BypassCache().ToCql()
	getAddedOpinionIds = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("period_added_to_root_opinion_ids").
		Columns(
			"poll_id",
			"root_opinion_id",
		).
		Where(
			qb.Eq("partition_period"),
			qb.Eq("root_opinion_id_mod"),
		).ToCql()
	getAddedToRootOpinionIds = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("period_added_to_root_opinion_ids").
		Columns(
			"data",
		).
		Where(
			qb.Eq("opinion_id"),
		).ToCql()
	getOpinionData = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("period_poll_ids_by_batch").
		Columns("poll_id").
		Where(
			qb.Eq("partition_period"),
			qb.Eq("poll_id_mod"),
		).BypassCache().ToCql()
	getPollIds = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("polls").
		Columns("data").
		Where(
			qb.Eq("poll_id"),
		).BypassCache().ToCql()
	getPollData = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("root_opinions").
		Columns("data").
		Where(
			qb.Eq("opinion_id"),
		).BypassCache().ToCql()
	getRootOpinion = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("opinion_updates").
		Columns("opinion_id").
		Where(
			qb.Eq("partition_period"),
			qb.Eq("root_opinion_id"),
		).BypassCache().ToCql()
	getUpdatedOpinionIds = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("period_updated_root_opinion_ids").
		Columns(
			"poll_id",
			"root_opinion_id",
		).
		Where(
			qb.Eq("partition_period"),
			qb.Eq("root_opinion_id_mod"),
		).ToCql()
	getUpdatedRootOpinionIds = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("opinions").
		Set("insert_processed").
		Where(
			qb.Eq("opinion_id"),
		).ToCql()
	updateOpinion = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("opinion_updates").
		Set("update_processed").
		Where(
			qb.Eq("partition_period"),
			qb.Eq("root_opinion_id"),
			qb.Eq("opinion_id"),
		).ToCql()
	updateOpinionUpdate = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("opinion_updates").
		Set("update_processed").
		Where(
			qb.Eq("partition_period"),
			qb.Eq("root_opinion_id"),
		).ToCql()
	updateOpinionUpdatePartition = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("polls").
		Set("insert_processed").
		Where(
			qb.Eq("poll_id"),
		).ToCql()
	updatePoll = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("root_opinions").
		Set(
			"opinion_id",
			"poll_id",
			"version",
			"data",
		).Where(
		qb.Eq("poll_id"),
	).ToCql()
	updateRootOpinion = gocqlx.Query(session.Query(stmt), names)

	// runMinutes := [4]int{0, 15, 30, 45}
	//
	// runOffsetNumMinutes := 1

	c := cron.New(cron.WithLocation(time.UTC))
	//c.AddFunc("1,16,31,46 * * * *", everyPartitionPeriod)
	//c.AddFunc("1,11,21,31,41,51 * * * *", everyPartitionPeriod)
	c.AddFunc("2,7,12,17,22,27,32,37,42,47,52,57 * * * *", everyPartitionPeriod)
	c.Start()

	r := router.New()
	r.GET("/alterConfig", alterConfig)
	log.Fatal(fasthttp.ListenAndServe(*addr, r.Handler))
}

func alterConfig(ctx *fasthttp.RequestCtx) {
	testVar := ctx.UserValue("testVar")
	fmt.Fprintf(ctx, "Hello, %s!\n", testVar)
}

func everyPartitionPeriod() {
	currentPeriod, previousPeriod = utils.GetCurrentAndPreviousParitionPeriods(5)
	fmt.Printf("Current period: %d, Previous period: %d\n", currentPeriod, previousPeriod)

	rootOpinionIdMod := mod.RootOpinionIdMod{
		GetAddedToRootOpinionIds: getAddedToRootOpinionIds,
		GetUpdatedRootOpinionIds: getUpdatedRootOpinionIds,
		ModValue:                 0,
		ModFactor:                rootOpinionIdModFactor,
		PartitionPeriod:          previousPeriod,
	}
	for {
		rootOpinionIds := rootOpinionIdMod.Next()
		if rootOpinionIds == nil {
			break
		}

		for _, rootOpinionId := range rootOpinionIds {
			opinionIngest := ingest.OpinionIngest{
				GetAddedOpinionIds:           getAddedOpinionIds,
				GetOpinionData:               getOpinionData,
				GetRootOpinion:               getRootOpinion,
				GetUpdatedOpinionIds:         getUpdatedOpinionIds,
				MaxParallelQueriesPerType:    2,
				UpdateOpinion:                updateOpinion,
				UpdateOpinionUpdate:          updateOpinionUpdate,
				UpdateOpinionUpdatePartition: updateOpinionUpdatePartition,
				UpdateRootOpinion:            updateRootOpinion,
				WaitGroup:                    sync.WaitGroup{},
			}
			opinionIngest.Process(previousPeriod, rootOpinionId)
		}
	}

	pollIdMod := mod.PollIdMod{
		GetPollIds:      getPollIds,
		ModValue:        0,
		ModFactor:       pollIdModFactor,
		PartitionPeriod: previousPeriod,
	}
	for {
		pollIds := pollIdMod.Next()
		if pollIds == nil {
			break
		}

		pollIngest := ingest.PollIngest{
			GetPollData:          getPollData,
			MaxParallelProcesses: 2,
			PartitionPeriod:      previousPeriod,
			UpdatePoll:           updatePoll,
			WaitGroup:            sync.WaitGroup{},
		}

		pollIngest.Process(pollIds)
	}

}
