package main

import (
	"bitbucket.org/votecube/votecube-go-lib/model/scylladb"
	"bitbucket.org/votecube/votecube-go-lib/utils"
	"flag"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"log"
	"strings"
)

var (
	scdbHosts                     = flag.String("scdbHosts", "localhost", "TCP address to listen to")
	addr                          = flag.String("addr", ":8446", "TCP address to listen to")
	cluster                       *gocql.ClusterConfig
	session                       *gocql.Session
	err                           error
	currentPeriod, previousPeriod = utils.GetPartitionPeriods()
	getOpinionDataForThread       *gocqlx.Queryx
	getThreadData                 *gocqlx.Queryx
	getPollIds                    *gocqlx.Queryx
	updateOpinions                *gocqlx.Queryx
	updateThread                  *gocqlx.Queryx

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

	stmt, names := qb.Select("poll_keys").Columns("poll_id").BypassCache().ToCql()
	getPollIds = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("threads").Columns("data").Where(qb.Eq("poll_id")).BypassCache().ToCql()
	getThreadData = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Select("opinions").Columns("data", "insert_processed").Where(qb.Eq("poll_id"), qb.Eq("create_period")).BypassCache().ToCql()
	getOpinionDataForThread = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("root_opinions").Set("data", "version").Where(qb.Eq("poll_id")).ToCql()
	updateThread = gocqlx.Query(session.Query(stmt), names)

	stmt, names = qb.Update("opinions").Set("insert_processed").Where(qb.Eq("poll_id"), qb.Eq("create_period")).ToCql()
	updateOpinions = gocqlx.Query(session.Query(stmt), names)

	runJob()
}

func runJob() {
	currentPeriod, previousPeriod = utils.GetPartitionPeriods()

	var pollKeys []scylladb.PollKey

	if err := getPollIds.Select(&pollKeys); err != nil {
		log.Fatal(err)
	}

	// TODO: make this code multi-threaded once we move off singe-core systems
	for _, pollKey := range pollKeys {
		processThread(pollKey)
	}
}

func processThread(
	pollKey scylladb.PollKey,
) bool {
	var threads []scylladb.Thread
	threadDataQuery := getThreadData.BindMap(qb.M{
		"poll_id": pollKey.PollId,
	})
	if err := threadDataQuery.Select(&threads); err != nil {
		log.Print(err)
		return false
	}

	previousCompressedData := threads[0].Data

	buf, gz, ok := utils.UnzipToNewZipper(previousCompressedData)
	if !ok {
		return false
	}
	defer utils.CloseZipper(gz)

	var opinions []scylladb.Opinion
	opinionDataQuery := getOpinionDataForThread.BindMap(qb.M{
		"poll_id": pollKey.PollId,
		"date":    previousPeriod,
	})
	if err := opinionDataQuery.Select(&opinions); err != nil {
		log.Print(err)
		return false
	}

	prefixComma := previousCompressedData != nil
	numUnprocessedOpinions := 0
	for _, opinion := range opinions {
		if opinion.InsertProcessed {
			continue
		}

		if prefixComma {
			gz.Write([]byte(","))
		}
		prefixComma = true

		if !utils.UnzipToZipper(opinion.Data, gz) {
			return false
		}

		numUnprocessedOpinions++
	}

	if numUnprocessedOpinions == 0 {
		return true
	}

	thread := scylladb.Thread{
		PollId: pollKey.PollId,
		Data:   buf.Bytes(),
		//LastProcessedDate: previousPeriod,
	}

	updateThreadCommand := updateThread.BindStruct(thread)
	if err := updateThreadCommand.Exec(); err != nil {
		log.Print(err)

		return false
	}

	opinion := scylladb.Opinion{
		PollId:          pollKey.PollId,
		CreatePeriod:    previousPeriod,
		InsertProcessed: true,
	}

	updateOpinionsCommand := updateOpinions.BindStruct(opinion)
	if err := updateOpinionsCommand.Exec(); err != nil {
		log.Print(err)

		return false
	}

	return true
}
