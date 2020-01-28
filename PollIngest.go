package main

import (
	"bitbucket.org/votecube/votecube-go-lib/model/scylladb"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"log"
	"sync"
)

type PollIngest struct {
	getPollData          *gocqlx.Queryx
	maxParallelProcesses int
	partitionPeriod      int32
	updatePoll           *gocqlx.Queryx
	waitGroup            sync.WaitGroup
}

func (cur *PollIngest) Process(
	pollIds []int64,
) bool {
	pollIdBuckets := getIdBuckets(pollIds, cur.maxParallelProcesses)

	cur.waitGroup.Add(len(pollIdBuckets))
	for _, idBucket := range pollIdBuckets {
		go func() {
			defer cur.waitGroup.Done()
			for _, pollId := range idBucket {
				getPollDataQuery := cur.getPollData.BindMap(qb.M{
					"poll_id": pollId,
				})
				poll := scylladb.Poll{}
				if error := getPollDataQuery.Select(poll); error != nil {
					log.Printf("Error retrieving poll with poll_id: %d\n",
						pollId)
					log.Print(error)
					continue
				}

				// TODO: process poll

				updatePollQuery := cur.updatePoll.BindMap(qb.M{
					"poll_id": pollId,
				})
				poll.InsertProcessed = true
				updatePollQuery.BindStruct(poll)
				if err := updatePollQuery.Exec(); err != nil {
					log.Printf("Error updating poll for poll_id: %d\n", pollId)
					log.Print(err)
					continue
				}
			}
		}()
	}
	cur.waitGroup.Wait()

	return false
}
