package main

import (
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"log"
)

type PollIdMod struct {
	getPollIds      *gocqlx.Queryx
	modValue        int16
	modFactor       int16
	partitionPeriod int32
}

func (cur *PollIdMod) Next() []int64 {
	if cur.modValue == cur.modFactor {
		return nil
	}

	var (
		getPollIdsError error
		pollIds         []int64
	)

	getPollIdsQuery := cur.getPollIds.BindMap(qb.M{
		"partition_period": cur.partitionPeriod,
		"poll_id_mod":      cur.modValue,
	})

	getPollIdsError = getPollIdsQuery.Select(pollIds)

	if getPollIdsError != nil {
		log.Println("Error looking poll_ids by mod.")
		log.Print(getPollIdsError)
		return nil
	}
	cur.modValue += 1

	return pollIds
}
