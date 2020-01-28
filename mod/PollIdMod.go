package mod

import (
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"log"
)

type PollIdMod struct {
	GetPollIds      *gocqlx.Queryx
	ModValue        int16
	ModFactor       int16
	PartitionPeriod int32
}

func (cur *PollIdMod) Next() []int64 {
	if cur.ModValue == cur.ModFactor {
		return nil
	}

	var (
		getPollIdsError error
		pollIds         []int64
	)

	getPollIdsQuery := cur.GetPollIds.BindMap(qb.M{
		"partition_period": cur.PartitionPeriod,
		"poll_id_mod":      cur.ModValue,
	})

	getPollIdsError = getPollIdsQuery.Select(&pollIds)

	if getPollIdsError != nil {
		log.Println("Error looking poll_ids by mod.")
		log.Print(getPollIdsError)
		return nil
	}
	cur.ModValue += 1

	return pollIds
}
