package mod

import (
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"log"
	"sync"
)

type RootOpinionId struct {
	HasNew     bool
	HasUpdated bool
	OpinionId  int64
	PollId     int64
}

type RootOpinionIdMod struct {
	GetAddedToRootOpinionIds *gocqlx.Queryx
	GetUpdatedRootOpinionIds *gocqlx.Queryx
	ModValue                 int16
	ModFactor                int16
	PartitionPeriod          int32
	WaitGroup                sync.WaitGroup
}

type PeriodAddedToRootOpinionId struct {
	PartitionPeriod  int32
	PollId           int64
	RootOpinionId    int64
	RootOpinionIdMod int16
}

type PeriodUpdatedRootOpinionId struct {
	PartitionPeriod  int32
	PollId           int64
	RootOpinionId    int64
	RootOpinionIdMod int16
}

func (cur *RootOpinionIdMod) Next() []RootOpinionId {
	if cur.ModValue == cur.ModFactor {
		return nil
	}

	var (
		newOpinionsQueryError     error
		rootIdsForAddedOpinions   []PeriodAddedToRootOpinionId
		rootIdsForUpdatedOpinions []PeriodUpdatedRootOpinionId
		updatedOpinionsQueryError error
	)

	cur.WaitGroup.Add(2)
	go func() {
		defer cur.WaitGroup.Done()
		boundQuery := cur.GetAddedToRootOpinionIds.BindMap(qb.M{
			"partition_period":    cur.PartitionPeriod,
			"root_opinion_id_mod": cur.ModValue,
		})

		newOpinionsQueryError = boundQuery.Select(&rootIdsForAddedOpinions)
	}()
	go func() {
		defer cur.WaitGroup.Done()
		boundQuery := cur.GetUpdatedRootOpinionIds.BindMap(qb.M{
			"partition_period":    cur.PartitionPeriod,
			"root_opinion_id_mod": cur.ModValue,
		})

		updatedOpinionsQueryError = boundQuery.Select(&rootIdsForUpdatedOpinions)
	}()
	cur.WaitGroup.Wait()

	if newOpinionsQueryError != nil {
		log.Println("Error looking up new opinion root_opinion_ids by mod.")
		log.Print(newOpinionsQueryError)
		return nil
	}
	if updatedOpinionsQueryError != nil {
		log.Println("Error looking up updated root_opinion_ids by mod.")
		log.Print(updatedOpinionsQueryError)
		return nil
	}
	cur.ModValue += 1

	idsMap := make(map[int64]RootOpinionId)
	for _, rootId := range rootIdsForAddedOpinions {
		rootOpinionId := RootOpinionId{
			HasNew:    true,
			OpinionId: rootId.RootOpinionId,
			PollId:    rootId.PollId,
		}
		idsMap[rootId.RootOpinionId] = rootOpinionId
	}
	for _, rootId := range rootIdsForUpdatedOpinions {
		newId, exists := idsMap[rootId.RootOpinionId]
		if exists {
			newId.HasUpdated = true
		} else {
			rootOpinionId := RootOpinionId{
				HasUpdated: true,
				OpinionId:  rootId.RootOpinionId,
				PollId:     rootId.PollId,
			}
			idsMap[rootId.RootOpinionId] = rootOpinionId
		}
	}

	rootOpinionIds := make([]RootOpinionId, 0, len(idsMap))
	for _, value := range idsMap {
		rootOpinionIds = append(rootOpinionIds, value)
	}

	return rootOpinionIds
}
