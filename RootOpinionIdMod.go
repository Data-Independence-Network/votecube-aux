package main

import (
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"log"
	"sync"
)

type RootOpinionId struct {
	hasNew     bool
	hasUpdated bool
	opinionId  int64
	pollId     int64
}

type RootOpinionIdMod struct {
	getAddedToRootOpinionIds *gocqlx.Queryx
	getUpdatedRootOpinionIds *gocqlx.Queryx
	modValue                 int16
	modFactor                int16
	partitionPeriod          int32
	waitGroup                sync.WaitGroup
}

type PeriodAddedToRootOpinionId struct {
	partitionPeriod  int32
	pollId           int64
	rootOpinionId    int64
	rootOpinionIdMod int16
}

type PeriodUpdatedRootOpinionId struct {
	partitionPeriod  int32
	pollId           int64
	rootOpinionId    int64
	rootOpinionIdMod int16
}

func (cur *RootOpinionIdMod) Next() []RootOpinionId {
	if cur.modValue == cur.modFactor {
		return nil
	}

	var (
		newOpinionsQueryError     error
		rootIdsForAddedOpinions   []PeriodAddedToRootOpinionId
		rootIdsForUpdatedOpinions []PeriodUpdatedRootOpinionId
		updatedOpinionsQueryError error
	)

	cur.waitGroup.Add(2)
	go func() {
		defer cur.waitGroup.Done()
		boundQuery := cur.getAddedToRootOpinionIds.BindMap(qb.M{
			"partition_period":    cur.partitionPeriod,
			"root_opinion_id_mod": cur.modValue,
		})

		newOpinionsQueryError = boundQuery.Select(rootIdsForAddedOpinions)
	}()
	go func() {
		defer cur.waitGroup.Done()
		boundQuery := cur.getUpdatedRootOpinionIds.BindMap(qb.M{
			"partition_period":    cur.partitionPeriod,
			"root_opinion_id_mod": cur.modValue,
		})

		updatedOpinionsQueryError = boundQuery.Select(rootIdsForUpdatedOpinions)
	}()
	cur.waitGroup.Wait()

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
	cur.modValue += 1

	idsMap := make(map[int64]RootOpinionId)
	for _, rootId := range rootIdsForAddedOpinions {
		rootOpinionId := RootOpinionId{
			hasNew:    true,
			opinionId: rootId.rootOpinionId,
			pollId:    rootId.pollId,
		}
		idsMap[rootId.rootOpinionId] = rootOpinionId
	}
	for _, rootId := range rootIdsForUpdatedOpinions {
		newId, exists := idsMap[rootId.rootOpinionId]
		if exists {
			newId.hasUpdated = true
		} else {
			rootOpinionId := RootOpinionId{
				hasUpdated: true,
				opinionId:  rootId.rootOpinionId,
				pollId:     rootId.pollId,
			}
			idsMap[rootId.rootOpinionId] = rootOpinionId
		}
	}

	rootOpinionIds := make([]RootOpinionId, 0, len(idsMap))
	for _, value := range idsMap {
		rootOpinionIds = append(rootOpinionIds, value)
	}

	return rootOpinionIds
}
