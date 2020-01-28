package ingest

import (
	"bitbucket.org/votecube/votecube-go-lib/model/data"
	"bitbucket.org/votecube/votecube-go-lib/model/scylladb"
	"bitbucket.org/votecube/votecube-go-lib/utils"
	"bitbucket.org/votecube/votecube-ui-aux/mod"
	"encoding/json"
	"fmt"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"log"
	"sync"
)

type OpinionIngest struct {
	GetAddedOpinionIds           *gocqlx.Queryx
	GetOpinionData               *gocqlx.Queryx
	GetRootOpinion               *gocqlx.Queryx
	GetUpdatedOpinionIds         *gocqlx.Queryx
	MaxParallelQueriesPerType    int
	UpdateOpinion                *gocqlx.Queryx
	UpdateOpinionUpdate          *gocqlx.Queryx
	UpdateOpinionUpdatePartition *gocqlx.Queryx
	UpdateRootOpinion            *gocqlx.Queryx
	WaitGroup                    sync.WaitGroup
}

func (cur *OpinionIngest) Process(
	partitionPeriod int32,
	rootOpinionId mod.RootOpinionId,
) bool {
	opinionIdsToUpdate, opinionIdsToAdd, ok := cur.getOpinionIdsToAddAndUpdate(
		partitionPeriod, rootOpinionId)
	if !ok {
		return false
	}

	opinionsToAddBuckets, idBucketsToAdd := getIdAndOpinionBuckets(
		opinionIdsToAdd, cur.MaxParallelQueriesPerType)
	opinionsToAddBuckets, idBucketsToUpdate := getIdAndOpinionBuckets(
		opinionIdsToUpdate, cur.MaxParallelQueriesPerType)
	cur.WaitGroup.Add(len(idBucketsToAdd) + len(idBucketsToUpdate))
	numLoadedAdditions := runDataQueries(
		idBucketsToAdd, opinionsToAddBuckets, cur.WaitGroup, cur.GetOpinionData,
		"Error looking up added opinion data for opinion_id: %d\n",
	)
	numLoadedUpdates := runDataQueries(
		idBucketsToUpdate, opinionsToAddBuckets, cur.WaitGroup, cur.GetOpinionData,
		"Error looking up updated opinion data for opinion_id: %d\n",
	)
	cur.WaitGroup.Wait()

	addedOpinions := consolidateData(opinionsToAddBuckets, numLoadedAdditions)
	updatedOpinions := consolidateData(opinionsToAddBuckets, numLoadedUpdates)
	addedOpinionIds, updatedOpinionIds, ok := cur.doUpdateRootOpinion(
		addedOpinions, updatedOpinions, rootOpinionId.OpinionId,
		partitionPeriod, rootOpinionId.PollId)
	if !ok {
		return false
	}

	allUpdatesRecorded := len(opinionIdsToUpdate) == len(updatedOpinionIds)

	addedIdBuckets := getIdBuckets(addedOpinionIds, cur.MaxParallelQueriesPerType)
	numFlagUpdateJobs := len(addedIdBuckets) + 1

	var updatedIdBuckets [][]int64
	if !allUpdatesRecorded {
		updatedIdBuckets = getIdBuckets(updatedOpinionIds, cur.MaxParallelQueriesPerType)
		numFlagUpdateJobs = len(addedIdBuckets) + len(updatedIdBuckets)
	}

	cur.WaitGroup.Add(numFlagUpdateJobs)
	setOpinionsFlags(addedIdBuckets, cur.WaitGroup, cur.UpdateOpinion)
	if allUpdatesRecorded {
		setAllOpinionUpdateFlagsInPartition(
			partitionPeriod, rootOpinionId.OpinionId,
			cur.WaitGroup, cur.UpdateOpinionUpdatePartition,
		)
	} else {
		setAllOpinionUpdateFlags(
			partitionPeriod, rootOpinionId.OpinionId, updatedIdBuckets,
			cur.WaitGroup, cur.UpdateOpinionUpdate,
		)
	}
	cur.WaitGroup.Wait()

	return true
}

func (cur *OpinionIngest) getOpinionIdsToAddAndUpdate(
	partitionPeriod int32,
	rootOpinionId mod.RootOpinionId,
) ([]int64, []int64, bool) {

	var (
		addedOpinionIds        []int64
		addedOpinionIdsError   error
		updatedOpinionIds      []int64
		updatedOpinionIdsError error
		numTasks               int
	)
	if rootOpinionId.HasNew {
		numTasks = numTasks + 1
	}
	if rootOpinionId.HasUpdated {
		numTasks = numTasks + 1
	}
	cur.WaitGroup.Add(numTasks)
	if rootOpinionId.HasNew {
		go func() {
			defer cur.WaitGroup.Done()

			getAddedOpinionIdsQuery := cur.GetAddedOpinionIds.BindMap(qb.M{
				"partition_period": partitionPeriod,
				"root_opinion_id":  rootOpinionId,
			})
			addedOpinionIdsError = getAddedOpinionIdsQuery.Select(&addedOpinionIds)
		}()
	}
	if rootOpinionId.HasUpdated {
		go func() {
			defer cur.WaitGroup.Done()

			getAddedOpinionIdsQuery := cur.GetUpdatedOpinionIds.BindMap(qb.M{
				"partition_period": partitionPeriod,
				"root_opinion_id":  rootOpinionId,
			})
			updatedOpinionIdsError = getAddedOpinionIdsQuery.Select(&updatedOpinionIds)
		}()
	}
	cur.WaitGroup.Wait()

	if addedOpinionIdsError != nil {
		log.Println("Error looking up new opinion ids.")
		log.Print(addedOpinionIdsError)
		return nil, nil, false
	}
	if updatedOpinionIdsError != nil {
		log.Println("Error looking up updated opinion ids.")
		log.Print(updatedOpinionIdsError)
		return nil, nil, false
	}

	return addedOpinionIds, updatedOpinionIds, true
}

func getOpinionIdsToFlag(
	opinions []scylladb.Opinion,
) []int64 {
	opinionIdsToFlag := make([]int64, len(opinions))

	for i, opinion := range opinions {
		opinionIdsToFlag[i] = opinion.OpinionId
	}

	return opinionIdsToFlag
}

func getIdAndOpinionBuckets(
	ids []int64,
	maxNumBuckets int,
) ([][]*scylladb.Opinion, [][]int64) {
	numIds := len(ids)
	numBuckets := numIds
	baseBucketSize := numIds / maxNumBuckets
	bucketSizeRemainder := numBuckets % maxNumBuckets

	if numIds >= maxNumBuckets {
		numBuckets = maxNumBuckets
	}
	opinionBuckets := make([][]*scylladb.Opinion, numBuckets)
	idBuckets := make([][]int64, numBuckets)

	idIndexOffset := 0
	for i := 0; i < numBuckets; i++ {
		bucketSize := baseBucketSize
		if (bucketSizeRemainder - 1) >= i {
			bucketSize = bucketSize + 1
		}
		opinionBuckets[i] = make([]*scylladb.Opinion, bucketSize)
		idBucket := make([]int64, bucketSize)
		idBuckets[i] = idBucket

		for j := 0; j < bucketSize; j++ {
			idBucket[j] = ids[idIndexOffset+j]
		}

		idIndexOffset = idIndexOffset + baseBucketSize
		if i < bucketSizeRemainder {
			idIndexOffset = idIndexOffset + 1
		}
	}

	return opinionBuckets, idBuckets
}

func runDataQueries(
	idBuckets [][]int64,
	opinionBuckets [][]*scylladb.Opinion,
	waitGroup sync.WaitGroup,
	getOpinionData *gocqlx.Queryx,
	errorMessage string,
) int {
	numLoadedOpinions := make([]int, len(idBuckets))
	for i, idBucket := range idBuckets {
		go func() {
			defer waitGroup.Done()
			for j, opinionId := range idBucket {
				getOpinionDataQuery := getOpinionData.BindMap(qb.M{
					"opinion_id": opinionId,
				})

				opinion := scylladb.Opinion{}
				if error := getOpinionDataQuery.Select(&opinion); error != nil {
					log.Printf(errorMessage, opinionId)
					log.Print(error)
					opinionBuckets[i][j] = nil
				} else {
					opinion.OpinionId = opinionId
					opinionBuckets[i][j] = &opinion
					numLoadedOpinions[i] = numLoadedOpinions[i] + 1
				}
			}
		}()
	}
	totalLoadedOpinions := 0
	for i := 0; i < len(numLoadedOpinions); i++ {
		totalLoadedOpinions = totalLoadedOpinions + numLoadedOpinions[i]
	}

	return totalLoadedOpinions
}

func setOpinionsFlags(
	idBuckets [][]int64,
	waitGroup sync.WaitGroup,
	statement *gocqlx.Queryx,
) {
	for _, idBucket := range idBuckets {
		go func() {
			defer waitGroup.Done()
			for _, opinionId := range idBucket {
				statementUpdate := statement.BindMap(qb.M{
					"opinion_id": opinionId,
				})

				opinion := scylladb.Opinion{}
				opinion.InsertProcessed = true
				statementUpdate = statementUpdate.BindStruct(opinion)

				if err := statementUpdate.Exec(); err != nil {
					log.Printf("Error updating opinion for opinion_id: %d\n", opinionId)
					log.Print(err)
				}
			}
		}()
	}
}

func setAllOpinionUpdateFlagsInPartition(
	partitionPeriod int32,
	rootOpinionId int64,
	waitGroup sync.WaitGroup,
	statement *gocqlx.Queryx,
) {
	waitGroup.Wait()

	statementUpdate := statement.BindMap(qb.M{
		"partition_period": partitionPeriod,
		"root_opinion_id":  rootOpinionId,
	})

	opinionUpdate := scylladb.OpinionUpdate{}
	opinionUpdate.UpdateProcessed = true
	statement.BindStruct(opinionUpdate)

	if err := statementUpdate.Exec(); err != nil {
		log.Printf(
			"Error updating opinion_updates for "+
				"partition_period: %d and root_opinion_id: %d\n",
			partitionPeriod, rootOpinionId)
		log.Print(err)
	}
}

func setAllOpinionUpdateFlags(
	partitionPeriod int32,
	rootOpinionId int64,
	idBuckets [][]int64,
	waitGroup sync.WaitGroup,
	statement *gocqlx.Queryx,
) {
	for _, idBucket := range idBuckets {
		go func() {
			defer waitGroup.Done()
			for _, opinionId := range idBucket {
				statementUpdate := statement.BindMap(qb.M{
					"partition_period": partitionPeriod,
					"root_opinion_id":  rootOpinionId,
					"opinion_id":       opinionId,
				})

				opinionUpdate := scylladb.OpinionUpdate{}
				opinionUpdate.UpdateProcessed = true
				statementUpdate = statementUpdate.BindStruct(opinionUpdate)

				if err := statementUpdate.Exec(); err != nil {
					log.Printf(
						"Error updating opinion_updates for "+
							"partition_period: %d, root_opinion_id: %d and opinion_id: %d",
						partitionPeriod, rootOpinionId, opinionId)
					log.Print(err)
				}
			}
		}()
	}
}

func consolidateData(
	opinionBuckets [][]*scylladb.Opinion,
	numLoadedOpinions int,
) []scylladb.Opinion {
	opinions := make([]scylladb.Opinion, 0, numLoadedOpinions)

	for _, opinionBucket := range opinionBuckets {
		for _, opinionPointer := range opinionBucket {
			if opinionPointer != nil {
				opinions = append(opinions, *opinionPointer)
			}
		}
	}

	return opinions
}

func (cur *OpinionIngest) doUpdateRootOpinion(
	addedOpinions []scylladb.Opinion,
	updatedOpinions []scylladb.Opinion,
	rootOpinionId int64,
	partitionPeriod int32,
	pollId int64,
) ([]int64, []int64, bool) {
	getRootOpinionQuery := cur.GetRootOpinion.BindMap(qb.M{
		"opinion_id": rootOpinionId,
	})

	addedOpinionIds := make([]int64, 0, len(addedOpinions))
	updatedOpinionIds := make([]int64, 0, len(updatedOpinions))

	rootOpinion := scylladb.RootOpinion{}

	if error := getRootOpinionQuery.Select(&rootOpinion); error != nil {
		log.Printf("Error retrieving root_opinion with opinion_id: %d\n",
			rootOpinionId)
		log.Print(error)
		return addedOpinionIds, updatedOpinionIds, false
	}

	var opinions []data.Opinion

	if rootOpinion.Data != nil {
		errorMessage := fmt.Sprint("Error unzipping root_opinion data for opinion_id: ",
			rootOpinionId, "\n")
		bytes, ok := utils.Unzip(rootOpinion.Data,
			errorMessage)
		if !ok {
			return addedOpinionIds, updatedOpinionIds, false
		}

		if error := json.Unmarshal(bytes, opinions); error != nil {
			log.Printf("Unable to unmarshal root_opinion data for opinion_id%d\n",
				rootOpinionId)
			log.Print(error)
			return addedOpinionIds, updatedOpinionIds, false
		}
	}

	opinionMap := make(map[int64]data.Opinion)
	for _, opinion := range opinions {
		opinionMap[opinion.Id] = opinion
	}
	for _, addedOpinion := range addedOpinions {
		opinionData := data.Opinion{}
		if error := json.Unmarshal(addedOpinion.Data, opinionData); error != nil {
			log.Printf("Unable to unmarshal added opinion data for opinion_id%d\n",
				addedOpinion.OpinionId)
			log.Print(error)
			continue
		} else {
			addedOpinionIds = append(addedOpinionIds, addedOpinion.OpinionId)
		}
		opinionMap[addedOpinion.OpinionId] = opinionData
	}
	for _, updatedOpinion := range updatedOpinions {
		opinionData := data.Opinion{}
		if error := json.Unmarshal(updatedOpinion.Data, opinionData); error != nil {
			log.Printf("Unable to unmarshal updated opinion data for opinion_id%d\n",
				updatedOpinion.OpinionId)
			log.Print(error)
			continue
		} else {
			updatedOpinionIds = append(updatedOpinionIds, updatedOpinion.OpinionId)
		}
		opinionMap[updatedOpinion.OpinionId] = opinionData
	}

	finalOpinions := make([]data.Opinion, 0, len(opinionMap))
	for _, opinion := range opinionMap {
		finalOpinions = append(finalOpinions, opinion)
	}

	errorMessageTemplate := fmt.Sprint(
		"Unable to %s root_opinion data for opinion_id: ",
		rootOpinionId, "\n")
	compressedRootOpinionData, ok := utils.MarshalZipAux(
		finalOpinions,
		fmt.Sprintf(errorMessageTemplate, "marshal"),
		fmt.Sprintf(errorMessageTemplate, "zip"),
	)
	if !ok {
		return nil, nil, false
	}

	rootOpinion.OpinionId = rootOpinionId
	rootOpinion.PollId = pollId
	rootOpinion.Version = partitionPeriod
	rootOpinion.Data = compressedRootOpinionData.Bytes()
	updateRootOpinionQuery := cur.UpdateRootOpinion.BindMap(qb.M{
		"opinion_id": rootOpinionId,
	})
	if !utils.ExecAux(
		updateRootOpinionQuery, rootOpinion,
		fmt.Sprint("Error updating root_opinion for opinion_id: ", rootOpinionId),
	) {
		return nil, nil, false
	}

	return addedOpinionIds, updatedOpinionIds, true
}

func markOpinionsAsInserted() {

}
