package main

func getIdBuckets(
	ids []int64,
	maxNumBuckets int,
) [][]int64 {
	numIds := len(ids)
	numBuckets := numIds
	baseBucketSize := numIds / maxNumBuckets
	bucketSizeRemainder := numBuckets % maxNumBuckets

	if numIds >= maxNumBuckets {
		numBuckets = maxNumBuckets
	}
	idBuckets := make([][]int64, numBuckets)

	idIndexOffset := 0
	for i := 0; i < numBuckets; i++ {
		bucketSize := baseBucketSize
		if (bucketSizeRemainder - 1) >= i {
			bucketSize = bucketSize + 1
		}
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

	return idBuckets
}
