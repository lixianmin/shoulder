package redisx

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/lixianmin/gloom"
	"github.com/lixianmin/got/convert"
	"github.com/lixianmin/got/timex"
)

func Benchmark_RedisBitSet(b *testing.B) {
	var estimatedKeys = 100000
	var bitSize, locationNum = gloom.EstimateParameters(estimatedKeys, 0.001)

	var bitSet = NewRedisBitSet(createRedis(), "redis.bit.set.bloom.filter")
	_ = bitSet.Expire(time.Minute)

	var bloomFilter = gloom.New(bitSize, locationNum, bitSet)
	b.Logf("estimatedKeys=%d, bitSize=%d, locationNum=%d", estimatedKeys, bitSize, locationNum)
	benchmarkBloomFilter(b, estimatedKeys, bloomFilter)
}

func Test_RedisBitSet(t *testing.T) {
	var estimatedKeys = 100000
	var bitSize, locationNum = gloom.EstimateParameters(estimatedKeys, 0.001)
	var bitSet = NewRedisBitSet(createRedis(), "redis.bit.set.bloom.filter")
	_ = bitSet.Expire(time.Minute)

	var bloomFilter = gloom.New(bitSize, locationNum, bitSet)
	fmt.Printf("estimatedKeys=%d, bitSize=%d, locationNum=%d\n", estimatedKeys, bitSize, locationNum)
	testBloomFilter(t, estimatedKeys, bloomFilter)
}

func testBloomFilter(t *testing.T, estimatedKeys int, bloomFilter *gloom.BloomFilter) {
	var startTime = time.Now()
	var falseCounter = 0
	for i := 0; i < estimatedKeys; i++ {
		var data = convert.Bytes(strconv.Itoa(i * 100))

		var existsBefore, err = bloomFilter.Exists(data)
		if err != nil {
			t.Logf("Error checking for existence in bloom filter, err=%v", err)
		}

		if existsBefore {
			falseCounter++
			t.Logf("i=%d, falseCounter=%d", i, falseCounter)
		}

		err = bloomFilter.Add(data)
		if err != nil {
			t.Logf("add err=%v", err)
		}
	}

	var costTime = time.Since(startTime)
	fmt.Printf("estimatedKeys=%d, costTime=%s\n", estimatedKeys, timex.FormatDuration(costTime))
}

func benchmarkBloomFilter(b *testing.B, estimatedKeys int, bloomFilter *gloom.BloomFilter) {
	b.ResetTimer()

	var falseCounter = 0
	for i := 0; i < estimatedKeys; i++ {
		var data = convert.Bytes(strconv.Itoa(i * 100))

		var existsBefore, err = bloomFilter.Exists(data)
		if err != nil {
			b.Logf("Error checking for existence in bloom filter, err=%v", err)
		}

		if existsBefore {
			falseCounter++
			b.Logf("i=%d, falseCounter=%d", i, falseCounter)
		}

		err = bloomFilter.Add(data)
		if err != nil {
			b.Logf("add err=%v", err)
		}
	}
}
