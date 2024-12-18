/*
 * @Author: werbenhu
 * @Email: werbenhuang@hk1180.com
 * @Date: 2024-12-03 17:11:02
 * @Update: 2024-12-03 17:11:02
 */
package ranker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testKey = "zset1"

func makeZSet() *ZSet {
	n := NewZSet()

	n.ZAdd(testKey, 1, "ced", nil)
	n.ZAdd(testKey, 2, "acd", nil)
	n.ZAdd(testKey, 3, "bcd", nil)
	n.ZAdd(testKey, 4, "acc", nil)
	n.ZAdd(testKey, 5, "mcd", nil)
	n.ZAdd(testKey, 6, "ccd", nil)
	n.ZAdd(testKey, 7, "ecd", nil)

	return n
}

func TestZSet_ZAdd(t *testing.T) {
	n := makeZSet()
	assert.Equal(t, 7, n.ZCard(testKey))
}

func TestZSet_ZScore(t *testing.T) {
	n := makeZSet()
	score, err := n.ZScore(testKey, "ced")
	assert.Equal(t, 1, int(score))
	assert.NoError(t, err)
	score, err = n.ZScore(testKey, "ecd")
	assert.Equal(t, 7, int(score))
	assert.NoError(t, err)
	score, err = n.ZScore(testKey, "defrwefrw")
	assert.ErrorIs(t, ErrKeyNotExist, err)
	assert.Equal(t, 0, int(score))
}

func TestZSet_ZRank(t *testing.T) {
	n := makeZSet()
	rank, err := n.ZRank(testKey, "ced")
	assert.NoError(t, err)
	assert.Equal(t, 0, int(rank))
	rank, err = n.ZRank(testKey, "ecd")
	assert.NoError(t, err)
	assert.Equal(t, 6, int(rank))

	_, err = n.ZRank(testKey, "not exist")
	assert.ErrorIs(t, ErrKeyNotExist, err)

	_, err = n.ZRank("not exist", "ced")
	assert.ErrorIs(t, ErrKeyNotExist, err)
}

func TestZSet_ZRevRank(t *testing.T) {
	n := makeZSet()

	rank, err := n.ZRevRank(testKey, "ced")
	assert.NoError(t, err)
	assert.Equal(t, 6, int(rank))
	rank, err = n.ZRevRank(testKey, "ecd")
	assert.NoError(t, err)
	assert.Equal(t, 0, int(rank))

	_, err = n.ZRevRank(testKey, "not exist")
	assert.ErrorIs(t, ErrKeyNotExist, err)
}

func TestZSet_ZIncrBy(t *testing.T) {
	n := makeZSet()

	latest, err := n.ZIncrBy(testKey, 300, "ced")
	assert.NoError(t, err)
	assert.Equal(t, float64(301), latest)

	score, err := n.ZScore(testKey, "ced")
	assert.NoError(t, err)
	assert.Equal(t, float64(301), score)

	_, err = n.ZScore(testKey, "not exist")
	assert.ErrorIs(t, ErrKeyNotExist, err)

	_, err = n.ZScore("not exist", "ced")
	assert.ErrorIs(t, ErrKeyNotExist, err)
}

func TestZSet_ZRevRange(t *testing.T) {
	n := makeZSet()
	items, err := n.ZRevRangeWithScores(testKey, 0, 3)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(items))
	assert.Equal(t, float64(7), items[0].Score)
	assert.Equal(t, "ecd", items[0].Member)
	assert.Equal(t, float64(6), items[1].Score)
	assert.Equal(t, "ccd", items[1].Member)

	_, err = n.ZRevRangeWithScores("not exist", 0, 3)
	assert.ErrorIs(t, ErrKeyNotExist, err)
}

func TestZSet_ZScan(t *testing.T) {
	n := makeZSet()

	items, cursor, err := n.ZScan(testKey, 0, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, uint64(5), cursor)

	items, cursor, err = n.ZScan(testKey, cursor, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, uint64(3), cursor)

	items, cursor, err = n.ZScan(testKey, cursor, 4)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(items))
	assert.Equal(t, uint64(0), cursor)

	_, _, err = n.ZScan("not exist", 0, 2)
	assert.ErrorIs(t, ErrKeyNotExist, err)
}

func TestZSet_ZScanRem(t *testing.T) {
	n := makeZSet()

	items, cursor, err := n.ZScan(testKey, 0, 2)
	assert.Equal(t, 2, len(items))
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), cursor)

	for _, v := range items {
		n.ZRem(testKey, v.(string))
	}

	items, cursor, err = n.ZScan(testKey, cursor, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, uint64(3), cursor)

	for _, v := range items {
		n.ZRem(testKey, v.(string))
	}

	items, cursor, err = n.ZScan(testKey, cursor, 4)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(items))
	assert.Equal(t, uint64(0), cursor)

	for _, v := range items {
		n.ZRem(testKey, v.(string))
	}

	assert.Equal(t, 0, n.ZCard(testKey))
}
