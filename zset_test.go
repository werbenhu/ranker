package ranker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeZSet() *ZSet {
	n := NewZSet()

	n.ZAdd(1, "ced")
	n.ZAdd(2, "acd")
	n.ZAdd(3, "bcd")
	n.ZAdd(4, "acc")
	n.ZAdd(5, "mcd")
	n.ZAdd(6, "ccd")
	n.ZAdd(7, "ecd")

	return n
}

func TestZSet_ZAdd(t *testing.T) {
	n := makeZSet()
	assert.Equal(t, 7, n.ZCard())
}

func TestZSet_ZScore(t *testing.T) {
	n := makeZSet()
	score, err := n.ZScore("ced")
	assert.Equal(t, 1, int(score))
	assert.NoError(t, err)
	score, err = n.ZScore("ecd")
	assert.Equal(t, 7, int(score))
	assert.NoError(t, err)
	score, err = n.ZScore("defrwefrw")
	assert.ErrorIs(t, ErrKeyNotExist, err)
	assert.Equal(t, 0, int(score))
}

func TestZSet_ZRank(t *testing.T) {
	n := makeZSet()
	rank, err := n.ZRank("ced")
	assert.NoError(t, err)
	assert.Equal(t, 0, int(rank))
	rank, err = n.ZRank("ecd")
	assert.NoError(t, err)
	assert.Equal(t, 6, int(rank))

	_, err = n.ZRank("not exist")
	assert.ErrorIs(t, ErrKeyNotExist, err)
}

func TestZSet_ZRevRank(t *testing.T) {
	n := makeZSet()

	rank, err := n.ZRevRank("ced")
	assert.NoError(t, err)
	assert.Equal(t, 6, int(rank))
	rank, err = n.ZRevRank("ecd")
	assert.NoError(t, err)
	assert.Equal(t, 0, int(rank))

	_, err = n.ZRevRank("not exist")
	assert.ErrorIs(t, ErrKeyNotExist, err)
}

func TestZSet_ZIncrBy(t *testing.T) {
	n := makeZSet()

	latest, err := n.ZIncrBy(300, "ced")
	assert.NoError(t, err)
	assert.Equal(t, float64(301), latest)

	score, err := n.ZScore("ced")
	assert.NoError(t, err)
	assert.Equal(t, float64(301), score)

	_, err = n.ZScore("not exist")
	assert.ErrorIs(t, ErrKeyNotExist, err)
}

func TestZSet_ZRevRange(t *testing.T) {
	n := makeZSet()
	items, err := n.ZRevRangeWithScores(0, 3)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(items))
	assert.Equal(t, float64(7), items[0].Score)
	assert.Equal(t, "ecd", items[0].Member)
	assert.Equal(t, float64(6), items[1].Score)
	assert.Equal(t, "ccd", items[1].Member)
}

func TestZSet_ZScan(t *testing.T) {
	n := makeZSet()

	items, cursor, err := n.ZScan(0, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, uint64(5), cursor)

	items, cursor, err = n.ZScan(cursor, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, uint64(3), cursor)

	items, cursor, err = n.ZScan(cursor, 4)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(items))
	assert.Equal(t, uint64(0), cursor)
}

func TestZSet_ZScanRem(t *testing.T) {
	n := makeZSet()

	items, cursor, err := n.ZScan(0, 2)
	assert.Equal(t, 2, len(items))
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), cursor)

	for _, v := range items {
		n.ZRem(v.(string))
	}

	items, cursor, err = n.ZScan(cursor, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, uint64(3), cursor)

	for _, v := range items {
		n.ZRem(v.(string))
	}

	items, cursor, err = n.ZScan(cursor, 4)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(items))
	assert.Equal(t, uint64(0), cursor)

	for _, v := range items {
		n.ZRem(v.(string))
	}

	assert.Equal(t, 0, n.ZCard())
}
