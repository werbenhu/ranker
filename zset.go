package ranker

import (
	"errors"
	"math"
	"math/rand"
	"sync"
)

const (
	SKIPLIST_MAXLEVEL    = 32   // 最大跳表层数，适用于最多 2^32 个元素
	SKIPLIST_Probability = 0.25 // 跳表每一层的概率为 1/4
)

var (
	ErrKeyNotExist   = errors.New("key not exist")
	ErrInvalidParams = errors.New("invalid params")
)

type (
	// ZSet 代表一个有序集合（Sorted Set）。
	ZSet struct {
		sync.Mutex
		records map[string]*zset // 用于存储成员的字典
	}

	// zskiplistLevel 代表跳表的每一层，包含了前进指针和跨度信息
	zskiplistLevel struct {
		forward *zskiplistNode // 当前层的前进指针
		span    uint64         // 当前层的跨度（跨越的节点数）
	}

	// zskiplistNode 代表跳表中的节点，包含成员、值、分数和指向下一个节点的指针
	// level 是跳表节点的每一层的指针数组
	zskiplistNode struct {
		member   string            // 成员（key）
		value    interface{}       // 对应的值
		score    float64           // 成员的分数
		backward *zskiplistNode    // 指向前一个节点的指针
		level    []*zskiplistLevel // 跳表层数的指针数组
	}

	// zskiplist 代表跳表结构，包含头节点、尾节点、长度和当前层数
	zskiplist struct {
		head   *zskiplistNode // 跳表的头节点
		tail   *zskiplistNode // 跳表的尾节点
		length int64          // 跳表的节点数
		level  int            // 跳表的层数
	}

	// zset 代表有序集合内部的结构，包含一个字典和跳表
	zset struct {
		dict map[string]*zskiplistNode // 字典，用于存储成员与节点的映射
		zsl  *zskiplist                // 跳表
	}

	// Z 表示一个有序集合的成员，包括分数和成员本身
	Z struct {
		Score  float64     // 分数
		Member interface{} // 成员，目前只支持string类型
	}

	// RankScore 用于表示成员的排名和分数
	RankScore struct {
		Rank  int64   // 排名
		Score float64 // 分数
	}
)

// randomLevel 返回一个随机的跳表层数，层数范围在 1 到 SKIPLIST_MAXLEVEL 之间。
// 返回值遵循幂次法分布（powerlaw distribution），即层数越高的节点越不常见。
func randomLevel() int {
	level := 1
	// 随机数决定是否提升层数，概率为 1/4
	for float64(rand.Int31()&0xFFFF) < float64(SKIPLIST_Probability*0xFFFF) {
		level += 1
	}
	// 如果层数超过最大层数，返回最大层数
	if level < SKIPLIST_MAXLEVEL {
		return level
	}

	return SKIPLIST_MAXLEVEL
}

// createNode 创建一个新的跳表节点，给定层数、分数、成员和对应的值
func createNode(level int, score float64, member string, value interface{}) *zskiplistNode {
	node := &zskiplistNode{
		score:  score,
		member: member,
		value:  value,
		level:  make([]*zskiplistLevel, level), // 初始化节点的层数
	}

	// 为每一层初始化 zskiplistLevel
	for i := range node.level {
		node.level[i] = new(zskiplistLevel)
	}

	return node
}

// newZSkipList 创建一个新的跳表，初始化头节点和层数
func newZSkipList() *zskiplist {
	return &zskiplist{
		level: 1,                                         // 初始化为1层
		head:  createNode(SKIPLIST_MAXLEVEL, 0, "", nil), // 创建头节点
	}
}

// insert 将一个新节点插入跳表中，假设插入的元素在跳表中不存在
func (z *zskiplist) insert(score float64, member string, value interface{}) *zskiplistNode {
	// 用于存储插入位置的节点
	updates := make([]*zskiplistNode, SKIPLIST_MAXLEVEL)
	// 用于存储每一层的排名
	rank := make([]uint64, SKIPLIST_MAXLEVEL)

	// 从头节点开始遍历
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		// 存储每一层经过的节点数
		if i == z.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		// 找到插入位置
		if x.level[i] != nil {
			for x.level[i].forward != nil &&
				(x.level[i].forward.score < score ||
					(x.level[i].forward.score == score && x.level[i].forward.member < member)) {

				rank[i] += x.level[i].span // 更新跨度
				x = x.level[i].forward     // 前进到下一个节点
			}
		}
		updates[i] = x
	}

	// 根据随机层数决定新节点的层数
	level := randomLevel()
	if level > z.level { // 如果层数增加了，需要更新头节点的相关信息
		for i := z.level; i < level; i++ {
			rank[i] = 0
			updates[i] = z.head
			updates[i].level[i].span = uint64(z.length)
		}
		z.level = level
	}

	// 创建新的节点
	x = createNode(level, score, member, value)
	for i := 0; i < level; i++ {
		x.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = x

		// 更新跨度信息
		x.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		updates[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 更新剩余层次的跨度
	for i := level; i < z.level; i++ {
		updates[i].level[i].span++
	}

	// 更新前向指针
	if updates[0] == z.head {
		x.backward = nil
	} else {
		x.backward = updates[0]
	}

	// 更新尾节点
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		z.tail = x
	}

	z.length++ // 增加跳表的长度
	return x
}

// deleteNode 删除跳表中的节点
func (z *zskiplist) deleteNode(x *zskiplistNode, updates []*zskiplistNode) {
	for i := 0; i < z.level; i++ {
		// 更新前进指针和跨度信息
		if updates[i].level[i].forward == x {
			updates[i].level[i].span += x.level[i].span - 1
			updates[i].level[i].forward = x.level[i].forward
		} else {
			updates[i].level[i].span--
		}
	}

	// 更新后向指针
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		z.tail = x.backward
	}

	// 如果跳表的最上层没有节点了，减少层数
	for z.level > 1 && z.head.level[z.level-1].forward == nil {
		z.level--
	}

	z.length-- // 跳表节点数减少
}

// delete 删除指定分数和成员的节点
func (z *zskiplist) delete(score float64, member string) {
	// 存储节点的指针
	update := make([]*zskiplistNode, SKIPLIST_MAXLEVEL)

	// 从头节点开始遍历
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	// 找到匹配的节点并删除
	x = x.level[0].forward
	if x != nil && score == x.score && x.member == member {
		z.deleteNode(x, update)
		return
	}
}

// Find the rank of the node specified by key
// 注意：rank 是一个 0-based 的整数，Rank 0 表示第一个节点
func (z *zskiplist) getRank(score float64, member string) int64 {
	var rank uint64 = 0
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		// 查找指定元素所在的位置，累加跨越的跨度
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					x.level[i].forward.member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		// 找到匹配的节点，返回当前的 rank
		if x.member == member {
			return int64(rank)
		}
	}
	return 0
}

// 根据排名获取节点
func (z *zskiplist) getNodeByRank(rank uint64) *zskiplistNode {
	var traversed uint64 = 0

	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		// 遍历每一层，找到对应排名的节点
		for x.level[i].forward != nil &&
			(traversed+x.level[i].span) <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}

	return nil
}

// 根据排名获取节点，并返回节点的 member 和 score
func (z *zset) getNodeByRank(rank int64, reverse bool) (string, float64) {
	// 检查排名范围是否合法
	if rank < 0 || rank > z.zsl.length {
		return "", math.MinInt64
	}

	// 如果是反向查询，调整排名
	if reverse {
		rank = z.zsl.length - rank
	} else {
		rank++ // 正向查询排名要加 1
	}

	// 获取指定排名的节点
	n := z.zsl.getNodeByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}

	// 根据节点的 member 查找字典
	node := z.dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}

	// 返回 member 和 score
	return node.member, node.score
}

// 根据 score 范围查找并返回节点
func (z *zset) findRangeWithScore(start, stop int64, reverse bool) (val []Z) {
	length := z.zsl.length

	// 处理负数的排名，从后往前查找
	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}

	// 处理 stop 范围
	if stop < 0 {
		stop += length
	}

	// 边界检查
	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}
	span := (stop - start) + 1

	var node *zskiplistNode
	// 反向查找时从尾部开始
	if reverse {
		node = z.zsl.tail
		if start > 0 {
			node = z.zsl.getNodeByRank(uint64(length - start))
		}
	} else {
		// 正向查找时从头部开始
		node = z.zsl.head.level[0].forward
		if start > 0 {
			node = z.zsl.getNodeByRank(uint64(start + 1))
		}
	}

	// 遍历指定范围的节点
	for span > 0 {
		span--
		val = append(val, Z{
			Member: node.member,
			Score:  node.score,
		})
		// 根据反向或正向遍历节点
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}

	return
}

// 根据排名范围查找并返回成员
func (z *zset) findRange(start, stop int64, reverse bool) (val []any) {
	length := z.zsl.length

	// 处理负数排名，支持从后往前查找
	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}

	// 处理 stop 范围
	if stop < 0 {
		stop += length
	}

	// 边界检查
	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}
	span := (stop - start) + 1

	var node *zskiplistNode
	// 反向查找时从尾部开始
	if reverse {
		node = z.zsl.tail
		if start > 0 {
			node = z.zsl.getNodeByRank(uint64(length - start))
		}
	} else {
		// 正向查找时从头部开始
		node = z.zsl.head.level[0].forward
		if start > 0 {
			node = z.zsl.getNodeByRank(uint64(start + 1))
		}
	}

	// 遍历指定范围的节点
	for span > 0 {
		span--
		val = append(val, node.member)

		// 根据反向或正向遍历节点
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}

	return
}

// 创建一个新的 ZSet 对象
func NewZSet() *ZSet {
	return &ZSet{
		records: make(map[string]*zset),
	}
}

// 检查 key 是否存在
func (z *ZSet) exist(key string) bool {
	_, exist := z.records[key]
	return exist
}

func (z *ZSet) zadd(key string, score float64, member string, value interface{}) (val int, err error) {

	item := z.records[key]
	v, exist := item.dict[member]

	var node *zskiplistNode
	if exist {
		val = 0
		// 如果 score 改变，删除并重新插入
		if score != v.score {
			item.zsl.delete(v.score, member)
			node = item.zsl.insert(score, member, value)
		} else {
			// 如果 score 没有变化，直接更新 value
			v.value = value
		}
	} else {
		val = 1
		// 如果元素不存在，直接插入
		node = item.zsl.insert(score, member, value)
	}

	// 更新字典中的节点
	if node != nil {
		item.dict[member] = node
	}
	return
}

// ZAdd 将指定的成员和分数添加到指定的有序集合中
// 该方法的时间复杂度是 O(log(N))
func (z *ZSet) ZAdd(key string, score float64, member string, value interface{}) (val int, err error) {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {

		// 如果不存在该 key，则创建新的 zset
		node := &zset{
			dict: make(map[string]*zskiplistNode),
			zsl:  newZSkipList(),
		}
		z.records[key] = node
	}

	return z.zadd(key, score, member, value)
}

// ZScore 返回指定成员在指定有序集合中的分数。
func (z *ZSet) ZScore(key string, member string) (score float64, err error) {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return score, ErrKeyNotExist
	}

	node, exist := z.records[key].dict[member]
	if !exist {
		return score, ErrKeyNotExist
	}

	return node.score, nil
}

// ZCard 返回指定 key 的有序集合元素数量
func (z *ZSet) ZCard(key string) int {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return 0
	}
	return len(z.records[key].dict)
}

// ZRank 返回指定成员在有序集合中的排名，按分数从低到高排序
func (z *ZSet) ZRank(key, member string) (int64, error) {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return -1, ErrKeyNotExist
	}
	n := z.records[key]
	v, exist := n.dict[member]
	if !exist {
		return -1, ErrKeyNotExist
	}
	rank := n.zsl.getRank(v.score, member)
	rank--
	return rank, nil
}

// ZRevRank 返回指定成员在有序集合中的排名，按分数从高到低排序
func (z *ZSet) ZRevRank(key, member string) (int64, error) {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return -1, ErrKeyNotExist
	}
	n := z.records[key]
	v, exist := n.dict[member]
	if !exist {
		return -1, ErrKeyNotExist
	}
	rank := n.zsl.getRank(v.score, member)
	return n.zsl.length - rank, nil
}

// ZRevRankWithScore 返回指定成员的排名及其分数，按分数从高到低排序
func (z *ZSet) ZRevRankWithScore(key, member string) (rs RankScore, err error) {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		rs.Rank = -1
		return rs, ErrKeyNotExist
	}
	n := z.records[key]
	v, exist := n.dict[member]
	if !exist {
		rs.Rank = -1
		err = ErrKeyNotExist
		return
	}
	rank := n.zsl.getRank(v.score, member)
	rs.Rank = n.zsl.length - rank
	rs.Score = v.score
	return
}

// ZIncrBy 增加指定成员的分数，如果成员不存在，则将其分数设置为 increment
func (z *ZSet) ZIncrBy(key string, increment float64, member string) (float64, error) {

	z.Lock()
	defer z.Unlock()

	keyExists := z.exist(key)

	if keyExists {
		node, memberExists := z.records[key].dict[member]

		if memberExists {
			increment += node.score
			z.zadd(key, increment, member, node.value)
		}
	}

	if !keyExists {

		// 如果不存在该 key，则创建新的 zset
		zs := &zset{
			dict: make(map[string]*zskiplistNode),
			zsl:  newZSkipList(),
		}

		z.records[key] = zs
		z.zadd(key, increment, member, nil)
	}
	return increment, nil
}

// ZRem 从有序集合中移除指定成员，成员不存在则忽略
func (z *ZSet) ZRem(key, member string) error {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return ErrKeyNotExist
	}
	n := z.records[key]
	v, exist := n.dict[member]
	if exist {
		n.zsl.delete(v.score, member)
		delete(n.dict, member)
		return nil
	}
	return ErrKeyNotExist
}

// ZScoreRange 返回有序集合中分数在 min 和 max 之间的元素（包括 min 和 max 的元素），按分数从低到高排序
func (z *ZSet) ZScoreRange(key string, min, max float64) (val []interface{}, err error) {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}

	if max < min {
		err = ErrInvalidParams
		return
	}

	item := z.records[key].zsl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}
	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}
	x := item.head
	for i := item.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.score < min {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	for x != nil {
		if x.score > max {
			break
		}
		val = append(val, x.member, x.score)
		x = x.level[0].forward
	}
	return
}

// ZRevScoreRange 返回有序集合中分数在 max 和 min 之间的元素（包括 max 和 min 的元素），按分数从高到低排序
func (z *ZSet) ZRevScoreRange(key string, max, min float64) (val []Z, err error) {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}

	if max < min {
		err = ErrInvalidParams
		return
	}

	item := z.records[key].zsl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}
	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}
	x := item.head
	for i := item.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.score <= max {
			x = x.level[i].forward
		}
	}
	for x != nil {
		if x.score < min {
			break
		}
		val = append(val, Z{Member: x.member, Score: x.score})
		x = x.backward
	}
	return
}

// ZKeyExists 检查指定的 key 是否存在
func (z *ZSet) ZKeyExists(key string) bool {
	z.Lock()
	defer z.Unlock()

	return z.exist(key)
}

// ZClear 清除指定 key 对应的 zset
func (z *ZSet) ZClear(key string) {
	z.Lock()
	defer z.Unlock()

	if z.ZKeyExists(key) {
		delete(z.records, key)
	}
}

// ZRange 获取指定范围内的 zset 元素
func (z *ZSet) ZRange(key string, start, stop int) ([]interface{}, error) {
	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}
	n := z.records[key]
	return n.findRange(int64(start), int64(stop), false), nil
}

// ZRangeWithScores 获取指定范围内的 zset 元素及分数
func (z *ZSet) ZRangeWithScores(key string, start, stop int) ([]Z, error) {
	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}

	n := z.records[key]
	return n.findRangeWithScore(int64(start), int64(stop), false), nil
}

// ZRevRange 获取按分数降序排列的指定范围内的 zset 元素
func (z *ZSet) ZRevRange(key string, start, stop int) ([]interface{}, error) {
	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}
	n := z.records[key]
	return n.findRange(int64(start), int64(stop), true), nil
}

// ZRevRangeWithScores 获取按分数降序排列的指定范围内的 zset 元素及分数
func (z *ZSet) ZRevRangeWithScores(key string, start, stop int) ([]Z, error) {
	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}
	n := z.records[key]
	return n.findRangeWithScore(int64(start), int64(stop), true), nil
}

// ZGetByRank 根据排名获取 zset 元素，排名从低到高
func (z *ZSet) ZGetByRank(key string, rank int) (val []interface{}, err error) {
	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}

	n := z.records[key]
	member, score := n.getNodeByRank(int64(rank), false)
	val = append(val, member, score)
	return
}

// ZRevGetByRank 根据排名获取 zset 元素，排名从高到低
func (z *ZSet) ZRevGetByRank(key string, rank int) (val []interface{}, err error) {
	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}

	n := z.records[key]
	member, score := n.getNodeByRank(int64(rank), true)
	val = append(val, member, score)
	return
}

// ZPopMin 获取并删除分数最小的元素，若 zset 为空返回 nil
func (z *ZSet) ZPopMin(key string) (rec *zskiplistNode, err error) {
	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}

	n := z.records[key]
	x := n.zsl.head.level[0].forward
	if x != nil {
		z.ZRem(key, x.member)
	}

	return x, nil
}

// ZPopMax 获取并删除分数最大的元素，若 zset 为空返回 nil
func (z *ZSet) ZPopMax(key string) (rec *zskiplistNode, err error) {
	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return nil, ErrKeyNotExist
	}

	n := z.records[key]
	x := n.zsl.tail
	if x != nil {
		z.ZRem(key, x.member)
	}

	return x, nil
}

// ZRangeOptions 用于指定 zset 范围查询的选项。
type ZRangeOptions struct {
	Limit        int  // 限制返回的最大节点数
	ExcludeStart bool // 是否排除起始值，决定查询区间是 (start, end] 还是 (start, end)
	ExcludeEnd   bool // 是否排除结束值，决定查询区间是 [start, end) 还是 (start, end)
}

// ZRangeByScore 根据分数范围获取 zset 元素。
func (z *ZSet) ZRangeByScore(key string, start, end float64, options *ZRangeOptions) (nodes []*zskiplistNode) {

	z.Lock()
	defer z.Unlock()

	if !z.exist(key) {
		return
	}

	n := z.records[key]
	zsl := n.zsl

	// 设置默认参数
	var limit int = int((^uint(0)) >> 1)
	if options != nil && options.Limit > 0 {
		limit = options.Limit
	}

	// 设置是否排除起始和结束值
	excludeStart := options != nil && options.ExcludeStart
	excludeEnd := options != nil && options.ExcludeEnd
	reverse := start > end
	if reverse {
		start, end = end, start
		excludeStart, excludeEnd = excludeEnd, excludeStart
	}

	// 若 zsl 为空，返回空列表
	if zsl.length == 0 {
		return nodes
	}

	if reverse { // 从后往前查找
		x := zsl.head

		if excludeEnd {
			for i := zsl.level - 1; i >= 0; i-- {
				for x.level[i].forward != nil && x.level[i].forward.score < end {
					x = x.level[i].forward
				}
			}
		} else {
			for i := zsl.level - 1; i >= 0; i-- {
				for x.level[i].forward != nil && x.level[i].forward.score <= end {
					x = x.level[i].forward
				}
			}
		}

		for x != nil && limit > 0 {
			if excludeStart {
				if x.score <= start {
					break
				}
			} else {
				if x.score < start {
					break
				}
			}

			next := x.backward
			nodes = append(nodes, x)
			limit--
			x = next
		}
	} else { // 从前往后查找
		x := zsl.head
		if excludeStart {
			for i := zsl.level - 1; i >= 0; i-- {
				for x.level[i].forward != nil && x.level[i].forward.score <= start {
					x = x.level[i].forward
				}
			}
		} else {
			for i := zsl.level - 1; i >= 0; i-- {
				for x.level[i].forward != nil && x.level[i].forward.score < start {
					x = x.level[i].forward
				}
			}
		}

		// 当前节点是分数小于或等于 start 的最后一个节点
		x = x.level[0].forward

		for x != nil && limit > 0 {
			if excludeEnd {
				if x.score >= end {
					break
				}
			} else {
				if x.score > end {
					break
				}
			}

			next := x.level[0].forward
			nodes = append(nodes, x)
			limit--
			x = next
		}
	}

	return nodes
}

// Keys 返回所有的 zset key
func (z *ZSet) Keys() []string {

	z.Lock()
	defer z.Unlock()

	keys := make([]string, 0, len(z.records))
	for k := range z.records {
		keys = append(keys, k)
	}
	return keys
}

// ZScan 实现了类似于 Redis 中的 ZSCAN 命令
func (z *ZSet) ZScan(key string, cursor uint64, count int64) ([]any, uint64, error) {

	z.Lock()
	if !z.exist(key) {
		z.Unlock()
		return nil, 0, ErrKeyNotExist
	}
	z.Unlock()

	end := int(cursor)
	if end == 0 {
		end = z.ZCard(key)
	}

	start := end - int(count)

	if start < 0 {
		start = 0
	}

	// 获取完整的有序集合
	items, err := z.ZRange(key, start, end-1)

	if err != nil {
		return nil, 0, err
	}

	// 如果集合为空
	if len(items) == 0 {
		return items, 0, nil
	}

	return items, uint64(start), nil
}
