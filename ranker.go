package ranker

import (
	"encoding/binary"
	"math"
	"os"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
)

const (
	defaultDir     = ".rank"
	defaultZSetKey = "rank"
)

// FloatToBytes converts a float64 to a byte slice using math.Float64bits.
func floatToBytes(f float64) []byte {
	bits := math.Float64bits(f)                // Convert to uint64
	bytes := make([]byte, 8)                   // Allocate a 8-byte slice
	binary.LittleEndian.PutUint64(bytes, bits) // Encode bits into bytes
	return bytes
}

// BytesToFloat converts a byte slice to a float64 using math.Float64frombits.
func bytesToFloat(b []byte) float64 {
	bits := binary.LittleEndian.Uint64(b) // Decode bytes into uint64
	return math.Float64frombits(bits)     // Convert uint64 back to float64
}

// 不安全的string转[]byte方法
func stringToBytes(s string) []byte {
	// 直接使用unsafe.Pointer进行类型转换
	return *(*[]byte)(unsafe.Pointer(&s))
}

// 不安全的[]byte转string方法
func bytesToString(b []byte) string {
	// 直接使用unsafe.Pointer进行类型转换
	return *(*string)(unsafe.Pointer(&b))
}

// Option represents a configuration option for initializing a Ranker.
type Option func(*Ranker)

// Ranker represents a ranking manager that handles ranking operations for players.
type Ranker struct {
	ID         string // Unique identifier for the ranker instance
	StorageDir string // Directory for storing persistent data

	zset *ZSet
	db   *pebble.DB
}

// Entry represents a single ranking entry, including the rank, score, and player information.
type Entry struct {
	Rank  int     // Player's rank in the leaderboard
	Score float64 // Player's score
	Key   string  // Player's unique identifier or name
}

// WithID sets the ID of the Ranker.
func WithID(id string) Option {
	return func(r *Ranker) {
		r.ID = id
	}
}

// WithStorageDir sets the storage directory for the Ranker.
func WithStorageDir(storageDir string) Option {
	return func(r *Ranker) {
		r.StorageDir = storageDir
	}
}

// New creates a new Ranker instance with the given options.
func New(opts ...Option) *Ranker {
	// Default Ranker instance

	ranker := &Ranker{
		ID:         uuid.NewString(), // Generate a default UUID if ID is not provided
		StorageDir: defaultDir,
		zset:       NewZSet(),
	}

	// Apply options
	for _, opt := range opts {
		opt(ranker)
	}

	return ranker
}

// Start initializes any necessary resources or processes for the Ranker.
func (r *Ranker) Start() error {
	// Initialize resources or processes.

	var err error
	exist := r.isDataExist(r.StorageDir)

	r.db, err = pebble.Open(r.StorageDir, &pebble.Options{})
	if err != nil {
		return err
	}

	if exist {
		if err := r.loadData(); err != nil {
			return err
		}
	}

	return nil
}

// Close releases any resources or stops any processes associated with the Ranker.
func (r *Ranker) Close() {
	// Release resources or stop processes.
	if r.db != nil {
		r.db.Close()
	}
}

// Update updates the score for a specific player. If the player does not exist, they are added to the ranking system.
func (r *Ranker) Update(key string, score float64) error {
	// Update or add the player's score.

	err := r.db.Set(stringToBytes(key), floatToBytes(score), pebble.NoSync)
	if err != nil {
		return err
	}

	_, err = r.zset.zadd(defaultZSetKey, score, key, nil)
	return err
}

// Rank retrieves the ranking entry for a specific player.
// Returns the Entry object for the player or an error if the player does not exist.
func (r *Ranker) Rank(key string) (*Entry, error) {
	ret, err := r.zset.ZRevRankWithScore(defaultZSetKey, key)
	if err != nil {
		return nil, err
	}

	return &Entry{
		Rank:  int(ret.Rank),
		Score: ret.Score,
		Key:   key,
	}, nil
}

// Range retrieves a slice of ranking entries within the specified range [start, end].
// Returns a list of Entries or an error if the range is invalid or there are no entries.
func (r *Ranker) Range(start int, end int) ([]*Entry, error) {
	// Retrieve ranking entries within the specified range.
	return nil, nil
}

// 检查数据库是否存在
func (r *Ranker) isDataExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

// 从数据库加载数据到内存
func (r *Ranker) loadData() error {

	// Iterate 遍历所有键值对
	iter, err := r.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {

		// 复制键值对到内存
		key := bytesToString(iter.Key())
		value := bytesToFloat(iter.Value())

		_, err = r.zset.zadd(defaultZSetKey, value, key, nil)
		if err != nil {
			return err
		}
	}

	// 检查迭代器是否有错误
	if err := iter.Error(); err != nil {
		return err
	}
	return nil
}
