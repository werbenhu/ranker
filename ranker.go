package ranker

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
)

const (
	defaultStorageDir = ".rank" // Default storage directory
)

// Converts float64 to a byte slice (little-endian).
func float64ToBytes(value float64) []byte {
	bits := math.Float64bits(value)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

// Converts a byte slice to float64 (little-endian).
func bytesToFloat64(data []byte) float64 {
	bits := binary.LittleEndian.Uint64(data)
	return math.Float64frombits(bits)
}

// Unsafe conversion of string to []byte.
func unsafeStringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

// Unsafe conversion of []byte to string.
func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// Option defines configuration options for the Ranker.
type Option func(*Ranker)

// Ranker manages leaderboard operations.
type Ranker struct {
	ID         string // Ranker instance identifier
	StorageDir string // Directory for persistent storage
	zset       *ZSet
	db         *pebble.DB
}

// Entry represents a player's rank, score, and identifier.
type Entry struct {
	Rank  int     // Player's rank
	Score float64 // Player's score
	Key   string  // Player's unique identifier
}

// Configures a custom ID for the Ranker instance.
func WithID(id string) Option {
	return func(r *Ranker) {
		r.ID = id
	}
}

// Configures a custom storage directory for the Ranker.
func WithStorageDir(storageDir string) Option {
	return func(r *Ranker) {
		r.StorageDir = storageDir
	}
}

// Creates a new Ranker with the specified options.
func New(options ...Option) *Ranker {
	ranker := &Ranker{
		ID:         uuid.NewString(),
		StorageDir: defaultStorageDir,
		zset:       NewZSet(),
	}
	for _, opt := range options {
		opt(ranker)
	}
	return ranker
}

// Initializes the Ranker, including loading existing data.
func (r *Ranker) Start() error {
	var err error
	exist := r.dataExists(r.StorageDir)

	r.db, err = pebble.Open(r.StorageDir, &pebble.Options{})
	if err != nil {
		return err
	}

	if exist {
		startTime := time.Now()
		if err := r.loadData(); err != nil {
			return err
		}
		elapsedTime := time.Since(startTime)
		fmt.Printf("loaded in %v\n", elapsedTime)
	}
	return nil
}

// Releases resources associated with the Ranker.
func (r *Ranker) Close() {
	if r.db != nil {
		r.db.Close()
	}
}

// Updates or adds a player's score in the leaderboard.
func (r *Ranker) Update(playerID string, score float64) error {
	if err := r.db.Set(unsafeStringToBytes(playerID), float64ToBytes(score), pebble.NoSync); err != nil {
		return err
	}
	_, err := r.zset.ZAdd(score, playerID)
	return err
}

// Retrieves the ranking details for a specific player.
func (r *Ranker) Rank(playerID string) (*Entry, error) {
	result, err := r.zset.ZRevRankWithScore(playerID)
	if err != nil {
		return nil, err
	}
	return &Entry{Rank: int(result.Rank), Score: result.Score, Key: playerID}, nil
}

// Retrieves a range of ranking entries.
func (r *Ranker) Range(start, end int) ([]*Entry, error) {
	return nil, nil
}

// Checks if persistent data exists at the specified path.
func (r *Ranker) dataExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

// Loads leaderboard data from persistent storage into memory.
func (r *Ranker) loadData() error {
	iter, err := r.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		playerID := unsafeBytesToString(iter.Key())
		score := bytesToFloat64(iter.Value())
		if _, err := r.zset.ZAdd(score, playerID); err != nil {
			return err
		}
	}
	return iter.Error()
}
