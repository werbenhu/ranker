package ranker

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/huandu/skiplist"

	sk2 "github.com/MauriceGit/skiplist"

	sk3 "github.com/sean-public/fast-skiplist"
)

func BenchmarkZAdd(b *testing.B) {
	n := NewZSet()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		score := rand.Float64() * 1000
		n.ZAdd(score, strconv.Itoa(i))
	}
}

func BenchmarkMapSet(b *testing.B) {
	n := make(map[string]float64, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		score := rand.Float64() * 1000
		n[strconv.Itoa(i)] = score
	}
}

func BenchmarkSkipList(b *testing.B) {
	list := skiplist.New(skiplist.String)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		score := rand.Float64() * 1000
		list.Set(strconv.Itoa(i), score)
	}
}

type Element struct {
	key   string
	value float64
}

// Implement the interface used in skiplist
func (e Element) ExtractKey() float64 {
	return e.value
}
func (e Element) String() string {
	return e.key
}

func BenchmarkSkipList2(b *testing.B) {
	list := sk2.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		score := rand.Float64() * 1000
		list.Insert(Element{key: strconv.Itoa(i), value: score})
	}
}

func BenchmarkSkipList3(b *testing.B) {
	list := sk3.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.Set(float64(i), strconv.Itoa(i))
	}
}

// func BenchmarkZAddProf(b *testing.B) {
// 	// 创建性能分析文件
// 	f, err := os.Create("cpu.prof")
// 	if err != nil {
// 		b.Fatalf("could not create CPU profile: %v", err)
// 	}
// 	defer f.Close()

// 	// 开始 CPU 性能分析
// 	if err := pprof.StartCPUProfile(f); err != nil {
// 		b.Fatalf("could not start CPU profile: %v", err)
// 	}
// 	defer pprof.StopCPUProfile() // 在函数结束时停止分析

// 	n := NewZSet()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		score := rand.Float64() * 1000
// 		n.ZAdd(score, strconv.Itoa(i), nil)
// 	}
// }

//go test -bench=BenchmarkZAddProf -cpuprofile=cpu.prof
