package ranker

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/werbenhu/skiplist"
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
	list := skiplist.New[string, int]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.Set(strconv.Itoa(i), i)
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
