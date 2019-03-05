package bloom

import (
	"math"

	"github.com/spaolacci/murmur3"
)

const errorRate = 0.0005

const unit = 32

//Filter bloomFilter
type Filter struct {
	N  uint32 //num
	K  uint   //hash count
	C  uint32 //capacity
	Bs Bitset
}

//New 创建bloomFilter
func New(n uint32, k uint) *Filter {
	f := &Filter{N: n, K: k}
	f = f.capacity()
	f.Bs = newBitset(f.C)

	return f
}

//Check 判断是否存在
func (f *Filter) Check(data string) bool {
	locs := f.locations(data)

	for _, loc := range locs {
		res, _ := f.Bs.check(loc)
		if !res {
			return false
		}
	}

	return true
}

//Add 添加数据
func (f *Filter) Add(data string) *Filter {
	locs := f.locations(data)

	for _, loc := range locs {
		f.Bs, _ = f.Bs.set(loc)
	}

	return f
}

func (f *Filter) locations(data string) []uint32 {
	locs := make([]uint32, f.K)
	for i := uint(0); i < f.K; i++ {
		locs[i] = hash(data, i) % f.C
	}

	return locs
}

func (f *Filter) capacity() *Filter {
	r := 8 / (math.Log(1 / (1 - math.Pow(errorRate, float64(1)/float64(f.K)))))
	f.C = uint32(math.Ceil(float64(f.N)*r/float64(unit))) * unit

	return f
}

func hash(data string, seed uint) uint32 {
	return murmur3.Sum32WithSeed([]byte(data), uint32(seed))
}
