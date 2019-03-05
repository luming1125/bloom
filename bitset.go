package bloom

import (
	"errors"
)

//Bitset 比特集
type Bitset struct {
	Len    uint32
	Bitset []uint32
}

//newBitset 创建bitset
func newBitset(len uint32) Bitset {
	bs := &Bitset{Len: len}
	bs.Bitset = make([]uint32, len/unit)

	return *bs
}

//set 设置
func (b Bitset) set(loc uint32) (Bitset, error) {
	if loc > b.Len {
		return b, errors.New("too large")
	}

	i := loc / unit
	m := loc % unit

	b.Bitset[i] |= (1 << m)

	return b, nil
}

//check 判断是否为1
func (b Bitset) check(loc uint32) (bool, error) {
	if loc > b.Len {
		return false, errors.New("too large")
	}

	i := loc / unit
	m := loc % unit

	return b.Bitset[i]&(1<<m) != 0, nil
}
