package sequence

import (
	"fmt"
	"sort"
	"sync"
)

var (
	ErrNotEnoughData = fmt.Errorf("Not enough data")
)

type Sequence struct {
	lock   sync.RWMutex
	data   []float64
	sorted bool
}

func NewSequence() *Sequence {
	s := new(Sequence)
	s.data = make([]float64, 10000)
	return s
}

func (s *Sequence) Append(v float64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data = append(s.data, v)
	s.sorted = false
}

func (s *Sequence) GetAverage() float64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.data) == 0 {
		return 0
	}

	sum := float64(0)
	for _, v := range s.data {
		sum += v
	}

	return sum / float64(len(s.data))
}

func (s *Sequence) sort() {
	if !s.sorted {
		sort.Slice(s.data, func(i, j int) bool { return s.data[i] < s.data[j] })
		s.sorted = true
	}
}

func (s *Sequence) Get50P() float64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.data) < 100 {
		return 0
	}

	s.sort()
	return s.data[(50*len(s.data))/100]
}

func (s *Sequence) Get99P() float64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.data) < 990 {
		return 0
	}

	s.sort()
	return s.data[(99*len(s.data))/100]
}

func (s *Sequence) Get95P() float64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.data) < 950 {
		return 0
	}

	s.sort()
	return s.data[(95*len(s.data))/100]
}

func (s *Sequence) Count() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.data)
}
