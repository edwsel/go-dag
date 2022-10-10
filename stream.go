package dag

import (
	"errors"
	"sync"
)

const (
	MaxSizeColumn = 1000
	MaxSizeRow    = 1000
)

var (
	ErrIndexOutOfRange          = errors.New("index out of range")
	ErrOutOfRangeColumnCapacity = errors.New("out of range column capacity")
	ErrOutOfRangeRowCapacity    = errors.New("out of range row capacity")
)

type Stream interface {
	Get(name string) []any
	GetElement(name string, index int) any
	SetReplace(name string, data []any) error
	SetAppend(name string, data []any) error
	SetElement(name string, index int, data any) error
	Delete(name string) error
	DeleteElement(name string, index int) error
	Keys() []string
	ToMap() map[string][]any
}

type DefaultStream struct {
	keys   []string
	data   map[string][]any
	locker sync.RWMutex
}

func NewDefaultStream() *DefaultStream {
	return &DefaultStream{
		keys: make([]string, 0, 1000),
		data: map[string][]any{},
	}
}

func (s *DefaultStream) Get(name string) []any {
	s.locker.RLock()
	defer s.locker.RUnlock()

	if currentColumn, ok := s.data[name]; !ok {
		return nil
	} else {
		return currentColumn
	}
}

func (s *DefaultStream) GetElement(name string, index int) any {
	s.locker.RLock()
	defer s.locker.RUnlock()

	if currentColumn, ok := s.data[name]; !ok {
		return nil
	} else {
		if index > len(currentColumn)-1 {
			return ErrIndexOutOfRange
		}

		return currentColumn[index]
	}
}

func (s *DefaultStream) SetAppend(name string, data []any) error {
	s.locker.Lock()
	defer s.locker.Unlock()

	return s.set(name, data, false)
}

func (s *DefaultStream) SetReplace(name string, data []any) error {
	s.locker.Lock()
	defer s.locker.Unlock()

	return s.set(name, data, true)
}

func (s *DefaultStream) SetElement(name string, index int, data any) error {
	s.locker.Lock()
	defer s.locker.Unlock()

	if currentColumn, ok := s.data[name]; !ok {
		if index > 0 {
			return ErrIndexOutOfRange
		}

		return s.set(name, []any{data}, false)
	} else {
		if index >= len(currentColumn)-1 {
			return ErrIndexOutOfRange
		}

		currentColumn[index] = data
	}

	return nil
}

func (s *DefaultStream) Delete(name string) error {
	s.locker.Lock()
	defer s.locker.Unlock()

	delete(s.data, name)

	newKeys := make([]string, 0, MaxSizeColumn)

	for i, key := range s.keys {
		if key == name {
			continue
		}

		newKeys[i] = key
	}

	return nil
}

func (s *DefaultStream) DeleteElement(name string, index int) error {
	s.locker.Lock()
	defer s.locker.Unlock()

	if _, ok := s.data[name]; ok {
		s.data[name] = append(s.data[name][:index], s.data[name][index+1:]...)
	}

	return nil
}

func (s *DefaultStream) Flush() {
	s.locker.Lock()
	defer s.locker.Unlock()

	s.keys = make([]string, 0, MaxSizeColumn)
	s.data = map[string][]any{}
}

func (s *DefaultStream) Keys() []string {
	s.locker.RLock()
	defer s.locker.RUnlock()

	result := make([]string, len(s.keys))
	copy(result, s.keys)

	return result
}

func (s *DefaultStream) ToMap() map[string][]any {
	s.locker.RLock()
	defer s.locker.RUnlock()

	result := map[string][]any{}

	for key, srcVals := range s.data {
		dstVals := make([]any, len(srcVals))

		copy(dstVals, srcVals)

		result[key] = dstVals
	}

	return result
}

func (s *DefaultStream) set(name string, data []any, replace bool) error {
	if currentColumn, ok := s.data[name]; !ok {
		if len(s.keys) > MaxSizeColumn {
			return ErrOutOfRangeRowCapacity
		} else if len(data) > MaxSizeRow {
			return ErrOutOfRangeColumnCapacity
		}

		s.data[name] = make([]any, 0, MaxSizeRow)
		s.data[name] = append([]any{}, data...)
		s.keys = append(s.keys, name)
	} else {
		if replace {
			s.data[name] = make([]any, 0, MaxSizeRow)

			if len(data) > 1000 {
				return ErrOutOfRangeRowCapacity
			}

			s.data[name] = append([]any{}, data...)
		} else {
			if len(currentColumn)+len(data) > MaxSizeRow {
				return ErrOutOfRangeRowCapacity
			}

			s.data[name] = append(s.data[name], data...)
		}
	}

	return nil
}

func StreamMerge(stream Stream, otherStreams ...Stream) error {
	for _, otherStream := range otherStreams {
		for _, key := range otherStream.Keys() {
			data := otherStream.Get(key)

			err := stream.SetAppend(key, data)

			if err != nil {
				return err
			}
		}
	}

	return nil
}
