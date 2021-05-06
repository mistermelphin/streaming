package streaming

import "sync"

type msgBuffer struct {
	mu sync.Mutex

	dst      MsgProcessor
	buffer   []Msg
	capacity int
}

func Buffered(dst MsgProcessor, capacity int) MsgProcessor {
	return &msgBuffer{
		mu:       sync.Mutex{},
		dst:      dst,
		buffer:   make([]Msg, 0),
		capacity: capacity,
	}
}

func (b *msgBuffer) Process(messages []Msg) error {
	b.mu.Lock()
	b.buffer = append(b.buffer, messages...)
	size := len(b.buffer)
	b.mu.Unlock()

	if size >= b.capacity {
		return b.Flush()
	}

	return nil
}

func (b *msgBuffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.buffer) == 0 {
		return nil
	}

	err := b.dst.Process(b.buffer)
	b.buffer = make([]Msg, 0)

	if err != nil {
		return err
	}

	err = b.dst.Flush()
	if err != nil {
		return err
	}
	return nil
}
