package streaming

import (
	"context"
	"time"
)

type MsgProcessor interface {
	Process(messages []Msg) error
	Flush() error
}

type msgAutoFlusher struct {
	processor MsgProcessor
}

func AutoFlushed(processor MsgProcessor, ctx context.Context, interval time.Duration) MsgProcessor {
	result := &msgAutoFlusher{
		processor: processor,
	}

	go result.worker(ctx, interval)

	return result
}

func (m *msgAutoFlusher) Process(messages []Msg) error {
	return m.processor.Process(messages)
}

func (m *msgAutoFlusher) Flush() error {
	return m.processor.Flush()
}

func (m *msgAutoFlusher) worker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			_ = m.Flush()
		case <-ctx.Done():
			return
		}
	}
}
