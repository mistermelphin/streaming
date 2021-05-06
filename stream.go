package streaming

import (
	"context"
)

type Stream interface {
	Process(ctx context.Context)
}

func New(src Source, dst MsgProcessor) Stream {
	return newStream(src, dst)
}

func newStream(src Source, dst MsgProcessor) *stream {
	return &stream{
		src: src,
		dst: dst,
	}
}

type stream struct {
	src Source
	dst MsgProcessor
}

func (s *stream) Process(ctx context.Context) {
	for {
		select {
		case element := <-s.src.Elements():
			_ = s.dst.Process([]Msg{element})
		case <-ctx.Done():
			return
		}
	}
}
