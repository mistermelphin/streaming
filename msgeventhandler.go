package streaming

import "time"

type ProcessReport interface {
	Amount() int
	Duration() time.Duration
	Error() error
}

type FlushReport interface {
	Duration() time.Duration
	Error() error
}

type eventHandler struct {
	process   MsgProcessor
	chProcess chan<- ProcessReport
	chFlush   chan<- FlushReport
	chError   chan<- error
}

func HandleErrors(processor MsgProcessor, ch chan<- error) MsgProcessor {
	return HandleEvents(processor, ch, nil, nil)
}

func HandleProcess(processor MsgProcessor, ch chan<- ProcessReport) MsgProcessor {
	return HandleEvents(processor, nil, ch, nil)
}

func HandleFlush(processor MsgProcessor, ch chan<- FlushReport) MsgProcessor {
	return HandleEvents(processor, nil, nil, ch)
}

func HandleEvents(processor MsgProcessor, chErr chan<- error,
	chProcess chan<- ProcessReport, chFlush chan<- FlushReport) MsgProcessor {
	return &eventHandler{
		process:   processor,
		chProcess: chProcess,
		chFlush:   chFlush,
		chError:   chErr,
	}
}

func (e *eventHandler) Process(messages []Msg) error {
	s := time.Now()
	err := e.process.Process(messages)
	d := time.Since(s)

	if e.chProcess != nil {
		e.chProcess <- &processReport{
			amount:   len(messages),
			duration: d,
			err:      err,
		}
	}

	if err != nil && e.chError != nil {
		e.chError <- err
	}

	return err
}

func (e *eventHandler) Flush() error {
	s := time.Now()
	err := e.process.Flush()
	d := time.Since(s)

	if e.chFlush != nil {
		e.chFlush <- &flushReport{
			duration: d,
			err:      err,
		}
	}

	if err != nil && e.chError != nil {
		e.chError <- err
	}

	return err
}

type processReport struct {
	amount   int
	duration time.Duration
	err      error
}

func (p *processReport) Amount() int {
	return p.amount
}

func (p *processReport) Duration() time.Duration {
	return p.duration
}

func (p *processReport) Error() error {
	return p.err
}

type flushReport struct {
	duration time.Duration
	err      error
}

func (f *flushReport) Duration() time.Duration {
	return f.duration
}

func (f *flushReport) Error() error {
	return f.err
}
