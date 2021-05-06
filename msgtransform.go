package streaming

type Transformer interface {
	Transform(original interface{}) (interface{}, error)
}

type TransformFunc func(original interface{}) (interface{}, error)

func (tf TransformFunc) Transform(original interface{}) (interface{}, error) {
	return tf(original)
}

type msgTransform struct {
	processor   MsgProcessor
	transformer Transformer
}

func Transformed(processor MsgProcessor, transformer Transformer) MsgProcessor {
	return &msgTransform{
		processor:   processor,
		transformer: transformer,
	}
}

func (m *msgTransform) Process(messages []Msg) error {
	result := make([]Msg, len(messages))
	for i, msg := range messages {
		newValue, err := m.transformer.Transform(msg.Value())
		if err != nil {
			return err
		}
		result[i] = &transformedMsg{
			original: msg,
			value:    newValue,
		}
	}
	return m.processor.Process(result)
}

func (m *msgTransform) Flush() error {
	return m.processor.Flush()
}

type transformedMsg struct {
	original Msg
	value    interface{}
}

func (t *transformedMsg) Value() interface{} {
	return t.value
}

func (t *transformedMsg) Ack() {
	t.original.Ack()
}
