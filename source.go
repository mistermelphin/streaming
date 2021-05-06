package streaming

type Msg interface {
	Value() interface{}
	Ack()
}

type Source interface {
	Elements() <-chan Msg
}
