package streaming

import (
	"errors"
	"hash/crc64"
	"sync"
)

type msgShards struct {
	keyProvider ShardKeyProvider
	shards      []MsgProcessor
	countShards uint64
}

type ShardKeyProvider interface {
	ProvideShardKey(item interface{}) ([]byte, error)
}

type ShardKeyProvideFunc func(item interface{}) ([]byte, error)

func (s ShardKeyProvideFunc) ProvideShardKey(item interface{}) ([]byte, error) {
	return s(item)
}

func Sharded(keyProvider ShardKeyProvider, processors ...MsgProcessor) MsgProcessor {
	return &msgShards{
		keyProvider: keyProvider,
		shards:      processors,
		countShards: uint64(len(processors)),
	}
}

func (m *msgShards) Process(messages []Msg) error {
	shards, err := m.splitByShard(messages)
	if err != nil {
		return err
	}

	err = m.processShards(shards)
	return err
}

func (m *msgShards) Flush() error {
	for _, shard := range m.shards {
		err := shard.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *msgShards) splitByShard(messages []Msg) (map[int][]Msg, error) {
	shards := make(map[int][]Msg)
	for _, msg := range messages {
		shardId, err := m.processMessage(msg.Value())
		if err != nil {
			return nil, err
		}

		shardMsgs, ok := shards[shardId]
		if !ok {
			shardMsgs = make([]Msg, 0)
		}
		shardMsgs = append(shardMsgs, msg)
		shards[shardId] = shardMsgs
	}
	return shards, nil
}

func (m *msgShards) processShards(shards map[int][]Msg) error {
	wg := sync.WaitGroup{}
	var errs []error
	for shardId, shardMsgs := range shards {
		wg.Add(1)
		go func(shard MsgProcessor, shardMsgs []Msg) {
			defer wg.Done()
			err := shard.Process(shardMsgs)
			if err != nil {
				errs = append(errs, err)
			}
		}(m.shards[shardId], shardMsgs)
	}

	wg.Wait()

	if errs != nil {
		errString := "sharding failure: "
		for _, err := range errs {
			errString += err.Error() + " "
		}
		return errors.New(errString)
	}

	return nil
}

func (m *msgShards) processMessage(item interface{}) (int, error) {
	key, err := m.keyProvider.ProvideShardKey(item)
	if err != nil {
		return 0, err
	}

	return int(crc64.Checksum(key, crc64.MakeTable(crc64.ISO)) % m.countShards), nil
}
