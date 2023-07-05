package eventbus

import (
	"context"
	"errors"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type kafkaBus struct {
	writer   *kafka.Writer
	reader   *kafka.Reader
	cfg      *kafkaConfig
	stopChan chan struct{}
}

func NewKafkaBus(options ...KafkaOption) Driver {
	kc := &kafkaConfig{}
	for _, option := range options {
		option(kc)
	}

	kb := &kafkaBus{
		stopChan: make(chan struct{}),
		cfg:      kc,
	}
	// 角色不是消费者就创建生产者
	if kc.Rule != Consumer {
		kwc := transformKafkaWriterConfig(kc)
		w := kafka.NewWriter(kwc)
		w.AllowAutoTopicCreation = true
		kb.writer = w
	}
	// 角色不是生产者就创建消费者
	if kc.Rule != Producer {
		kwr := transformKafkaReaderConfig(kc)
		r := kafka.NewReader(kwr)
		kb.reader = r
	}
	return kb
}

func (k *kafkaBus) SendWithTopic(ctx context.Context, topic string, data []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Value: data,
	}
	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		err = k.writer.WriteMessages(ctx, msg)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}
		break
	}
	return err
}

func (k *kafkaBus) Send(ctx context.Context, data []byte) error {
	if k.cfg.Topic == "" {
		return errors.New("empty topic")
	}
	return k.SendWithTopic(ctx, k.cfg.Topic, data)
}

func (k *kafkaBus) ConsumerWithCallback(cb func(content any, err error)) error {
	go func() {
		for {
			select { // todo: 不能解决协程泄露问题，因为读取数据函数是阻塞的
			case <-k.stopChan:
				return
			default:
			}
			msg, err := k.reader.ReadMessage(context.Background())
			if err != nil {
				continue
			}
			cb(msg, err)
		}
	}()
	return nil
}

func (k *kafkaBus) Close() {
	if k.writer != nil {
		k.writer.Close()
	}
	if k.reader != nil {
		k.stopChan <- struct{}{}
		k.reader.Close()
	}
}
