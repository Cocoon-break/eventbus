package eventbus

import (
	"context"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func TestKafkaProduce(t *testing.T) {
	ops := []KafkaOption{
		WithKafkaBrokers([]string{"172.18.156.112:29092"}),
		WithKafkaTopic("test"),
		WithKafkaRule(Producer),
	}
	kb := NewKafkaBus(ops...)
	err := kb.Send(context.Background(), []byte("test4"))
	if err != nil {
		t.Error(err.Error())
		t.Fail()
	}
}

func TestKafkaConsumer(t *testing.T) {
	ops := []KafkaOption{
		WithKafkaBrokers([]string{"172.18.156.112:29092"}),
		WithKafkaTopic("test"),
		WithKafkaRule(Consumer),
	}
	kb := NewKafkaBus(ops...)
	kb.ConsumerWithCallback(func(content any, err error) {
		if err != nil {
			t.Error(err.Error())
			t.Fail()
			return
		}
		m, ok := content.(kafka.Message)
		if ok {
			t.Logf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		}
	})
	time.Sleep(5 * time.Second)
	kb.Close()
	time.Sleep(10 * time.Second)
}
