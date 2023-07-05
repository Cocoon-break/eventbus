package main

import (
	"eventbus"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	start()
	select {}
}

func start() {
	ops := []eventbus.KafkaOption{
		eventbus.WithKafkaBrokers([]string{"172.18.156.112:29092"}),
		eventbus.WithKafkaTopic("test"),
		eventbus.WithKafkaUsedFor(eventbus.Consumer),
	}
	kb := eventbus.NewKafkaBus(ops...)
	kb.ConsumerWithCallback(func(content any, err error) {
		if err != nil {
			fmt.Printf("consumer err:%+v \n", err.Error())
			return
		}
		m, ok := content.(kafka.Message)
		if ok {
			fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		}
	})
}
