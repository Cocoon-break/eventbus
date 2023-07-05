package eventbus

import "github.com/segmentio/kafka-go"

type kafkaConfig struct {
	Rule    Rule // 作为使用的类型,default is ProducerAndConsumer
	Brokers []string
	Topic   string
	GroupId string // 消费方的分组id，如果不设置客户端重启后offset从0开始
}

type KafkaOption func(*kafkaConfig)

func WithKafkaTopic(topic string) KafkaOption {
	return func(c *kafkaConfig) {
		c.Topic = topic
	}
}

func WithKafkaBrokers(brokers []string) KafkaOption {
	return func(c *kafkaConfig) {
		c.Brokers = brokers
	}
}

func WithKafkaRule(rule Rule) KafkaOption {
	return func(c *kafkaConfig) {
		c.Rule = rule
	}
}

func WithKafkaGroupID(groupID string) KafkaOption {
	return func(c *kafkaConfig) {
		c.GroupId = groupID
	}
}

func transformKafkaWriterConfig(c *kafkaConfig) kafka.WriterConfig {
	if c == nil {
		panic("transformKafkaWriterConfig: kafkaConfig is nil")
	}
	return kafka.WriterConfig{
		Brokers: c.Brokers,
	}
}

func transformKafkaReaderConfig(c *kafkaConfig) kafka.ReaderConfig {
	if c == nil {
		panic("transformKafkaReaderConfig: kafkaConfig is nil")
	}
	return kafka.ReaderConfig{
		Topic:   c.Topic,
		Brokers: c.Brokers,
		GroupID: c.GroupId,
	}
}
