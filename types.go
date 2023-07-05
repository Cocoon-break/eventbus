package eventbus

type Rule int

const (
	ProducerAndConsumer Rule = iota
	Consumer
	Producer
)
