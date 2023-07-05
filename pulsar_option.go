package eventbus

type pulsarConfig struct {
	SubscriptionName string
	Token            string
	Url              string
	Topic            string
	Rule             Rule
}

type PulsarOption func(*pulsarConfig)

func WithPulsarToken(token string) PulsarOption {
	return func(c *pulsarConfig) {
		c.Token = token
	}
}

func WithPulsarTopic(topic string) PulsarOption {
	return func(c *pulsarConfig) {
		c.Topic = topic
	}
}

func WithPulsarUrl(url string) PulsarOption {
	return func(c *pulsarConfig) {
		c.Url = url
	}
}

func WithPulsarRule(rule Rule) PulsarOption {
	return func(c *pulsarConfig) {
		c.Rule = rule
	}
}

func WithPulsarSubscriptionName(sub string) PulsarOption {
	return func(c *pulsarConfig) {
		c.SubscriptionName = sub
	}
}
