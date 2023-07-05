package eventbus

import (
	"context"
	"errors"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

type pulsarBus struct {
	cfg              *pulsarConfig
	client           pulsar.Client
	topicProducerMap map[string]pulsar.Producer
	stopChan         chan struct{}
	err              error
	mu               sync.RWMutex
}

func NewPulsarBus(options ...PulsarOption) Driver {
	pc := &pulsarConfig{}
	for _, option := range options {
		option(pc)
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            pc.Url,
		Authentication: pulsar.NewAuthenticationToken(pc.Token),
	})
	return &pulsarBus{
		stopChan:         make(chan struct{}),
		topicProducerMap: make(map[string]pulsar.Producer),
		cfg:              pc,
		client:           client,
		err:              err,
	}
}

// ConsumerWithCallback implements Driver.
func (p *pulsarBus) ConsumerWithCallback(cb func(content any, err error)) error {
	if p.err != nil {
		return p.err
	}
	if p.cfg.Topic == "" {
		return errors.New("empty topic")
	}
	if p.cfg.Rule == Producer {
		return errors.New("rule is producer not support receive msg")
	}
	channel := make(chan pulsar.ConsumerMessage, 100)
	options := pulsar.ConsumerOptions{
		Topic:            p.cfg.Topic,
		Type:             pulsar.Shared, // todo: make it configurable
		SubscriptionName: p.cfg.SubscriptionName,
	}
	options.MessageChannel = channel
	reader, err := p.client.Subscribe(options)
	if err != nil {
		return err
	}
	go func(reader pulsar.Consumer) {
		select {
		case cm := <-channel:
			msg := cm.Message
			if cb != nil {
				cb(msg.Payload(), err)
			}
			_ = reader.Ack(msg)
		case <-p.stopChan:
			reader.Close()
			return
		}
	}(reader)
	return nil
}

// Send implements Driver.
func (p *pulsarBus) Send(ctx context.Context, data []byte) error {
	if p.err != nil {
		return p.err
	}
	if p.cfg.Topic == "" {
		return errors.New("empty topic")
	}
	return p.SendWithTopic(ctx, p.cfg.Topic, data)
}

// SendWithTopic implements Driver.
func (p *pulsarBus) SendWithTopic(ctx context.Context, topic string, data []byte) error {
	if p.err != nil {
		return p.err
	}
	if p.cfg.Rule == Consumer {
		return errors.New("rule is consumer not support send msg")
	}
	p.mu.RLock()
	producer, ok := p.topicProducerMap[topic]
	p.mu.RUnlock()
	if !ok {
		p.mu.Lock()
		tmpProducer, err := p.client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
		})
		if err != nil {
			p.mu.Unlock()
			return err
		}
		p.topicProducerMap[topic] = tmpProducer
		producer = tmpProducer
		p.mu.Unlock()
	}
	_, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: data,
	})
	return err
}

// Close implements Driver.
func (p *pulsarBus) Close() {
	p.mu.RLock()
	defer p.mu.Unlock()
	for _, producer := range p.topicProducerMap {
		producer.Close()
	}
	p.stopChan <- struct{}{}
}
