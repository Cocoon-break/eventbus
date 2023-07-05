package eventbus

import (
	"context"
	"testing"
	"time"
)

func TestPulsarProduce(t *testing.T) {
	ops := []PulsarOption{
		WithPulsarTopic("bsc/taibai/test-wsp"),
		WithPulsarToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJob25na2FpLnNoaSJ9.mZUOipLl-Kab_wW_2NsxYZYFGUPK3nV9yQNhvh0IcOc"),
		WithPulsarUrl("pulsar://172.18.153.56:6650"),
		WithPulsarRule(Producer),
	}
	p := NewPulsarBus(ops...)
	err := p.Send(context.Background(), []byte("test-pulsar"))
	if err != nil {
		t.Error(err.Error())
		t.Fail()
	}
}

func TestPulsarConsumer(t *testing.T) {
	ops := []PulsarOption{
		WithPulsarTopic("bsc/taibai/test-wsp"),
		WithPulsarToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJob25na2FpLnNoaSJ9.mZUOipLl-Kab_wW_2NsxYZYFGUPK3nV9yQNhvh0IcOc"),
		WithPulsarUrl("pulsar://172.18.153.56:6650"),
		WithPulsarSubscriptionName("test"),
		WithPulsarRule(Consumer),
	}
	p := NewPulsarBus(ops...)
	err := p.ConsumerWithCallback(func(content any, err error) {
		if err != nil {
			t.Error(err.Error())
			t.Fail()
			return
		}
		payload, ok := content.([]byte)
		if ok {
			t.Logf("message payload:%s", string(payload))
		}

	})
	if err != nil {
		t.Error(err.Error())
		t.Fail()
		return
	}
	time.Sleep(15 * time.Second)
}
