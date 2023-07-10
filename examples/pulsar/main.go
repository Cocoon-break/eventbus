package main

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/Cocoon-break/eventbus"
)

func main() {
	start()
	select {}
}

func start() {
	sub := rand.Intn(100)
	ops := []eventbus.PulsarOption{
		eventbus.WithPulsarTopic("bsc/taibai/test-wsp"),
		eventbus.WithPulsarToken("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJob25na2FpLnNoaSJ9.mZUOipLl-Kab_wW_2NsxYZYFGUPK3nV9yQNhvh0IcOc"),
		eventbus.WithPulsarUrl("pulsar://172.18.153.56:6650"),
		eventbus.WithPulsarSubscriptionName("test" + strconv.Itoa(sub)),
		eventbus.WithPulsarRule(eventbus.Consumer),
	}
	p := eventbus.NewPulsarBus(ops...)
	err := p.ConsumerWithCallback(func(content any, err error) {
		if err != nil {
			fmt.Printf("consumer err:%+v \n", err.Error())
			return
		}
		payload, ok := content.([]byte)
		if ok {
			fmt.Printf("message payload:%s \n", string(payload))
		}
	})
	if err != nil {
		panic(err)
	}
}
