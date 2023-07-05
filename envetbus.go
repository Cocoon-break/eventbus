package eventbus

import "context"

type Driver interface {
	// 指定topic发送消息
	SendWithTopic(ctx context.Context, topic string, data []byte) error
	// 使用全局配置的topic
	Send(ctx context.Context, data []byte) error
	// 消费回调
	ConsumerWithCallback(cb func(content any, err error))
}
