package kafka

import (
	"os"

	"github.com/Shopify/sarama"
)

// Producer 生产者，封装sarama异步生产
type Producer struct {
	producer sarama.AsyncProducer
}

// NewProducer 根据broker的addrs地址，生成Producer实例
func NewProducer(addrs []string) (*Producer, error) {
	var (
		config = sarama.NewConfig()
		err    error
	)

	if len(addrs) == 0 {
		return nil, errBrokerAddrs
	}

	//TODO:对config做配置优化
	//这里只是设置了ClientID
	if os.Getenv("ENV_SERVER_NAME") != "" {
		config.ClientID = os.Getenv("ENV_SERVER_NAME")
	}

	producer, err := sarama.NewAsyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer: producer,
	}, nil
}

// SendMsg 向topic通道发送msg消息
func (p *Producer) SendMsg(topic string, msg string) error {
	select {
	case p.producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   nil,
		Value: sarama.StringEncoder(msg),
	}:
	case err := <-p.producer.Errors():
		return err
	}

	return nil
}

// Close 关闭生产者
func (p *Producer) Close() error {
	return p.producer.Close()
}
