package kafka

import (
	"errors"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/tiny911/doraemon/log"
)

var (
	defaultDialTimeout  = 3 * time.Second
	defaultReadTimeout  = 3 * time.Second
	defaultWriteTimeout = 3 * time.Second
)

var errBrokerAddrs = errors.New("broker address empty")

// Handler 从kafka拉取消息后，进行回调处理的handler
type Handler func(msg []byte) error

// Consumer 消费者
type Consumer struct {
	config    *cluster.Config
	addrs     []string
	quits     []chan interface{}
	consumers []*cluster.Consumer
}

// NewConsumer 根据broker的addrs地址，生成Consumer实例
func NewConsumer(addrs []string) (*Consumer, error) {
	if len(addrs) == 0 {
		return nil, errBrokerAddrs
	}

	config := cluster.NewConfig()
	config.Net.DialTimeout = defaultDialTimeout
	config.Net.ReadTimeout = defaultReadTimeout
	config.Net.WriteTimeout = defaultWriteTimeout
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	return &Consumer{
		addrs:     addrs,
		config:    config,
		consumers: make([]*cluster.Consumer, 0),
		quits:     make([]chan interface{}, 0),
	}, nil
}

// Subscribe 在group上订阅topics，如果有消息则条用callback做处理
func (c *Consumer) Subscribe(topics []string, group string, callback Handler) error {
	consumer, err := cluster.NewConsumer(c.addrs, group, topics, c.config)
	if err != nil {
		return err
	}
	c.consumers = append(c.consumers, consumer)

	quit := make(chan interface{})
	c.quits = append(c.quits, quit)

	for {
		select {
		case msg, more := <-consumer.Messages():
			if more {
				log.WithField(log.Fields{
					"msg":  msg,
					"more": more,
				}).Debug("kafka consumer msg.")

				//TODO:这里有一个问题:如果callback没有处理成功，则不做MarkOffset操作，此条消息在本进程生命期不会再次被消费,但在重启后会被消费。这个问题已提交issue，待解决。
				if callback(msg.Value) == nil { //处理成功则提交offset
					consumer.MarkOffset(msg, "")
				}
			}
		case err, more := <-consumer.Errors():
			if more {
				log.WithField(log.Fields{
					"error": err,
				}).Error("kafka consumer error.")
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				log.WithField(log.Fields{
					"ntf": ntf,
				}).Info("kafka consumer notification.")
			}
		case <-quit:
			return nil
		}
	}

	return nil
}

// Close 关闭生产者
func (c *Consumer) Close() error {
	for _, quit := range c.quits {
		close(quit)
	}

	for _, consumer := range c.consumers {
		err := consumer.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
