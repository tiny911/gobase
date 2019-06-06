package kafka

import (
	"fmt"
	"strings"
	"testing"
)

func TestProducer_SendMsg(t *testing.T) {
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 1, 2222)
	producer, err := NewProducer(strings.Split(testBroker, ","))
	if err != nil {
		t.Errorf("producer new err:%s", err)
	}
	for i := 0; i < 100; i++ {
		err = producer.SendMsg(testTopic, "abc...hello world222233333!!a21111111")
	}
	if err != nil {
		t.Errorf("producer send err:%s", err)
	}
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 2, 222)
	producer.Close()
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 3, 333)
}

func TestProducer_SendMsg2(t *testing.T) {
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 11, 11)
	producer, err := NewProducer(strings.Split(testBroker2, ","))
	if err != nil {
		t.Errorf("producer new err:%s", err)
	}
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 22, 22)
	err = producer.SendMsg(testTopic2, "hello world by monitor+3!!")
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 33, 33)
	if err != nil {
		t.Errorf("producer send err:%s", err)
	}
	producer.Close()
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 44, 44)
}
