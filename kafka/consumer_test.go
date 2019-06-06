package kafka

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

var (
	testTopic  = "test"
	testGroup  = "gobaseTest2"
	testBroker = "10.69.41.51:9092,10.69.41.41:9092,10.69.41.40:9092"
)

var (
	testTopic2  = "video_broadcast_predict_response"
	testGroup2  = "monitor"
	testBroker2 = "10.69.41.31:9092,10.69.41.234:9092,10.69.41.245:909"
)

func cb(msg []byte) error {
	fmt.Printf("receive:[%s].\n", msg)
	return errors.New("err")
	return nil
}

func cb2(msg []byte) error {
	fmt.Printf("receive2:[%s].\n", msg)
	return errors.New("err")
}

func TestConsumer_Subscribe(t *testing.T) {
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 4, 444)
	consumer, err := NewConsumer(strings.Split(testBroker, ","))
	if err != nil {
		t.Errorf("consumer new err:%s", err)
	}

	go func() {
		consumer.Subscribe([]string{testTopic}, testGroup, cb2)
	}()

	err = consumer.Subscribe([]string{testTopic}, testGroup, cb)
	if err != nil {
		t.Errorf("consumer sub err:%s", err)
	}

}

func TestConsumer_Subscribe2(t *testing.T) {
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 1, 1)
	consumer, err := NewConsumer(strings.Split(testBroker2, ","))
	if err != nil {
		t.Errorf("consumer new err:%s", err)
	}
	fmt.Printf("[DEBUG] tag:%d, msg:%+v.\n", 2, 2)

	go func() {
		consumer.Subscribe([]string{testTopic2}, testGroup2, cb2)
	}()

	err = consumer.Subscribe([]string{testTopic2}, testGroup2, cb)
	if err != nil {
		t.Errorf("consumer sub err:%s", err)
	}

}
