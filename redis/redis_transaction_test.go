package redis

import (
	"fmt"
	"testing"
)

func TestTransactionMethod(t *testing.T) {
	fmt.Println("==============test transaction method==============")
	cli := getCli()
	trans := TransactionWith(cli)
	trans.UnWatch()
	trans.Multi()
	trans.Do("SET", "key1", "val1")
	trans.Do("SET", "key2", "val2")
	trans.Do("GET", "key2")
	trans.Exec()
	val, _ := cli.Get("key1")
	equalString(val, "val1", "Exec failed!")
}
