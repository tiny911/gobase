package redis

import (
	"fmt"
	"testing"
)

func TestIncrAndGet(t *testing.T) {
	fmt.Println("==============test incrand get==============")

	cli := &Cli{}

	for i := 0; i < 50; i++ {
		val_i := cli.incrAndGet()
		equalInt(val_i, i+1, "incrAndGet failed")
	}

	cli.Close()
}
