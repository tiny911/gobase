package utils

import (
	"fmt"
	"runtime"
	"time"
)

var debugFlag = "[DEBUG]"

func DEBUG(i ...interface{}) {
	now := time.Now().Format("2006-01-02 15:04:05")
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("\n%s %s [BEGIN] @%s:%d \n", now, debugFlag, file, line)
	fmt.Printf("%s %s [DATA]:%+v ", now, debugFlag, i)
	fmt.Printf("\n%s %s [END]   @%s:%d \n", now, debugFlag, file, line)
}
