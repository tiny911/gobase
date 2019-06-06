package mongo

import (
	"github.com/tiny911/gobase/utils"
	"gopkg.in/mgo.v2"
)

type Log struct {
}

func (l *Log) Output(calldepth int, s string) error {
	utils.DEBUG(calldepth, s)

	return nil
}

func init() {
	return
	mgo.SetDebug(true)
	mgo.SetLogger(&Log{})
}
