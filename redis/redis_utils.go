package redis

import (
	"sync/atomic"

	"github.com/gomodule/redigo/redis"
	"github.com/tiny911/doraemon/log"
)

func cmdLog(cmd string, args ...interface{}) {
	log.WithField(log.Fields{
		"cmd":  cmd,
		"args": args,
	}).Debug("redis cmd log.")
}

func (this *Cli) doRead(cmd string, args ...interface{}) (reply interface{}, err error) {
	conn := this.getRead()
	defer conn.Close()

	return this.do(conn, cmd, args...)
}

func (this *Cli) doWrite(cmd string, args ...interface{}) (reply interface{}, err error) {
	conn := this.getWrite()
	defer conn.Close()

	return this.do(conn, cmd, args...)
}

func (this *Cli) do(conn redis.Conn, cmd string, args ...interface{}) (reply interface{}, err error) {
	cmdLog(cmd, args...)

	reply, err = conn.Do(cmd, args...)
	if err != nil {
		log.WithField(log.Fields{
			"error": err,
			"cmd":   cmd,
			"args":  args,
		}).Error("redis cmd failed.")
	}

	return reply, err
}

func (this *Cli) incrAndGet() int { // RR choose one pool
	count := atomic.AddInt32(&this.posCnt, 1)
	if count >= 0 && count < 2100000000 {
		return int(count)
	} else {
		atomic.StoreInt32(&this.posCnt, 0)
		return 0
	}
}

func (this *Cli) getRead() redis.Conn {
	length := len(this.slavePool)
	if length == 0 { //没有从库，则取主库
		return this.getWrite()
	}
	count := this.incrAndGet()
	return this.slavePool[count%length].Get()
}

func (this *Cli) getWrite() redis.Conn {
	return this.masterPool.Get()
}

func (this *Cli) doInt(read bool, methodname string, masknil bool, args ...interface{}) (int, error) {
	var (
		reply interface{}
		err   error
	)

	if read {
		reply, err = this.doRead(methodname, args...)
	} else {
		reply, err = this.doWrite(methodname, args...)
	}

	value, err := redis.Int(reply, err)
	if masknil && err == redis.ErrNil {
		return -1, nil
	}

	return value, err
}

func (this *Cli) doBool(read bool, methodname string, masknil bool, args ...interface{}) (bool, error) {
	var (
		reply interface{}
		err   error
	)

	if read {
		reply, err = this.doRead(methodname, args...)
	} else {
		reply, err = this.doWrite(methodname, args...)
	}

	value, err := redis.Bool(reply, err)
	if masknil && err == redis.ErrNil {
		return false, nil
	}

	return value, err
}

func (this *Cli) doString(read bool, methodname string, masknil bool, args ...interface{}) (string, error) {
	var (
		reply interface{}
		err   error
	)

	if read {
		reply, err = this.doRead(methodname, args...)
	} else {
		reply, err = this.doWrite(methodname, args...)
	}

	value, err := redis.String(reply, err)
	if masknil && err == redis.ErrNil {
		return "", nil
	}

	return value, err
}

func (this *Cli) doStringSlice(read bool, methodname string, masknil bool, args ...interface{}) ([]string, error) {
	var reply interface{}
	var err error
	if read {
		reply, err = this.doRead(methodname, args...)
	} else {
		reply, err = this.doWrite(methodname, args...)
	}

	value, err := redis.Strings(reply, err)
	if masknil && err == redis.ErrNil {
		return []string{}, nil
	}

	return value, err
}

func strSliToInterfSli(keys ...string) []interface{} {
	args := make([]interface{}, len(keys))
	for i, key := range keys {
		args[i] = key
	}

	return args
}

func strSliToInterfSliTwo(key string, members ...string) []interface{} {
	args := make([]interface{}, len(members)+1)
	args[0] = key
	for i, member := range members {
		args[i+1] = member
	}

	return args
}
