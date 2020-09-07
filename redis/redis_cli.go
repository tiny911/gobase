package redis

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tiny911/doraemon/log"
)

const (
	defaultConnectTimeout = 1 * time.Second
	defaultReadTimeout    = 1 * time.Second
	defaultWriteTimeout   = 1 * time.Second
	defaultIdleTimeout    = 10 * time.Second
	defaultMaxActive      = 100
	defaultMaxIdle        = 10
	defualtWait           = true
)

type (
	Redis struct {
		ip   string
		port string
		pwd  string
		dbno string
	}

	Cli struct {
		masterPool *redis.Pool
		slavePool  []*redis.Pool
		posCnt     int32
	}
)

func NewRedis(ip, port, pwd, dbno string) *Redis {
	return &Redis{
		ip:   ip,
		port: port,
		pwd:  pwd,
		dbno: dbno,
	}
}

func (this *Redis) Addr() string {
	return fmt.Sprintf("%s:%s", this.ip, this.port)
}

func (this *Redis) Pwd() string {
	return this.pwd
}

func (this *Redis) Dbno() string {
	return this.dbno
}

func NewCli(master *Redis, slaves []*Redis) *Cli {
	return NewCliCustom(
		master,
		slaves,
		defaultConnectTimeout,
		defaultReadTimeout,
		defaultWriteTimeout,
		defaultIdleTimeout,
		defaultMaxActive,
		defaultMaxIdle,
		defualtWait,
	)
}

func (this *Cli) Close() {
	if this.masterPool != nil {
		this.masterPool.Close()
	}

	for _, pool := range this.slavePool {
		pool.Close()
	}
}

func NewCliCustom(master *Redis, slaves []*Redis, connectTimeout, readTimeout, writeTimeout, idleTimeout time.Duration, maxActive, maxIdle int, waitConn bool) *Cli {
	cli := &Cli{slavePool: make([]*redis.Pool, len(slaves))}
	cli.masterPool = newRedisPool(
		master.Addr(),
		master.Pwd(),
		master.Dbno(),
		connectTimeout,
		readTimeout,
		writeTimeout,
		idleTimeout,
		maxActive,
		maxIdle,
		waitConn,
	)

	for index, slave := range slaves {
		cli.slavePool[index] = newRedisPool(
			slave.Addr(),
			slave.Pwd(),
			slave.Dbno(),
			connectTimeout,
			readTimeout,
			writeTimeout,
			idleTimeout,
			maxActive,
			maxIdle,
			waitConn,
		)
	}

	return cli
}

func newRedisPool(addr string, pwd string, dbno string, connectTimeout, readTimeout, writeTimeout, idleTimeout time.Duration, maxActive, maxIdle int, waitConn bool) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: idleTimeout,
		Wait:        waitConn,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", addr, connectTimeout, readTimeout, writeTimeout)
			if err != nil {
				log.WithField(log.Fields{
					"error": err,
					"addr":  addr,
				}).Error("redis dial failed!")
				return nil, err
			}

			if pwd != "" {
				if _, err := c.Do("AUTH", pwd); err != nil {
					log.WithField(log.Fields{
						"error": err,
						"pwd":   pwd,
					}).Error("redis auth failed!")

					c.Close()
					return nil, err
				}
			}

			if dbno != "" {
				_, err = c.Do("SELECT", dbno)
				if err != nil {
					log.WithField(log.Fields{
						"error": err,
						"dbno":  dbno,
					}).Error("redis select failed!")

					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < idleTimeout {
				return nil
			}
			_, err := c.Do("PING")
			if err != nil {
				log.WithField(log.Fields{
					"error": err,
				}).Error("redis ping failed!")
				return err
			}
			return err
		},
	}
}
