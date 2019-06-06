package mysql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/tiny911/doraemon/log"
)

const (
	defaultConnectTimeout = 1 * time.Second
	defaultReadTimeout    = 1 * time.Second
	defaultWriteTimeout   = 1 * time.Second
	defaultLifeTimeout    = 10 * time.Second
	defaultMaxActive      = 100
	defaultMaxIdle        = 10
)

type (
	Mysql struct {
		mysql.Config

		MaxIdle        int
		MaxActive      int
		MaxLifeTimeout time.Duration
	}

	Cli struct {
		masterConn *sql.DB
		slaveConn  []*sql.DB
		posCnt     int32
	}
)

func NewMysql(ip, port, user, pwd, dbname string) *Mysql {
	return &Mysql{
		Config: mysql.Config{
			Net:          "tcp",
			Addr:         fmt.Sprintf("%s:%s", ip, port),
			User:         user,
			Passwd:       pwd,
			DBName:       dbname,
			Timeout:      defaultConnectTimeout,
			ReadTimeout:  defaultReadTimeout,
			WriteTimeout: defaultWriteTimeout,
		},

		MaxIdle:        defaultMaxIdle,
		MaxActive:      defaultMaxActive,
		MaxLifeTimeout: defaultLifeTimeout,
	}
}

func (this *Mysql) Custom(connectTimeout, readTimeout, writeTimeout, lifeTimeout time.Duration, maxActive, maxIdle int) {
	this.Config.Timeout = connectTimeout
	this.Config.ReadTimeout = readTimeout
	this.Config.WriteTimeout = writeTimeout

	this.MaxIdle = maxIdle
	this.MaxActive = maxActive
	this.MaxLifeTimeout = lifeTimeout
}

func NewCli(master *Mysql, slaves []*Mysql) *Cli {
	cli, err := NewCliCustom(
		master,
		slaves,
		defaultConnectTimeout,
		defaultReadTimeout,
		defaultWriteTimeout,
		defaultLifeTimeout,
		defaultMaxActive,
		defaultMaxIdle,
	)

	if err != nil {
		//TODO : log err
	}

	return cli
}

func (this *Cli) Close() {
	if this.masterConn != nil {
		this.masterConn.Close()
	}

	for _, pool := range this.slaveConn {
		pool.Close()
	}
}

func NewCliCustom(master *Mysql, slaves []*Mysql, connectTimeout, readTimeout, writeTimeout, lifeTimeout time.Duration, maxActive, maxIdle int) (*Cli, error) {
	var (
		err error
		cli = &Cli{slaveConn: make([]*sql.DB, len(slaves))}
	)

	master.Custom(connectTimeout, readTimeout, writeTimeout, lifeTimeout, maxActive, maxIdle)
	cli.masterConn, err = newMysqlConn(master)
	if err != nil {
		return nil, err
	}

	for index, slave := range slaves {
		slave.Custom(connectTimeout, readTimeout, writeTimeout, lifeTimeout, maxActive, maxIdle)
		pool, err := newMysqlConn(master)
		if err != nil {
			return nil, err
		}
		cli.slaveConn[index] = pool
	}

	return cli, nil
}

func newMysqlConn(mysql *Mysql) (*sql.DB, error) {
	dsn := mysql.FormatDSN()

	dbsql, err := sql.Open("mysql", dsn)
	if err != nil {
		log.WithField(log.Fields{
			"error": err,
			"dsn":   dsn,
		}).Error("mysql open failed.")
		return nil, err
	}
	dbsql.SetMaxOpenConns(mysql.MaxActive)
	dbsql.SetMaxIdleConns(mysql.MaxIdle)
	dbsql.SetConnMaxLifetime(mysql.MaxLifeTimeout)

	return dbsql, nil
}
