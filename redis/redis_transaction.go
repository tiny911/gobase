package redis

import "github.com/garyburd/redigo/redis"

type Transaction struct {
	conn redis.Conn
}

func TransactionWith(cli *Cli) *Transaction {
	return &Transaction{
		conn: cli.getWrite(), //从主的连接池取出一个
	}
}

func (this *Transaction) Close() error {
	return this.conn.Close()
}

func (this *Transaction) Exec() ([]interface{}, error) {
	rsp, err := this.conn.Do("EXEC")
	if err != nil {
		return nil, err
	}

	reply, _ := rsp.([]interface{})
	return reply, nil
}

func (this *Transaction) Multi() error {
	_, err := this.conn.Do("MULTI")
	return err
}

func (this *Transaction) UnWatch() error {
	_, err := this.conn.Do("UNWATCH")
	return err
}

func (this *Transaction) Watch(keys ...string) error {
	args := strSliToInterfSli(keys...)
	_, err := this.conn.Do("WATCH", args)
	return err
}

func (this *Transaction) Do(cmd string, args ...interface{}) error {
	cmdLog(cmd, args...)

	_, err := this.conn.Do(cmd, args...)
	return err
}
