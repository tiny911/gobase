package mysql

import (
	"fmt"
	"log"
	"regexp"
	"testing"
	"time"
)

var (
	masterIp, slaveIp         = "127.0.0.1", "127.0.0.1"
	masterPort, slavePort     = "3306", "3306"
	masterUser, slaveUser     = "", ""
	masterPwd, slavePwd       = "", ""
	masterDbname, slaveDbname = "test", "test"
)

func getCli() *Cli {
	master := NewMysql(masterIp, masterPort, masterUser, masterPwd, masterDbname)
	slave1 := NewMysql(slaveIp, slavePort, slaveUser, slavePwd, slaveDbname)
	slave2 := NewMysql(slaveIp, slavePort, slaveUser, slavePwd, slaveDbname)
	slave3 := NewMysql(slaveIp, slavePort, slaveUser, slavePwd, slaveDbname)
	cli := NewCli(master, []*Mysql{slave1, slave2, slave3})
	return cli
}

func creatTable() error {
	var (
		sql string = `create table t_test(
						id INT NOT NULL AUTO_INCREMENT,
						title VARCHAR(100) NOT NULL,
						PRIMARY KEY ( id )
					);`
		cli *Cli = getCli()
	)

	_, _, err := cli.Write(sql)
	cli.Close()
	return err
}

func dropTable() error {
	var (
		sql string = `drop table t_test;`
		cli *Cli   = getCli()
	)
	_, _, err := cli.Write(sql)
	cli.Close()
	return err
}

func insertRow() error {
	var (
		sql string = `insert into t_test (title) values ("hello tittle");`
		cli *Cli   = getCli()
	)
	_, _, err := cli.Write(sql)
	cli.Close()
	return err
}

func TestNewCli(t *testing.T) {
	fmt.Println("==============test new cli==============")

	var err error
	err = creatTable()
	equalError(nil, err, "write failed")

	err = dropTable()
	equalError(nil, err, "drop failed")
}

func TestConnectTimeout(t *testing.T) {
	fmt.Println("==============test connect timeout==============")

	master := NewMysql(masterIp, masterPort, masterUser, masterPwd, masterDbname)
	slave1 := NewMysql(slaveIp, slavePort, slaveUser, slavePwd, slaveDbname)
	slave2 := NewMysql(slaveIp, slavePort, slaveUser, slavePwd, slaveDbname)
	slave3 := NewMysql(slaveIp, slavePort, slaveUser, slavePwd, slaveDbname)
	cli, _ := NewCliCustom(master, []*Mysql{slave1, slave2, slave3}, 1*time.Nanosecond, 1*time.Second, 1*time.Second, 10*time.Second, 100, 10)

	sql := `select * from t_test`
	_, err := cli.Select(sql)
	equalRegexp("dial.*i/o timeout", err.Error(), "connect timeout failed")
}

func BenchmarkInsert(b *testing.B) {
	creatTable()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			insertRow()
		}
	})
	dropTable()
}

func BenchmarkInsertNoClose(b *testing.B) {
	var (
		sql string = `insert into t_test (title) values ("hello tittle with no close.");`
		cli *Cli   = getCli()
	)

	creatTable()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cli.Write(sql)
		}
	})
	cli.Close()
	dropTable()
}

func equalInt(a, b int, msg string) {
	if a != b {
		log.Panicln(msg)
	}
}

func equalError(a, b error, msg string) {
	if a != b {
		log.Panicln(msg)
	}
}

func notEqualError(a, b error, msg string) {
	if a == b {
		log.Panicln(msg)
	}
}

func equalString(a, b, msg string) {
	if a != b {
		log.Panicln(msg)
	}
}

func equalBool(a, b bool, msg string) {
	if a != b {
		log.Panicln(msg)
	}
}

func equalRegexp(pattern, str, msg string) {
	match, err := regexp.Match(pattern, []byte(str))
	if err != nil || !match {
		log.Panicln(msg)
	}
}
