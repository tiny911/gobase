package mysql

import (
	"testing"
	"time"
)

var (
	mycli *Cli = nil
)

func TestTransaction(t *testing.T) {
	creatTable()
	mycli = getCli()
	go func() {
		tx_insert_3time()
	}()
	tx_insert_1time()
	dropTable()
	time.Sleep(3 * time.Second)
}

func tx_insert_3time() {
	sql := `insert into t_test (title) values ("hello !!!.");`
	rsql := `select * from t_test;`
	transaction := TransactionWith(mycli)
	transaction.Begin()
	transaction.Write(sql)
	transaction.Write(sql)
	transaction.Write(sql)
	transaction.Read(rsql)
	err := transaction.Commit()
	equalError(nil, err, "insert 3 times failed")
}

func tx_insert_1time() {
	sql := `insert into t_test (title) values ("hello !.");`
	transaction := TransactionWith(mycli)
	transaction.Begin()
	transaction.Write(sql)
	transaction.Write(sql)
	transaction.Write(sql)
	err := transaction.Commit()
	equalError(nil, err, "insert 1 times failed")
}
