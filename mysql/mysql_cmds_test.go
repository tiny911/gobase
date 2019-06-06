package mysql

import (
	"testing"
)

func TestInsert(t *testing.T) {
	var (
		cli *Cli   = getCli()
		sql string = `insert into t_test (title) values ("hello tittle with no close.");`
	)

	creatTable()
	cli.Insert(sql)
	cli.Close()
	dropTable()
}
