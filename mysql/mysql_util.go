package mysql

import (
	"database/sql"
	"sync/atomic"

	"github.com/tiny911/doraemon/log"
)

func sqlLog(sqlstr string, args ...interface{}) {
	log.WithField(log.Fields{
		"sqlstr": sqlstr,
		"args":   args,
	}).Debug("mysql sql info.")
}

func (this *Cli) doWrite(sqlstr string, args ...interface{}) (int64, int64, error) {
	var (
		result sql.Result
		err    error
	)

	result, err = this.getWrite().Exec(sqlstr, args...)

	if err != nil {
		return 0, 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, 0, err
	}

	num, err := result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}

	sqlLog(sqlstr, args)

	return id, num, nil
}

func (this *Cli) doReadRow(sqlstr string, args ...interface{}) (map[string]string, error) {
	var (
		rows *sql.Rows
		err  error
	)

	rows, err = this.getRead().Query(sqlstr, args...)

	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	ret := make(map[string]string)

	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		var value string
		for i, col := range values {
			if col == nil {
				value = "" //把数据表中所有为null的地方改成“”
			} else {
				value = string(col)
			}

			ret[columns[i]] = value
		}

		break
	}

	rows.Close()
	sqlLog(sqlstr, args)

	return ret, err
}

func (this *Cli) doReadRows(sqlstr string, args ...interface{}) ([]map[string]string, error) {
	var (
		rows *sql.Rows
		err  error
	)

	rows, err = this.getRead().Query(sqlstr, args...)

	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	var rets = make([]map[string]string, 0)

	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		var ret = make(map[string]string) //这里要注意(对语法的理解)

		var value string
		for i, col := range values {
			if col == nil {
				value = "" //把数据表中所有为null的地方改成“”
			} else {
				value = string(col)
			}

			ret[columns[i]] = value
		}

		rets = append(rets, ret)
	}

	sqlLog(sqlstr, args)

	return rets, err
}

func (this *Cli) getWrite() *sql.DB {
	return this.masterConn
}

func (this *Cli) getRead() *sql.DB {
	length := len(this.slaveConn)
	if length == 0 { //没有从库，则取主库
		return this.getWrite()
	}
	count := this.incrAndGet()
	return this.slaveConn[count%length]
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
