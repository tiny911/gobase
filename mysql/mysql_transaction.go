package mysql

import (
	"database/sql"
	"errors"
)

var (
	errTxEmpty error = errors.New("transaction is empty.")
)

type Transaction struct {
	db *sql.DB
	tx *sql.Tx
}

func TransactionWith(cli *Cli) *Transaction {
	return &Transaction{
		db: cli.masterConn,
	}
}

func (this *Transaction) Begin() error {
	var (
		err error
	)

	this.tx, err = this.db.Begin()

	if err != nil {
		return err
	}

	return nil
}

func (this *Transaction) Commit() error {
	if this.tx == nil {
		return errTxEmpty
	}

	err := this.tx.Commit()
	if err != nil {
		return err
	}

	this.tx = nil
	return nil
}

func (this *Transaction) Rollback() error {
	if this.tx == nil {
		return errTxEmpty
	}

	err := this.tx.Rollback()
	if err != nil {
		return err
	}

	this.tx = nil
	return nil
}

func (this *Transaction) Write(sqlstr string, args ...interface{}) (int64, int64, error) {
	result, err := this.tx.Exec(sqlstr, args...)

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

func (this *Transaction) Read(sqlstr string, args ...interface{}) ([]map[string]string, error) {
	var (
		rows *sql.Rows
		err  error
	)

	rows, err = this.tx.Query(sqlstr, args...)

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

		var ret = make(map[string]string)

		var value string
		for i, col := range values {
			if col == nil {
				value = ""
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
