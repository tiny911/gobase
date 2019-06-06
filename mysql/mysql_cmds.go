package mysql

//insert
func (this *Cli) Insert(sqlstr string, args ...interface{}) (int64, error) {
	id, _, err := this.doWrite(sqlstr, args...)

	return id, err
}

//update
func (this *Cli) Update(sqlstr string, args ...interface{}) (int64, error) {
	_, num, err := this.doWrite(sqlstr, args...)

	return num, err
}

//delete
func (this *Cli) Delete(sqlstr string, args ...interface{}) (int64, error) {
	_, num, err := this.doWrite(sqlstr, args...)

	return num, err
}

//select
func (this *Cli) Select(sqlstr string, args ...interface{}) (map[string]string, error) {
	return this.doReadRow(sqlstr, args...)
}

//multi select
func (this *Cli) Selects(sqlstr string, args ...interface{}) ([]map[string]string, error) {
	return this.doReadRows(sqlstr, args...)
}

//write
func (this *Cli) Write(sqlstr string, args ...interface{}) (int64, int64, error) {
	id, num, err := this.doWrite(sqlstr, args...)

	return id, num, err
}
