package mongo

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Cli mongo client
type Cli struct {
	url     string
	dbName  string
	session *mgo.Session
}

// NewCli 生成cli实例
func NewCli(url, dbName string) *Cli {
	session, err := mgo.Dial(url)
	if err != nil {
		//TODO: log err
		return nil
	}

	session.SetMode(mgo.Monotonic, true)
	return &Cli{
		url:     url,
		dbName:  dbName,
		session: session,
	}
}

func (c *Cli) Insert(table string, docs interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Insert(docs)
}

func (c *Cli) Update(table string, selector interface{}, update interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Update(selector, update)
}

func (c *Cli) UpdateId(table string, id string, update interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).UpdateId(bson.ObjectIdHex(id), update)
}

func (c *Cli) Upsert(table string, selector interface{}, update interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	_, err := session.DB(c.dbName).C(table).Upsert(selector, update)
	return err
}

func (c *Cli) GetOne(table string, query interface{}, result interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Find(query).One(result)
}

func (c *Cli) GetOneWithSelector(table string, query interface{}, selector interface{}, result interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Find(query).Select(selector).One(result)
}

func (c *Cli) GetCnt(table string, query interface{}) (int, error) {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Find(query).Count()
}

func (c *Cli) GetAll(table string, query interface{}, result interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Find(query).All(result)
}

func (c *Cli) PipeAll(table string, query interface{}, result interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Pipe(query).All(result)
}

func (c *Cli) GetAllBySort(table string, query interface{}, result interface{}, fields ...string) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Find(query).Sort(fields...).All(result)
}

func (c *Cli) GetAllBySortOnLimit(table string, query interface{}, cnt int, result interface{}, fields ...string) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Find(query).Sort(fields...).Limit(cnt).All(result)
}

func (c *Cli) GetAllWithSelectorBySort(table string, query interface{}, selector interface{}, result interface{}, fields ...string) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Find(query).Select(selector).Sort(fields...).All(result)
}

func (c *Cli) GetAllWithSelectorBySortOnLimit(table string, query interface{}, selector interface{}, cnt int, result interface{}, fields ...string) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).Find(query).Select(selector).Sort(fields...).Limit(cnt).All(result)
}

func (c *Cli) DelAll(table string, selector interface{}) error {
	session := c.session.Copy()
	defer session.Close()

	_, err := session.DB(c.dbName).C(table).RemoveAll(selector)
	return err
}

func (c *Cli) Index(table string, index mgo.Index) error {
	session := c.session.Copy()
	defer session.Close()

	return session.DB(c.dbName).C(table).EnsureIndex(index)
}

const UuidCollection = "t_uuid"

type Uuid struct {
	Collection string `bson:"collection"`
	CurrentID  int64  `bson:"current_id"`
}

func (c *Cli) Uuid(collection string) (int64, error) {
	var (
		err  error
		uuid = &Uuid{}
	)

	session := c.session.Copy()
	defer session.Close()

	_, err = session.DB(c.dbName).C(UuidCollection).Find(
		bson.M{"collection": collection},
	).Apply(mgo.Change{
		Update:    bson.M{"$inc": bson.M{"current_id": 1}},
		ReturnNew: true,
	}, uuid)

	return uuid.CurrentID, err
}

// Close 关闭连接
func (c *Cli) Close() {
	if c.session != nil {
		c.session.Close()
	}
}
