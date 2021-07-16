package mongo

import (
	"context"
	"time"

	"github.com/qiniu/qmgo"
)

// Cli mongo client
type Cli struct {
	url    string
	dbName string
	*qmgo.Client
}

// NewCli 生成cli实例
func NewCli(url, dbName string) *Cli {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//client, err := qmgo.NewClient(ctx, &qmgo.Config{Uri: "mongodb://localhost:27017"})
	client, err := qmgo.NewClient(ctx, &qmgo.Config{Uri: "mongodb://localhost:27017"})
	if err != nil {
		panic(err)
	}
	client.Database(dbName)

	return &Cli{
		url:    url,
		dbName: dbName,
		Client: client,
	}
}

// Close 关闭连接
func (c *Cli) Close() {
	if c.Client != nil {
		c.Client.Close(context.Background())
	}
}
