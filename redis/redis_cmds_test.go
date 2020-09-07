package redis

import (
	"fmt"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestKeyMethod(t *testing.T) {
	fmt.Println("==============test key method==============")
	cli := getCli()

	//key
	_, err := cli.Del("foo", "foo1", "foo2")
	equalError(nil, err, "del failed")

	val_i, err := cli.Expire("foo", 10)
	equalInt(0, val_i, "expire failed")
	equalError(nil, err, "expire failed")

	val_b, err := cli.Exists("foo")
	equalBool(false, val_b, "exists failed")
	equalError(nil, err, "exists failed")

	_, err = cli.RandomKey()
	equalError(nil, err, "randomkey failed")

	ttl, err := cli.TTL("foo")
	equalError(nil, err, "ttl failed")
	fmt.Println("ttl:", ttl)

	cli.Close()
}

func TestStringMethod(t *testing.T) {
	fmt.Println("==============test string method==============")
	cli := getCli()

	cli.Del("foo")
	cli.Del("foo2")

	//string
	val_i, err := cli.Append("foo", "bar")
	fmt.Printf("append:%d", val_i)

	equalInt(val_i, 3, "append failed")
	equalError(nil, err, "append failed")

	val_s, err := cli.Get("foo")
	equalString(val_s, "bar", "get failed")
	equalError(nil, err, "get failed")

	val_s, err = cli.GetRaw("fdjlafjka")
	equalString(val_s, "", "getraw failed")
	equalError(err, redis.ErrNil, "getraw failed")

	val_s, err = cli.GetSet("foo", "bar")
	equalString(val_s, "bar", "getset failed")
	equalError(nil, err, "getset failed")

	val_i, err = cli.IncrBy("foo", 10)
	equalInt(0, val_i, "incrby failed")
	notEqualError(nil, err, "incrby failed")

	val_ss, err := cli.MGet("foo", "foo2")
	equalString(val_ss[0], "bar", "mget failed")
	equalString(val_ss[1], "", "mget failed")
	equalError(nil, err, "mget failed")

	val_s, err = cli.MSet("foo1", "bar1", "foo2", "bar2")
	equalString(val_s, "OK", "mset failed")
	equalError(nil, err, "mset failed")

	val_s, err = cli.Set("foo1", "bar1", 0)
	equalString(val_s, "OK", "set failed")
	equalError(nil, err, "set failed")

	val_i, err = cli.SetNx("foo1", "bar2")
	equalInt(0, val_i, "setnx failed")
	equalError(nil, err, "setnx failed")
	val_s, _ = cli.Get("foo1")
	equalString(val_s, "bar1", "setnx failed")

	val_s, err = cli.MSet("foo1", "bar1", "foo2", "bar2")
	equalString(val_s, "OK", "setex failed")
	equalError(nil, err, "setex failed")

	val_i, err = cli.StrLen("foo")
	equalInt(val_i, 3, "strlen failed")
	equalError(nil, err, "strlen failed")

	cli.Close()
}

func TestBitMethod(t *testing.T) {
	fmt.Println("==============test bit method==============")
	cli := getCli()

	//bit
	cli.Set("mykey", "foobar", 0)
	val_i, err := cli.BitCount("mykey")
	equalInt(val_i, 26, "bitcount failed")
	equalError(nil, err, "bitcount failed")

	val_i, err = cli.BitCountWithPos("mykey", 1, 1)
	equalInt(val_i, 6, "bitcount failed")
	equalError(nil, err, "bitcount failed")

	val_i, err = cli.BitCount("mykeyNoExist")
	equalInt(val_i, 0, "bitcount failed")
	equalError(nil, err, "bitcount failed")
	cli.Del("mykey")

	cli.Set("mykey1", "foobar", 0)
	cli.Set("mykey2", "abcdef", 0)
	val_i, err = cli.BitOp("AND", "mydest", "mykey1", "mykey2")
	equalInt(val_i, 6, "bitop failed")
	equalError(nil, err, "bitop failed")
	val_s, err := cli.Get("mydest")
	equalString(val_s, "`bc`ab", "bitop failed")
	equalError(nil, err, "bitop failed")
	cli.Del("mydest", "mykey1", "mykey2")

	cli.Set("mykey", "\xff\xf0\x00", 0)
	val_i, err = cli.BitPos("mykey", 0)
	equalInt(val_i, 12, "bitpos failed")
	equalError(nil, err, "bitpos failed")

	cli.Set("mykey", "\x00\x00\x00", 0)
	val_i, err = cli.BitPos("mykey", 1)
	equalInt(val_i, -1, "bitpos failed")
	equalError(nil, err, "bitpos failed")

	cli.Set("mykey", "\x00\xff\xf0", 0)
	val_i, err = cli.BitPosWithPos("mykey", 1, 0, -1)
	equalInt(val_i, 8, "bitpos failed")
	equalError(nil, err, "bitpos failed")

	val_i, err = cli.BitPosWithPos("mykey", 1, 2, -1)
	equalInt(val_i, 16, "bitpos failed")
	equalError(nil, err, "bitpos failed")
	cli.Del("mykey")

	val_i, err = cli.SetBit("mykey", 7, 1)
	equalInt(val_i, 0, "setbit failed")
	equalError(nil, err, "setbit failed")

	val_i, err = cli.GetBit("mykey", 0)
	equalInt(val_i, 0, "getbit failed")
	equalError(nil, err, "getbit failed")

	val_i, err = cli.GetBit("mykey", 7)
	equalInt(val_i, 1, "getbit failed")
	equalError(nil, err, "getbit failed")

	val_i, err = cli.GetBit("mykey", 100)
	equalInt(val_i, 0, "getbit failed")
	equalError(nil, err, "getbit failed")
	cli.Del("mykey")

	cli.Close()
}

func TestHashMethod(t *testing.T) {
	fmt.Println("==============test hash method==============")
	cli := getCli()

	//hash
	_, err := cli.Del("hfoo", "hfoo1", "hfoo2")
	_, err = cli.HDel("hfoo", "hfoo1", "hfoo2")
	equalError(nil, err, "hdel failed")

	val_b, err := cli.HExists("hfoo", "bar")
	equalBool(false, val_b, "hexists failed")
	equalError(nil, err, "hexists failed")

	val_s, err := cli.HGet("hfoo", "bar")
	equalString(val_s, "", "hget failed")
	equalError(nil, err, "hget failed")

	val_mss, err := cli.HGetAll("hfoo")
	equalInt(0, len(val_mss), "hgetall failed")
	equalError(nil, err, "hgetall failed")

	val_i, err := cli.HIncr("hfoo", "ibar")
	equalInt(val_i, 1, "hincr failed")
	equalError(nil, err, "hincr failed")

	val_i, err = cli.HIncrBy("hfoo", "ibar", 2)
	equalInt(val_i, 3, "hincrby failed")
	equalError(nil, err, "hincrby failed")

	cli.HDel("hfoo", "ibar")

	val_ss, err := cli.HKeys("hfoo")
	equalInt(0, len(val_ss), "hkeys failed")
	equalError(nil, err, "hkeys failed")

	val_i, err = cli.HLen("hfoo")
	equalInt(0, val_i, "hlen failed")
	equalError(nil, err, "hlen failed")

	val_ss, err = cli.HMGet("hfoo", "bar", "bar1")
	equalString(val_ss[0], "", "hmget failed")
	equalString(val_ss[1], "", "hmget failed")
	equalError(nil, err, "hmget failed")

	val_s, err = cli.HMSet("hfoo", "bar", "0", "bar1", "1")
	equalString(val_s, "OK", "hmset failed")
	equalError(nil, err, "hmset failed")

	val_i, err = cli.HSet("hfoo", "bar2", "2")
	equalInt(val_i, 1, "hset failed")
	equalError(nil, err, "hset failed")

	val_ss, err = cli.HVals("hfoo")
	equalString(val_ss[0], "0", "hvals failed")
	equalString(val_ss[1], "1", "hvals failed")
	equalString(val_ss[2], "2", "hvals failed")
	equalError(nil, err, "hvals failed")

	cli.Close()
}

func TestListMethod(t *testing.T) {
	fmt.Println("==============test list method==============")
	cli := getCli()

	//list
	_, err := cli.Del("hfoo")
	_, err = cli.LIndex("hfoo", 0)
	equalError(nil, err, "lindex failed")

	val_i, err := cli.LInsert("hfoo", "BEFORE", "bar", "bar0")
	equalError(nil, err, "linsert failed")

	val_i, err = cli.LLen("hfoo")
	equalInt(0, val_i, "llen failed")
	equalError(nil, err, "llen failed")

	val_s, err := cli.LPop("hfoo")
	equalError(nil, err, "lindex failed")

	val_i, err = cli.LPush("hfoo", "bar", "bar1")
	equalInt(val_i, 2, "lpush failed")
	equalError(nil, err, "lpush failed")

	val_ss, err := cli.LRange("hfoo", 0, -1)
	equalString(val_ss[0], "bar1", "lrange failed")
	equalString(val_ss[1], "bar", "lrange failed")
	equalError(nil, err, "lrange failed")

	val_i, err = cli.LRem("hfoo", 0, "bar1")
	equalInt(val_i, 1, "lrem failed")
	equalError(nil, err, "lrem failed")

	val_s, err = cli.LSet("hfoo", 0, "barbar")
	equalString(val_s, "OK", "lset failed")
	equalError(nil, err, "lset failed")

	val_s, err = cli.LTrim("hfoo", 1, -1)
	equalString(val_s, "OK", "ltrim failed")
	equalError(nil, err, "ltrim failed")

	val_s, err = cli.RPop("hfoo")
	equalString(val_s, "", "rpop failed")
	equalError(nil, err, "rpop failed")

	val_i, err = cli.RPush("hfoo", "bar")
	equalInt(val_i, 1, "rpush failed")
	equalError(nil, err, "rpush failed")

	cli.Close()
}

func TestSetMethod(t *testing.T) {
	fmt.Println("==============test set method==============")
	cli := getCli()

	//set
	_, err := cli.Del("sfoo", "sfoo1")
	val_i, err := cli.SAdd("sfoo", "bar", "bar1")
	equalInt(val_i, 2, "sadd failed")
	equalError(nil, err, "sadd failed")
	_, _ = cli.SAdd("sfoo1", "bar2", "bar1")

	val_i, err = cli.SCard("sfoo")
	equalInt(val_i, 2, "scard failed")
	equalError(nil, err, "scard failed")

	val_ss, err := cli.SDiff("sfoo", "sfoo1")
	equalInt(len(val_ss), 1, "sdiff failed")
	equalError(nil, err, "sdiff failed")

	val_ss, err = cli.SInter("sfoo", "sfoo1")
	equalInt(len(val_ss), 1, "sinter failed")
	equalError(nil, err, "sinter failed")

	val_b, err := cli.SIsMember("sfoo", "bar")
	equalBool(true, val_b, "sismember failed")
	equalError(nil, err, "sismember failed")

	val_ss, err = cli.SMembers("sfoo")
	equalInt(len(val_ss), 2, "smembers failed")
	equalError(nil, err, "smembers failed")

	_, err = cli.SPop("sfoo")
	equalError(nil, err, "spop failed")

	_, err = cli.SRandMember("sfoo")
	equalError(nil, err, "srandmember failed")

	val_i, err = cli.SRem("sfoo1", "bar1", "bar2")
	equalInt(val_i, 2, "srem failed")
	equalError(nil, err, "srem failed")

	val_ss, err = cli.SUnion("sfoo", "sfoo1")
	equalInt(len(val_ss), 1, "sunion failed")
	equalError(nil, err, "sunion failed")

	cli.Close()
}

func TestZsetMethod(t *testing.T) {
	fmt.Println("==============test zset method==============")
	cli := getCli()

	//zset
	_, err := cli.Del("zfoo")
	val_i, err := cli.ZAdd("zfoo", 1, "bar1")
	equalInt(val_i, 1, "zadd failed")
	equalError(nil, err, "zadd failed")

	val_i, err = cli.ZMAdd("zfoo", &ScorePair{"bar2", 2}, &ScorePair{"bar3", 3})
	equalInt(val_i, 2, "zmadd failed")
	equalError(nil, err, "zmadd failed")

	val_i, err = cli.ZCard("zfoo")
	equalInt(val_i, 3, "zcard failed")
	equalError(nil, err, "zcard failed")

	val_i, err = cli.ZCount("zfoo", "-inf", "+inf")
	equalInt(val_i, 3, "zcount failed")
	equalError(nil, err, "zcount failed")

	val_i, err = cli.ZIncrBy("zfoo", 10, "bar1")
	equalInt(val_i, 11, "zincrby failed")
	equalError(nil, err, "zincrby failed")

	val_ss, err := cli.ZRange("zfoo", 0, -1)
	equalInt(len(val_ss), 3, "zrange failed")
	equalError(nil, err, "zrange failed")

	val_sps, err := cli.ZRangeWithScores("zfoo", 0, -1)
	equalInt(len(val_sps), 3, "zrangewithscores failed")
	equalError(nil, err, "zrangewithscores failed")

	val_i, err = cli.ZRank("zfoo", "bar1")
	equalInt(val_i, 2, "zrank failed")
	equalError(nil, err, "zrank failed")

	val_i, err = cli.ZRem("zfoo", "bar1", "bar2")
	equalInt(val_i, 2, "zrem failed")
	equalError(nil, err, "zrem failed")

	_, _ = cli.ZMAdd("zfoo", &ScorePair{"bar2", 2}, &ScorePair{"bar3", 3})
	val_i, err = cli.ZRemRangeByRank("zfoo", 0, -1)
	equalInt(val_i, 2, "zremrangebyrank failed")
	equalError(nil, err, "zremrangebyrank failed")

	_, _ = cli.ZMAdd("zfoo", &ScorePair{"bar2", 2}, &ScorePair{"bar3", 3})
	val_ss, err = cli.ZRevRange("zfoo", 0, -1)
	equalInt(len(val_ss), 2, "zrevrange failed")
	equalError(nil, err, "zrevrange failed")

	val_sps, err = cli.ZRevRangeWithScores("zfoo", 0, -1)
	equalInt(len(val_sps), 2, "zrevrangewithscores failed")
	equalError(nil, err, "zrevrangewithscores failed")

	val_i, err = cli.ZRevRank("zfoo", "bar3")
	equalInt(val_i, 0, "zrevrank failed")
	equalError(nil, err, "zrevrank failed")

	val_i, err = cli.ZScore("zfoo", "bar3")
	equalInt(val_i, 3, "zscore failed")
	equalError(nil, err, "zscore failed")

	cli.Set("foo", "bar", 0)
	script := cli.Script(1, "return redis.call('get', KEYS[1])")
	err = cli.LoadScript(script)
	equalError(nil, err, "loadscript failed")
	val_inter, err := cli.Eval(script, "foo")
	equalString(string(val_inter.([]byte)), "bar", "eval failed")
	equalError(nil, err, "eval failed")
	cli.Del("foo")

	pipe, err := cli.PipeLine(false)
	equalError(nil, err, "pipe line failed")
	err = cli.PipeSend(pipe, "set", "foo", "bar")
	equalError(nil, err, "pipe send failed")
	err = cli.PipeSend(pipe, "get", "foo")
	equalError(nil, err, "pipe send failed")
	val_inter, err = cli.PipeExec(pipe)
	equalString(val_inter.([]interface{})[0].(string), "OK", "pipe exec failed")
	equalString(string(val_inter.([]interface{})[1].([]byte)), "bar", "pipe exec failed")
	equalError(nil, err, "pipe exec failed")
	err = cli.PipeClose(pipe)
	equalError(nil, err, "pipe close failed")
	cli.Del("foo")

	cli.Close()
}
