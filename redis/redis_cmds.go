package redis

import (
	"net"
	"strconv"

	"github.com/gomodule/redigo/redis"
	"github.com/tiny911/doraemon/log"
)

type (
	ScorePair struct { // zset 的 member 和 score 对，字段名根据业务需要再修改
		Member string `json:"member"`
		Score  int    `json:"score"`
	}

	LuaScript struct {
		*redis.Script
	}

	Pipe struct {
		redis.Conn
	}

	Message struct {
		redis.Message
	}

	SubscribeT struct {
		conn redis.PubSubConn
		C    chan []byte
	}
)

// Keys 匹配得到所有的信息 key
func (this *Cli) Keys(patten string) ([]string, error) {
	return this.doStringSlice(true, "KEYS", false, patten)
}

//Key 相关
func (this *Cli) Del(keys ...string) (int, error) {
	args := strSliToInterfSli(keys...)
	return this.doInt(false, "DEL", false, args...)
}

func (this *Cli) Expire(key string, ttl int) (int, error) {
	return this.doInt(false, "EXPIRE", false, key, ttl)
}

func (this *Cli) Exists(key string) (bool, error) {
	return this.doBool(true, "EXISTS", false, key)
}

func (this *Cli) RandomKey() (string, error) {
	//ErrNil时，表示没有key
	return this.doString(true, "RANDOMKEY", true)
}

func (this *Cli) TTL(key string) (int, error) {
	return this.doInt(false, "TTL", false, key)
}

//if pattern=="", no regex, if count=0, decide by server
func (this *Cli) Scan(cursor int, pattern string, count int) (int, []string, error) {
	params := make([]interface{}, 0, 6)
	params = append(params, cursor)
	if pattern != "" {
		params = append(params, "MATCH", pattern)
	}
	if count > 0 {
		params = append(params, "COUNT", count)
	}

	reply, err := this.doRead("SCAN", params...)
	if err != nil {
		return 0, nil, err
	}

	replySlice, _ := reply.([]interface{})
	newCursor, _ := redis.Int(replySlice[0], nil)
	result, _ := redis.Strings(replySlice[1], nil)

	return newCursor, result, nil
}

//string 相关
func (this *Cli) Append(key, val string) (int, error) {
	return this.doInt(false, "APPEND", false, key, val)
}

func (this *Cli) Get(key string) (string, error) {
	return this.doString(true, "GET", true, key)
}

func (this *Cli) GetRaw(key string) (string, error) {
	return this.doString(true, "GET", false, key)
}

func (this *Cli) GetSet(key, val string) (string, error) {
	return this.doString(false, "GETSET", true, key, val)
}

func (this *Cli) IncrBy(key string, step int) (int, error) {
	return this.doInt(false, "INCRBY", false, key, step)
}

func (this *Cli) MGet(keys ...string) ([]string, error) {
	args := strSliToInterfSli(keys...)

	return this.doStringSlice(true, "MGET", false, args...)
}

func (this *Cli) MSet(keyvals ...string) (string, error) {
	args := strSliToInterfSli(keyvals...)
	return this.doString(false, "MSET", false, args...)
}

func (this *Cli) Set(key, val string, ttl int) (string, error) {
	var err error
	var reply interface{}
	if ttl == 0 {
		reply, err = this.doWrite("SET", key, val)
	} else {
		reply, err = this.doWrite("SETEX", key, ttl, val)
	}

	return redis.String(reply, err)
}

func (this *Cli) SetNx(key, val string) (int, error) {
	return this.doInt(false, "SETNX", false, key, val)
}

func (this *Cli) StrLen(key string) (int, error) {
	return this.doInt(true, "STRLEN", false, key)
}

//bit 相关
func (this *Cli) BitCount(key string) (int, error) {
	return this.doInt(true, "BITCOUNT", false, key)
}

func (this *Cli) BitCountWithPos(key string, start, end int) (int, error) {
	return this.doInt(true, "BITCOUNT", false, key, start, end)
}

func (this *Cli) BitOp(operation, destkey, key string, key2 ...string) (int, error) {
	params := []string{operation, destkey, key}
	params = append(params, key2...)
	args := strSliToInterfSli(params...)
	return this.doInt(false, "BITOP", false, args...)
}

func (this *Cli) BitPos(key string, bit int) (int, error) {
	return this.doInt(true, "BITPOS", false, key, bit)

}
func (this *Cli) BitPosWithPos(key string, bit, start, end int) (int, error) {
	return this.doInt(true, "BITPOS", false, key, bit, start, end)
}

func (this *Cli) GetBit(key string, offset int) (int, error) {
	return this.doInt(true, "GETBIT", false, key, offset)
}

func (this *Cli) SetBit(key string, offset, value int) (int, error) {
	return this.doInt(false, "SETBIT", false, key, offset, value)
}

//hash 相关
func (this *Cli) HDel(key string, fields ...string) (int, error) {
	args := strSliToInterfSliTwo(key, fields...)
	return this.doInt(false, "HDEL", false, args...)
}

func (this *Cli) HExists(key, field string) (bool, error) {
	return this.doBool(true, "HEXISTS", false, key, field)
}

func (this *Cli) HGet(key, field string) (string, error) {
	return this.doString(true, "HGET", true, key, field)
}

func (this *Cli) HGetAll(key string) (map[string]string, error) {
	values, err := this.doStringSlice(true, "HGETALL", false, key)
	ret := make(map[string]string)
	for i := 0; i < len(values); i += 2 {
		key := values[i]
		value := values[i+1]

		ret[key] = value
	}

	return ret, err
}

func (this *Cli) HIncr(key, field string) (int, error) {
	return this.doInt(false, "HINCRBY", false, key, field, 1)
}

func (this *Cli) HIncrBy(key, field string, increment int) (int, error) {
	return this.doInt(false, "HINCRBY", false, key, field, increment)
}

func (this *Cli) HKeys(key string) ([]string, error) {
	return this.doStringSlice(true, "HKEYS", false, key)
}

func (this *Cli) HLen(key string) (int, error) {
	return this.doInt(true, "HLEN", false, key)
}

func (this *Cli) HMGet(key string, fields ...string) ([]string, error) {
	args := strSliToInterfSliTwo(key, fields...)
	return this.doStringSlice(true, "HMGET", false, args...)
}

func (this *Cli) HMSet(key string, fieldvals ...string) (string, error) {
	args := strSliToInterfSliTwo(key, fieldvals...)
	return this.doString(false, "HMSET", false, args...)
}

func (this *Cli) HSet(key, field, val string) (int, error) {
	return this.doInt(false, "HSET", false, key, field, val)
}

func (this *Cli) HVals(key string) ([]string, error) {
	return this.doStringSlice(true, "HVALS", false, key)
}

//if pattern=="", no regex, if count=0, decide by server
func (this *Cli) HScan(key string, cursor int, pattern string, count int) (int, map[string]string, error) {
	params := make([]interface{}, 0, 6)
	params = append(params, key, cursor)
	if pattern != "" {
		params = append(params, "MATCH", pattern)
	}
	if count > 0 {
		params = append(params, "COUNT", count)
	}

	reply, err := this.doRead("HSCAN", params...)
	if err != nil {
		return 0, nil, err
	}

	replySlice, _ := reply.([]interface{})
	newCursor, _ := redis.Int(replySlice[0], nil)
	result, _ := redis.Strings(replySlice[1], nil)
	ret := make(map[string]string)
	for i := 0; i < len(result); i += 2 {
		key := result[i]
		value := result[i+1]

		ret[key] = value
	}

	return newCursor, ret, nil
}

//list 相关
func (this *Cli) LIndex(key string, index int) (string, error) {
	//ErrNil时，表示对应index不存在
	return this.doString(true, "LINDEX", true, key, index)
}

func (this *Cli) LInsert(key, op, pivot, val string) (int, error) {
	return this.doInt(false, "LINSERT", false, key, op, pivot, val)
}

func (this *Cli) LLen(key string) (int, error) {
	return this.doInt(true, "LLEN", false, key)
}

func (this *Cli) LPop(key string) (string, error) {
	//ErrNil时，表示对应list不存在
	return this.doString(false, "LPOP", true, key)
}

func (this *Cli) LPush(key string, vals ...string) (int, error) {
	args := strSliToInterfSliTwo(key, vals...)
	return this.doInt(false, "LPUSH", false, args...)
}

func (this *Cli) LRange(key string, start, stop int) ([]string, error) {
	return this.doStringSlice(true, "LRANGE", false, key, start, stop)
}

func (this *Cli) LRem(key string, count int, val string) (int, error) {
	return this.doInt(false, "LREM", false, key, count, val)
}

func (this *Cli) LSet(key string, index int, val string) (string, error) {
	return this.doString(false, "LSET", false, key, index, val)
}

func (this *Cli) LTrim(key string, start, stop int) (string, error) {
	return this.doString(false, "LTRIM", false, key, start, stop)
}

func (this *Cli) RPop(key string) (string, error) {
	//ErrNil时，表示对应list不存在
	return this.doString(false, "RPOP", true, key)
}

func (this *Cli) RPush(key string, vals ...string) (int, error) {
	args := strSliToInterfSliTwo(key, vals...)
	return this.doInt(false, "RPUSH", false, args...)
}

//set 结构操作
func (this *Cli) SAdd(key string, members ...string) (int, error) {
	args := strSliToInterfSliTwo(key, members...)
	return this.doInt(false, "SADD", false, args...)
}

func (this *Cli) SCard(key string) (int, error) {
	return this.doInt(true, "SCARD", false, key)
}

func (this *Cli) SDiff(keys ...string) ([]string, error) {
	args := strSliToInterfSli(keys...)
	return this.doStringSlice(true, "SDIFF", false, args...)
}

func (this *Cli) SInter(keys ...string) ([]string, error) {
	args := strSliToInterfSli(keys...)
	return this.doStringSlice(true, "SINTER", false, args...)
}

func (this *Cli) SIsMember(key, member string) (bool, error) {
	return this.doBool(true, "SISMEMBER", false, key, member)
}

func (this *Cli) SMembers(key string) ([]string, error) {
	return this.doStringSlice(true, "SMEMBERS", false, key)
}

func (this *Cli) SPop(key string) (string, error) {
	//ErrNil时，表示对应set不存在
	return this.doString(false, "SPOP", true, key)
}

func (this *Cli) SRandMember(key string) (string, error) {
	//ErrNil时，表示对应set不存在
	return this.doString(true, "SRANDMEMBER", true, key)
}

func (this *Cli) SRem(key string, members ...string) (int, error) {
	args := strSliToInterfSliTwo(key, members...)
	return this.doInt(false, "SREM", false, args...)
}

func (this *Cli) SUnion(keys ...string) ([]string, error) {
	args := strSliToInterfSli(keys...)
	return this.doStringSlice(true, "SUNION", false, args...)
}

//if pattern=="", no regex, if count=0, decide by server
func (this *Cli) SScan(key string, cursor int, pattern string, count int) (int, []string, error) {
	params := make([]interface{}, 0, 6)
	params = append(params, key, cursor)
	if pattern != "" {
		params = append(params, "MATCH", pattern)
	}
	if count > 0 {
		params = append(params, "COUNT", count)
	}

	reply, err := this.doRead("SSCAN", params...)
	if err != nil {
		return 0, nil, err
	}

	replySlice, _ := reply.([]interface{})
	newCursor, _ := redis.Int(replySlice[0], nil)
	result, _ := redis.Strings(replySlice[1], nil)

	return newCursor, result, nil
}

//zset 相关
func parseScorePair(strAry []string) []*ScorePair {
	ret := make([]*ScorePair, len(strAry)/2)

	for i := 0; i < len(strAry); i = i + 2 {
		member := strAry[i]
		score, err := strconv.ParseInt(strAry[i+1], 10, 64)

		if err == nil {
			ret[i/2] = &ScorePair{member, int(score)}
		}
	}

	return ret
}

func (this *Cli) ZAdd(key string, score int, member string) (int, error) {
	return this.doInt(false, "ZADD", false, key, score, member)
}

func (this *Cli) ZMAdd(key string, members ...*ScorePair) (int, error) {
	args := make([]interface{}, len(members)*2+1)
	args[0] = key
	for i, member := range members {
		args[2*i+1] = member.Score
		args[2*i+2] = member.Member
	}
	reply, err := this.doWrite("ZADD", args...)

	return redis.Int(reply, err)
}

func (this *Cli) ZCard(key string) (int, error) {
	return this.doInt(true, "ZCARD", false, key)
}

//min, max 可以是+inf, -inf
func (this *Cli) ZCount(key string, min, max string) (int, error) {
	return this.doInt(true, "ZCOUNT", false, key, min, max)
}

func (this *Cli) ZIncrBy(key string, incrNum int, member string) (int, error) {
	return this.doInt(false, "ZINCRBY", false, key, incrNum, member)
}

func (this *Cli) ZRange(key string, start, stop int) ([]string, error) {
	return this.doStringSlice(true, "ZRANGE", false, key, start, stop)
}

func (this *Cli) ZRangeWithScores(key string, start, stop int) ([]*ScorePair, error) {
	reply, err := this.doRead("ZRANGE", key, start, stop, "WITHSCORES")
	values, err := redis.Strings(reply, err)

	return parseScorePair(values), err
}

func (this *Cli) ZRank(key, member string) (int, error) {
	//ErrNil时，表示member不存在
	return this.doInt(true, "ZRANK", true, key, member)
}

func (this *Cli) ZRem(key string, members ...string) (int, error) {
	args := strSliToInterfSliTwo(key, members...)
	return this.doInt(false, "ZREM", false, args...)
}

func (this *Cli) ZRemRangeByRank(key string, start, stop int) (int, error) {
	return this.doInt(false, "ZREMRANGEBYRANK", false, key, start, stop)
}

func (this *Cli) ZRevRange(key string, start, stop int) ([]string, error) {
	return this.doStringSlice(true, "ZREVRANGE", false, key, start, stop)
}

func (this *Cli) ZRevRangeWithScores(key string, start, stop int) ([]*ScorePair, error) {
	reply, err := this.doRead("ZREVRANGE", key, start, stop, "WITHSCORES")
	values, err := redis.Strings(reply, err)

	return parseScorePair(values), err
}

func (this *Cli) ZRevRank(key, member string) (int, error) {
	//ErrNil时，表示member不存在
	return this.doInt(true, "ZREVRANK", true, key, member)
}

func (this *Cli) ZScore(key, member string) (int, error) {
	return this.doInt(true, "ZSCORE", true, key, member)
}

//lua script相关
func (this *Cli) Script(keyCount int, src string) *LuaScript {
	return &LuaScript{redis.NewScript(keyCount, src)}
}

func (this *Cli) LoadScript(script *LuaScript) error {
	conn := this.getWrite()
	defer conn.Close()
	return script.Load(conn)
}

func (this *Cli) Eval(script *LuaScript, keysAndArgs ...interface{}) (interface{}, error) {
	conn := this.getWrite()
	defer conn.Close()
	return script.Do(conn, keysAndArgs...)
}

//pipeline 相关
func (this *Cli) PipeLine(readonly bool) (*Pipe, error) {
	var conn redis.Conn
	if readonly {
		conn = this.getRead()
	} else {
		conn = this.getWrite()
	}

	return &Pipe{conn}, nil
}

func (this *Cli) PipeSend(pipe *Pipe, cmd string, args ...interface{}) error {
	return pipe.Send(cmd, args...)
}

func (this *Cli) PipeExec(pipe *Pipe) (interface{}, error) {
	return pipe.Do("")
}

func (this *Cli) PipeClose(pipe *Pipe) error {
	return pipe.Close()
}

//server 相关
func (this *Cli) BgSave() (string, error) {
	return this.doString(false, "BGSAVE", false)
}

//pubsub 相关  readtimeout need set to 0
func (this *Cli) Publish(channel, message string) (int, error) {
	return this.doInt(false, "PUBLISH", false, channel, message)
}

func (this *Cli) Subscribe(channel string) (*SubscribeT, error) {
	conn := this.getWrite()

	psc := redis.PubSubConn{Conn: conn}
	err := psc.Subscribe(channel)
	if err != nil {
		conn.Close()
		return nil, err
	}

	st := &SubscribeT{psc, make(chan []byte, 10)}

	go func() {
		defer func() {
			close(st.C)
		}()
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				st.C <- v.Data
			case redis.Subscription:
				log.WithField(log.Fields{
					"channel": v.Channel,
					"kind":    v.Kind,
					"count":   v.Count,
				}).Info("redis subscription.")
			case error:
				netError, ok := v.(net.Error)
				if ok && netError.Timeout() {
					log.WithField(log.Fields{
						"redis": v,
					}).Info("redis timeout!")
				} else if v.Error() != "redigo: connection closed" {
					log.WithField(log.Fields{
						"redis": v,
					}).Error("redis conn closed!")
				}
				return
			}
		}
	}()

	return st, nil
}

func (this *Cli) SubscribeClose(st *SubscribeT) error {
	return st.conn.Close()
}
