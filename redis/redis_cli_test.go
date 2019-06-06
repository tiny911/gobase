package redis

import (
	"fmt"
	"log"
	"regexp"
	"runtime"
	"testing"
	"time"
)

var (
	masterIp, slaveIp     = "127.0.0.1", "127.0.0.1"
	masterPort, slavePort = "6379", "6379"
	masterPwd, slavePwd   = "", ""
	masterDbno, slaveDbno = "", ""
)

func getCli() *Cli {
	master := NewRedis(masterIp, masterPort, masterPwd, masterDbno)
	slave1 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave2 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave3 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	cli := NewCli(master, []*Redis{slave1, slave2, slave3})
	return cli
}

func TestInitOldPool(t *testing.T) {
	fmt.Println("==============test init old pool==============")

	cli := getCli()

	_, err := cli.Del("foo", "testfoo")
	equalError(nil, err, "del failed")

	val_s, err := cli.Set("foo", "bar", 0)
	equalString(val_s, "OK", "set failed")
	equalError(nil, err, "set failed")

	val_s, err = cli.Get("foo")
	equalString(val_s, "bar", "get failed")
	equalError(nil, err, "get failed")

	val_i, err := cli.IncrBy("testfoo", 3)
	equalInt(val_i, 3, "incrby failed")
	equalError(nil, err, "incrby failed")

	cli.Close()
}

func TestInitNewPool(t *testing.T) {
	fmt.Println("==============test init new pool==============")

	master := NewRedis(masterIp, masterPort, masterPwd, masterDbno)
	slave1 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave2 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave3 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	cli := NewCliCustom(master, []*Redis{slave1, slave2, slave3}, 1000*time.Millisecond, 500*time.Millisecond, 500*time.Millisecond, 60*time.Second, 50, 3, true)

	_, err := cli.Del("foo", "testfoo")
	equalError(nil, err, "del failed")

	val_s, err := cli.Set("foo", "bar", 0)
	equalString(val_s, "OK", "set failed")
	equalError(nil, err, "set failed")

	val_s, err = cli.Get("foo")
	equalString(val_s, "bar", "get failed")
	equalError(nil, err, "get failed")

	val_i, err := cli.IncrBy("testfoo", 3)
	equalInt(val_i, 3, "incrby failed")
	equalError(nil, err, "incrby failed")

	cli.Close()
}

func TestConnectTimeout(t *testing.T) {
	fmt.Println("==============test connect timeout==============")

	master := NewRedis(masterIp, masterPort, masterPwd, masterDbno)
	slave1 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave2 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave3 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	cli := NewCliCustom(master, []*Redis{slave1, slave2, slave3}, 1*time.Nanosecond, 500*time.Millisecond, 500*time.Millisecond, 60*time.Second, 50, 3, true)

	_, err := cli.Set("foo", "bar", 0)
	equalRegexp("dial.*i/o timeout", err.Error(), "connect timeout failed")

	cli.Close()
}

func TestReadTimeout(t *testing.T) {
	fmt.Println("==============test read timeout==============")

	master := NewRedis(masterIp, masterPort, masterPwd, masterDbno)
	slave1 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave2 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave3 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	cli := NewCliCustom(master, []*Redis{slave1, slave2, slave3}, 1000*time.Millisecond, 5*time.Nanosecond, 500*time.Millisecond, 60*time.Second, 50, 3, true)

	_, err := cli.Set("foo", "bar", 0)
	equalRegexp("read.*i/o timeout", err.Error(), "read timeout failed")

	_, err = cli.Get("foo")
	equalRegexp("read.*i/o timeout", err.Error(), "read timeout failed")

	cli.Close()
}

func TestWriteTimeout(t *testing.T) {
	fmt.Println("==============test write timeout==============")

	master := NewRedis(masterIp, masterPort, masterPwd, masterDbno)
	slave1 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave2 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave3 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	cli := NewCliCustom(master, []*Redis{slave1, slave2, slave3}, 1000*time.Millisecond, 500*time.Millisecond, 5*time.Nanosecond, 60*time.Second, 50, 3, true)

	_, err := cli.Set("foo", "bar", 0)
	equalRegexp("write.*i/o timeout", err.Error(), "write timeout failed")

	_, err = cli.Get("foo")
	equalRegexp("write.*i/o timeout", err.Error(), "write timeout failed")

	cli.Close()
}

func TestMaxActive(t *testing.T) {
	fmt.Println("==============test max active==============")

	master := NewRedis(masterIp, masterPort, masterPwd, masterDbno)
	slave1 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave2 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave3 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	cli := NewCliCustom(master, []*Redis{slave1, slave2, slave3}, 1000*time.Millisecond, 500*time.Millisecond, 500*time.Millisecond, 1*time.Second, 1, 1, false)

	num := 5
	for i := 0; i < num; i++ {
		go func() {
			_, err := cli.Set("foo", "bar", 0)
			if err != nil {
				fmt.Println("set fail! err:", err)
			}

			val, err := cli.Get("foo")
			if err != nil {
				fmt.Println("get fail! err:", err)
			} else {
				fmt.Println(val)
			}
		}()
	}

	select {
	case <-time.After(time.Millisecond * 500):
	}
	cli.Close()
}

func TestWait(t *testing.T) {
	fmt.Println("==============test wait==============")

	master := NewRedis(masterIp, masterPort, masterPwd, masterDbno)
	slave1 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave2 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	slave3 := NewRedis(slaveIp, slavePort, slavePwd, slaveDbno)
	cli := NewCliCustom(master, []*Redis{slave1, slave2, slave3}, 1000*time.Millisecond, 500*time.Millisecond, 500*time.Millisecond, 1*time.Second, 1, 1, true)

	num := 5
	for i := 0; i < num; i++ {
		go func() {
			val_s, err := cli.Set("foo", "bar", 0)
			equalString(val_s, "OK", "set failed")
			equalError(nil, err, "wait failed")

			val_s, err = cli.Get("foo")
			equalString(val_s, "bar", "get failed")
			equalError(nil, err, "wait failed")
		}()
	}

	select {
	case <-time.After(time.Millisecond * 500):
	}

	cli.Close()
}

func BenchmarkNoConsul1(b *testing.B) {
	cli := getCli()

	for i := 0; i < b.N; i++ {
		val_s, err := cli.Set("foo", "bar", 0)
		if val_s != "OK" || err != nil {
			fmt.Println("set error:", err)
		}

		val_s, err = cli.Get("foo")
		if val_s != "bar" || err != nil {
			fmt.Println("get error:", err)
		}
	}
	cli.Close()
}

func BenchmarkNoConsul20(b *testing.B) {
	cli := getCli()

	b.SetParallelism(20 / runtime.GOMAXPROCS(0))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			val_s, err := cli.Set("foo", "bar", 0)
			if val_s != "OK" || err != nil {
				fmt.Println("set error:", err)
			}

			val_s, err = cli.Get("foo")
			if val_s != "bar" || err != nil {
				fmt.Println("get error:", err)
			}
		}
	})
	cli.Close()
}

func BenchmarkNoConsul50(b *testing.B) {
	cli := getCli()

	b.SetParallelism(50 / runtime.GOMAXPROCS(0))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			val_s, err := cli.Set("foo", "bar", 0)
			if val_s != "OK" || err != nil {
				fmt.Println("set error:", err)
			}

			val_s, err = cli.Get("foo")
			if val_s != "bar" || err != nil {
				fmt.Println("get error:", err)
			}
		}
	})
	cli.Close()
}

func BenchmarkNoConsul100(b *testing.B) {
	cli := getCli()

	b.SetParallelism(100 / runtime.GOMAXPROCS(0))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			val_s, err := cli.Set("foo", "bar", 0)
			if val_s != "OK" || err != nil {
				fmt.Println("set error:", err)
			}

			val_s, err = cli.Get("foo")
			if val_s != "bar" || err != nil {
				fmt.Println("get error:", err)
			}
		}
	})
	cli.Close()
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
