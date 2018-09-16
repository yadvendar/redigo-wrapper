package redis

import (
	"errors"
	"log"
	"strings"
	"time"

	redigo "github.com/garyburd/redigo/redis"
)

const (
	REDIS_KEYWORD_SADD            = "SADD"
	REDIS_KEYWORD_SCARD           = "SCARD"
	REDIS_KEYWORD_SISMEMBER       = "SISMEMBER"
	REDIS_KEYWORD_SMEMBERS        = "SMEMBERS"
	REDIS_KEYWORD_SREM            = "SREM"
	REDIS_KEYWORD_HSET            = "HSET"
	REDIS_KEYWORD_HGET            = "HGET"
	REDIS_KEYWORD_HMSET           = "HMSET"
	REDIS_KEYWORD_HMGET           = "HMGET"
	REDIS_KEYWORD_HDEL            = "HDEL"
	REDIS_KEYWORD_HGETALL         = "HGETALL"
	REDIS_KEYWORD_SET             = "SET"
	REDIS_KEYWORD_SETNX           = "SETNX"
	REDIS_KEYWORD_SETEX           = "SETEX"
	REDIS_KEYWORD_GET             = "GET"
	REDIS_KEYWORD_TTL             = "TTL"
	REDIS_KEYWORD_STRLEN          = "STRLEN"
	REDIS_KEYWORD_EXPIRE          = "EXPIRE"
	REDIS_KEYWORD_DELETE          = "DEL"
	REDIS_KEYWORD_KEYS            = "KEYS"
	REDIS_KEYWORD_HKEYS           = "HKEYS"
	REDIS_KEYWORD_EXISTS          = "EXISTS"
	REDIS_KEYWORD_PERSIST         = "PERSIST"
	REDIS_KEYWORD_ZADD            = "ZADD"
	REDIS_KEYWORD_ZREM            = "ZREM"
	REDIS_KEYWORD_ZRANGE          = "ZRANGE"
	REDIS_KEYWORD_ZRANGE_BY_SCORE = "ZRANGEBYSCORE"
	REDIS_KEYWORD_WITHSCORES      = "WITHSCORES"
	REDIS_KEYWORD_INCR            = "INCR"
	REDIS_KEYWORD_DECR            = "DECR"
	REDIS_KEYWORD_INCRBY          = "INCRBY"
	REDIS_KEYWORD_DECRBY          = "DECRBY"
	REDIS_KEYWORD_INCRBYFLOAT     = "INCRBYFLOAT"
	REDIS_KEYWORD_DECRBYFLOAT     = "DECRBYFLOAT"
)

type Config struct {
	Server   string
	Password string
	MaxIdle  int // Maximum number of idle connections in the pool.

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait                bool
	KEY_PREFIX          string // prefix to all keys; example is "dev environment name"
	KEY_DELIMITER       string // delimiter to be used while appending keys; example is ":"
	KEY_VAR_PLACEHOLDER string // placeholder to be parsed using given arguments to obtain a final key; example is "?"
}

var conf Config

func NewRConnectionPool(c Config) *redigo.Pool {
	conf = c
	return &redigo.Pool{
		MaxIdle:     conf.MaxIdle,
		IdleTimeout: conf.IdleTimeout,
		MaxActive:   conf.MaxActive,
		Wait:        conf.Wait,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", conf.Server)
			if err != nil {
				log.Println("redigo-wrapper: Redis: Dial failed", err)
				return nil, err
			}
			if _, err := c.Do("AUTH", conf.Password); err != nil && conf.Password != "" {
				log.Println("redigo-wrapper: Redis: AUTH failed", err)
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				log.Println("redigo-wrapper: Unable to ping to redis server:", err)
			}
			return err
		},
	}
}

func ParseKey(key string, vars []string) (string, error) {
	arr := strings.Split(key, conf.KEY_VAR_PLACEHOLDER)
	actualKey := ""
	if len(arr) != len(vars)+1 {
		return "", errors.New("redis/connection.go: Insufficient arguments to parse key")
	} else {
		for index, val := range arr {
			if index == 0 {
				actualKey = arr[index]
			} else {
				actualKey += vars[index-1] + val
			}
		}
	}
	return getPrefixedKey(actualKey), nil
}

func getPrefixedKey(key string) string {
	return conf.KEY_PREFIX + conf.KEY_DELIMITER + key
}
func StripEnvKey(key string) string {
	return strings.TrimLeft(key, conf.KEY_PREFIX+conf.KEY_DELIMITER)
}
func SplitKey(key string) []string {
	return strings.Split(key, conf.KEY_DELIMITER)
}
func Expire(RConn *redigo.Conn, key string, ttl int) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_EXPIRE, key, ttl)
}
func Persist(RConn *redigo.Conn, key string) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_PERSIST, key)
}

func Delete(RConn *redigo.Conn, key string) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_DELETE, key)
}
func Set(RConn *redigo.Conn, key string, data interface{}) (interface{}, error) {
	//set
	return (*RConn).Do(REDIS_KEYWORD_SET, key, data)
}
func SetNX(RConn *redigo.Conn, key string, data interface{}) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_SETNX, key, data)
}
func SetEx(RConn *redigo.Conn, key string, ttl int, data interface{}) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_SETEX, key, ttl, data)
}
func Get(RConn *redigo.Conn, key string) (interface{}, error) {
	//get
	return (*RConn).Do(REDIS_KEYWORD_GET, key)
}
func GetTTL(RConn *redigo.Conn, key string) (time.Duration, error) {
	ttl, err := redigo.Int64((*RConn).Do(REDIS_KEYWORD_TTL, key))
	return time.Duration(ttl) * time.Second, err
}
func GetString(RConn *redigo.Conn, key string) (string, error) {
	return redigo.String((*RConn).Do(REDIS_KEYWORD_GET, key))
}
func GetInt(RConn *redigo.Conn, key string) (int, error) {
	return redigo.Int((*RConn).Do(REDIS_KEYWORD_GET, key))
}
func GetStringLength(RConn *redigo.Conn, key string) (int, error) {
	return redigo.Int((*RConn).Do(REDIS_KEYWORD_STRLEN, key))
}
func ZAdd(RConn *redigo.Conn, key string, score float64, data interface{}) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_ZADD, key, score, data)
}
func ZRem(RConn *redigo.Conn, key string, data interface{}) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_ZREM, key, data)
}
func ZRange(RConn *redigo.Conn, key string, start int, end int, withScores bool) ([]interface{}, error) {
	if withScores {
		return redigo.Values((*RConn).Do(REDIS_KEYWORD_ZRANGE, key, start, end, REDIS_KEYWORD_WITHSCORES))
	}
	return redigo.Values((*RConn).Do(REDIS_KEYWORD_ZRANGE, key, start, end))
}
func SAdd(RConn *redigo.Conn, setName string, data interface{}) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_SADD, setName, data)
}
func SCard(RConn *redigo.Conn, setName string) (int64, error) {
	return redigo.Int64((*RConn).Do(REDIS_KEYWORD_SCARD, setName))
}
func SIsMember(RConn *redigo.Conn, setName string, data interface{}) (bool, error) {
	return redigo.Bool((*RConn).Do(REDIS_KEYWORD_SISMEMBER, setName, data))
}
func SMembers(RConn *redigo.Conn, setName string) ([]string, error) {
	return redigo.Strings((*RConn).Do(REDIS_KEYWORD_SMEMBERS, setName))
}
func SRem(RConn *redigo.Conn, setName string, data interface{}) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_SREM, setName, data)
}
func HSet(RConn *redigo.Conn, key string, HKey string, data interface{}) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_HSET, key, HKey, data)
}

func HGet(RConn *redigo.Conn, key string, HKey string) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_HGET, key, HKey)
}

func HMGet(RConn *redigo.Conn, key string, hashKeys ...string) ([]interface{}, error) {
	args := []interface{}{key}
	args = append(args, hashKeys)

	ret, err := (*RConn).Do(REDIS_KEYWORD_HMGET, args)
	if err != nil {
		return nil, err
	}
	reta, ok := ret.([]interface{})
	if !ok {
		return nil, errors.New("result not an array")
	}
	return reta, nil
}

func HMSet(RConn *redigo.Conn, key string, hashKeys []string, vals []interface{}) (interface{}, error) {
	if len(hashKeys) == 0 || len(hashKeys) != len(vals) {
		var ret interface{}
		return ret, errors.New("bad length")
	}
	input := []interface{}{key}
	for i, v := range hashKeys {
		input = append(input, v, vals[i])
	}
	return (*RConn).Do(REDIS_KEYWORD_HMSET, input...)
}

func HGetString(RConn *redigo.Conn, key string, HKey string) (string, error) {
	return redigo.String((*RConn).Do(REDIS_KEYWORD_HGET, key, HKey))
}
func HGetFloat(RConn *redigo.Conn, key string, HKey string) (float64, error) {
	f, err := redigo.Float64((*RConn).Do(REDIS_KEYWORD_HGET, key, HKey))
	return float64(f), err
}
func HGetInt(RConn *redigo.Conn, key string, HKey string) (int, error) {
	return redigo.Int((*RConn).Do(REDIS_KEYWORD_HGET, key, HKey))
}
func HGetInt64(RConn *redigo.Conn, key string, HKey string) (int64, error) {
	return redigo.Int64((*RConn).Do(REDIS_KEYWORD_HGET, key, HKey))
}
func HGetBool(RConn *redigo.Conn, key string, HKey string) (bool, error) {
	return redigo.Bool((*RConn).Do(REDIS_KEYWORD_HGET, key, HKey))
}
func HDel(RConn *redigo.Conn, key string, HKey string) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_HDEL, key, HKey)
}
func HGetAll(RConn *redigo.Conn, key string) (interface{}, error) {
	return (*RConn).Do(REDIS_KEYWORD_HGETALL, key)
}

func HGetAllValues(RConn *redigo.Conn, key string) ([]interface{}, error) {
	return redigo.Values((*RConn).Do(REDIS_KEYWORD_HGETALL, key))
}
func HGetAllString(RConn *redigo.Conn, key string) ([]string, error) {
	return redigo.Strings((*RConn).Do(REDIS_KEYWORD_HGETALL, key))
}

// NOTE: Use this in production environment with extreme care.
// Read more here:https://redis.io/commands/keys
func Keys(RConn *redigo.Conn, pattern string) ([]string, error) {
	return redigo.Strings((*RConn).Do(REDIS_KEYWORD_KEYS, pattern))
}

func HKeys(RConn *redigo.Conn, key string) ([]string, error) {
	return redigo.Strings((*RConn).Do(REDIS_KEYWORD_HKEYS, key))
}

func Exists(RConn *redigo.Conn, key string) (bool, error) {
	count, err := redigo.Int((*RConn).Do(REDIS_KEYWORD_EXISTS, key))
	if count == 0 {
		return false, err
	} else {
		return true, err
	}
}

func Incr(RConn *redigo.Conn, key string) (int64, error) {
	return redigo.Int64((*RConn).Do(REDIS_KEYWORD_INCR, key))
}

func Decr(RConn *redigo.Conn, key string) (int64, error) {
	return redigo.Int64((*RConn).Do(REDIS_KEYWORD_DECR, key))
}

func IncrBy(RConn *redigo.Conn, key string, incBy int64) (int64, error) {
	return redigo.Int64((*RConn).Do(REDIS_KEYWORD_INCRBY, key, incBy))
}

func DecrBy(RConn *redigo.Conn, key string, decrBy int64) (int64, error) {
	return redigo.Int64((*RConn).Do(REDIS_KEYWORD_DECRBY, key))
}

func IncrByFloat(RConn *redigo.Conn, key string, incBy float64) (float64, error) {
	return redigo.Float64((*RConn).Do(REDIS_KEYWORD_INCRBYFLOAT, key, incBy))
}

func DecrByFloat(RConn *redigo.Conn, key string, decrBy float64) (float64, error) {
	return redigo.Float64((*RConn).Do(REDIS_KEYWORD_DECRBYFLOAT, key, decrBy))
}

func Scan(RConn *redigo.Conn, cursor int64, pattern string, count int64) (int64, []string, error) {
	var items []string
	var newCursor int64

	values, err := redigo.Values((*RConn).Do("SCAN", cursor, "MATCH", pattern, "COUNT", count))
	if err != nil {
		return 0, nil, err
	}
	values, err = redigo.Scan(values, &newCursor, &items)
	if err != nil {
		return 0, nil, err
	}

	return newCursor, items, nil
}
