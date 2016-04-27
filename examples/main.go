package main

import (
	"fmt"

	redis "github.com/yadvendar/redigo-wrapper"
)

const (
	KEY_TEMPLATE                   = "uniqueid:?:suffix" // define a key template using KEY_VAR_PLACEHOLDER as specified below in Config object
	HASH_KEY_STATUS                = "myhashkeyforstatus"
	HASH_KEY_BOOL_TEMPLATE         = "hashkeyfor:?" // define a hash key template using KEY_VAR_PLACEHOLDER
	VALUE_HASH_KEY_STATUS_ACTIVE   = "active"
	VALUE_HASH_KEY_STATUS_INACTIVE = "inactive"
)

func main() {
	// initialize a redis connection pool
	RPool := redis.NewRConnectionPool(
		redis.Config{
			Server:              "localhost:6379",
			Password:            "password",
			KEY_PREFIX:          "development",
			KEY_DELIMITER:       ":",
			KEY_VAR_PLACEHOLDER: "?",
		},
	)

	// get hold of connection object from the pool
	RConnection := RPool.Get()
	defer RConnection.Close()
	RConn := (&RConnection)

	id := "asdf"
	// parse a template to obtain key
	parsedKey, _ := redis.ParseKey(KEY_TEMPLATE, []string{id})

	// store a string value
	redis.HSet(RConn, parsedKey, HASH_KEY_STATUS, VALUE_HASH_KEY_STATUS_ACTIVE)

	// read a string value
	returnedStringValue, err := redis.HGetString(RConn, parsedKey, HASH_KEY_STATUS)

	// for bool
	hasAccess := true
	// parse a template to obtain hash key
	parsedHKey, _ := redis.ParseKey(HASH_KEY_BOOL_TEMPLATE, []string{id})

	// store a bool value
	redis.HSet(RConn, parsedKey, parsedHKey, hasAccess)

	//read a bool value
	returnedBoolValue, err := redis.HGetBool(RConn, parsedKey, parsedHKey)

	fmt.Println(returnedStringValue, returnedBoolValue, err)
}
