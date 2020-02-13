// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package env

import (
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/grab/talaria/internal/config"
)

var errConvert = errors.New("Unable to convert")
var errNotSupported = errors.New("Doesn't support this type")

// Configurer to fetch env variable values
type Configurer struct {
	key string
}

func New(key string) *Configurer {
	return &Configurer{
		key: key,
	}
}

// Configure fetches the values of the env variable for file name and sets that in the config
func (e *Configurer) Configure(c *config.Config) error {
	populate(c, e.key)
	return nil
}

// populate recursively traverse the config and populates the key from the env
// it uses the `env` tag in the struct definition
// ex := config struct {
//		presto struct `env:"PRESTO"`{
// 			port int `env:"PORT"`
//		}
// }
// to set the value of the port, set env variable KEY_PRESTO_PORT where key is the key used to initialize the env configurer
func populate(config interface{}, pre string) {
	reflectType := reflect.TypeOf(config).Elem()
	reflectValue := reflect.ValueOf(config).Elem()

	for i := 0; i < reflectType.NumField(); i++ {
		field := reflectValue.Field(i)

		switch field.Kind() {
		case reflect.Interface, reflect.Struct:
			a := field.Addr()
			tag, ok := reflectType.Field(i).Tag.Lookup("env")
			if ok {
				populate(a.Interface(), pre+"_"+tag)
			}

		case reflect.Ptr:
			if !field.IsNil() {
				switch field.Elem().Kind() {

				// If pointer to interface or struct, the recursively populate the struct/interface
				case reflect.Interface, reflect.Struct:
					v := field.Elem().Addr()
					tag, ok := reflectType.Field(i).Tag.Lookup("env")
					if ok {
						populate(v.Interface(), pre+"_"+tag)
					}
				// If pointer to primitive types then directly fill the values
				case reflect.Int64, reflect.Int32, reflect.Int, reflect.Float32, reflect.Float64, reflect.Bool, reflect.String, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					tag, ok := reflectType.Field(i).Tag.Lookup("env")
					if ok {
						val, ok := os.LookupEnv(pre + "_" + tag)
						if ok {
							vlc, err := convert(field.Elem(), val)
							if err == nil {
								field.Elem().Set(reflect.ValueOf(vlc))
							}
						}
					}

				}
			}

		case reflect.Array, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
			// not supported
			return

		//For primitive types end the recursion and directly fill the values
		default:
			tag, ok := reflectType.Field(i).Tag.Lookup("env")
			if ok {
				val, ok := os.LookupEnv(pre + "_" + tag)
				if ok {
					vlc, err := convert(field, val)
					if err == nil {
						field.Set(reflect.ValueOf(vlc))
					}
				}
			}
		}
	}
}

// convert a string to a particular type.
// Returns error if the conversion is not possible
func convert(key reflect.Value, value string) (interface{}, error) {
	switch key.Kind() {
	case reflect.Int:
		v, err := strconv.Atoi(value)
		if err != nil {
			return nil, errConvert
		}
		return v, nil

	case reflect.Int32:
		v, err := strconv.Atoi(value)
		if err != nil {
			return nil, errConvert
		}
		return int32(v), nil
	case reflect.Int64:
		v, err := strconv.Atoi(value)
		if err != nil {
			return nil, errConvert
		}
		return int64(v), nil
	case reflect.String:
		return value, nil
	case reflect.Bool:
		return strings.ToUpper(value) == "TRUE", nil
	case reflect.Float32:
		v, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, errConvert
		}
		return v, nil
	case reflect.Float64:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, errConvert
		}
		return v, nil
	}

	return "", errNotSupported

}
