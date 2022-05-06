package main

import "encoding/json"

var fieldsDefined = []string{
	"time", "scope", "event", "update_at", "location", "city", "weather",
	"uuid", "id",
}

func isKeyInCommonfield(key string) bool {
	for _, field := range fieldsDefined {
		if key == field {
			return true
		}
	}
	return false
}

func ComputeRow(rowArg map[string]interface{}) (interface{}, error) {
	data := make(map[string]interface{})
	for key, val := range rowArg {
		if !isKeyInCommonfield(key) {
			data[key] = val
		}
	}
	dataJSONStr, err := json.Marshal(data)
	return string(dataJSONStr), err
}
