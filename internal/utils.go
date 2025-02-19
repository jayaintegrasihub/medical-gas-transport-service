package internal

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

func extractTopic(topicString string) (*eventTopic, error) {
	topic := strings.Split(topicString, "/")

	if len(topic) == 4 {
		return &eventTopic{
			prefix:    topic[0],
			version:   topic[1],
			gatewayId: topic[2],
			subject:   topic[3],
			deviceId:  topic[2],
		}, nil
	} else if len(topic) == 5 {
		return &eventTopic{
			prefix:    topic[0],
			version:   topic[1],
			gatewayId: topic[2],
			nodeId:    topic[3],
			subject:   topic[4],
			deviceId:  topic[3],
		}, nil
	} else {
		return nil, fmt.Errorf("topic not supported to be extract : %v", topicString)
	}
}

func StructToMapReflect(data interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	value := reflect.ValueOf(data)
	dataType := reflect.TypeOf(data)

	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		fieldName := dataType.Field(i).Name
		result[lowercaseFirstChar(fieldName)] = field.Interface()
	}
	return result
}

func lowercaseFirstChar(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

// AI/v2/gatewayid/devicehealth
// AI/v2/gatewayid/nodeid/devicehealth
// AI/v2/gatewayid/nodeid/serial
// AI/v2/gatewayid/nodeid/io
