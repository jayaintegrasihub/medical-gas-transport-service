package internal

import "fmt"

type DeviceHealth struct {
	MessageId   int           `json:"msgId"`
	Ts          int           `json:"ts"`
	Uptime      int           `json:"uptime"`
	Temperature float32       `json:"temp"`
	Humidity    float32       `json:"hum"`
	Rssi        float32       `json:"rssi"`
	HwVersion   string        `json:"hwVer"`
	FwVersion   string        `json:"fwVer"`
	RdVersion   string        `json:"rdVer"`
	Model       string        `json:"model"`
	Modules     []ModulesData `json:"modules"`
	ResetReason int           `json:"resetReason"`
}

type ModulesData struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type eventTopic struct {
	prefix    string
	version   string
	gatewayId string
	nodeId    string
	subject   string
	deviceId  string
}

type ProvisionRequest struct {
	SerialNumber string `json:"serialNumber"`
}

type ProvisionResponse struct {
	Pattern string                `json:"pattern"`
	Data    ProvisionResponseData `json:"data"`
}

type ProvisionResponseData struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Status   string `json:"status"`
}

type NodeIOData struct {
	Ts   int      `json:"ts"`
	Rssi int      `json:"rssi"`
	Data []IOData `json:"data"`
}

type IOData struct {
	Value interface{} `json:"value"`
	Tag   string      `json:"tag"`
}

func convertToMap(dataArray []IOData) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	if len(dataArray) == 0 {
		return nil, fmt.Errorf("array length is 0")
	}

	for _, data := range dataArray {
		if len(data.Tag) > 0 {
			m[data.Tag] = data.Value
		}
	}

	if len(m) == 0 {
		return nil, fmt.Errorf("data map is empty")
	}
	return m, nil
}
