package internal

import (
	"fmt"
	"time"
)

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

type Device struct {
	DeviceUptime  int     `json:"uptime"`
	DeviceTemp    float64 `json:"temp"`
	DeviceHum     float64 `json:"hum"`
	DeviceLong    float64 `json:"long"`
	DeviceLat     float64 `json:"lat"`
	DeviceRSSI    int     `json:"rssi"`
	DeviceHWVer   string  `json:"hwVer"`
	DeviceFWVer   string  `json:"fwVer"`
	DeviceRDVer   string  `json:"rdVer"`
	DeviceModel   string  `json:"model"`
}

type OxygenLevelData struct {
	Timestamp          time.Time `json:"-"`
	SerialNumber       string    `json:"-"`
	
	Ts                int64     `json:"ts"`
	Device            Device	`json:"device"`
	
	Level             float64   `json:"level"`
	Solar             struct {
		SolarBattStatus    []string  `json:"battStat"`
		SolarDeviceStatus  []string  `json:"deviceStat"`
		SolarLoadStatus    []string  `json:"loadStat"`
		SolarBattTemp      int       `json:"battTemp"`
		SolarBattLevel     int       `json:"battLevel"`
		SolarEGen          []int     `json:"eGen"`
		SolarECom          []int     `json:"eCom"`
	} `json:"solar"`
}

type OxygenFlowData struct {
	Timestamp          		time.Time `json:"-"`
	SerialNumber       		string    `json:"-"`
	
	Ts                	int64     	`json:"ts"`
	Device            	Device		`json:"device"`
	
	FlowRate          	float64   `json:"flow_rate"`
}

type PressureData struct {
	Measurement string  `json:"measurement"`
	Value       float64 `json:"value"`
	Connection  int     `json:"connection"`
	Enable      bool    `json:"enable"`
	HighLimit   float64 `json:"high_limit"`
	LowLimit    float64 `json:"low_limit"`
}

type OxygenPressureData struct {
	Ts           int64          `json:"ts"`
	Device       Device         `json:"device"`
	Data         []PressureData `json:"data"`
	SerialNumber string         `json:"-"`
	Timestamp    time.Time      `json:"-"`
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
