package internal

import (
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
	DeviceUptime 			int     `json:"uptime"`
	DeviceTemp   			float64 `json:"temp"`
	DeviceHum    			float64 `json:"hum"`
	DeviceLong   			float64 `json:"long"`
	DeviceLat    			float64 `json:"lat"`
	DeviceRSSI   			int     `json:"rssi"`
	DeviceHWVer  			string  `json:"hwVer"`
	DeviceFWVer  			string  `json:"fwVer"`
	DeviceRDVer  			string  `json:"rdVer"`
	DeviceModel  			string  `json:"model"`
	DeviceMemUsage 		int			`json:"memory"`
	DeviceResetReason int    	`json:"resetReason"`
}

type SensorLevelData struct {
	Timestamp    time.Time `json:"-"`
	SerialNumber string    `json:"-"`

	Ts     int64  `json:"ts"`
	Device Device `json:"device"`

	Level float64 `json:"level"`
	Solar struct {
		SolarBattStatus   []string `json:"battStat"`
		SolarDeviceStatus []string `json:"solarStat"`
		SolarLoadStatus   []string `json:"loadStat"`
		SolarBattTemp     int      `json:"battTemp"`
		SolarBattLevel    int      `json:"battLevel"`
		SolarBattVolt	  int      `json:"battVolt"`
		SolarEGen         []int    `json:"eGen"`
		SolarECom         []int    `json:"eCom"`
	} `json:"power"`
}

type SensorFlowData struct {
	Timestamp    time.Time `json:"-"`
	SerialNumber string    `json:"-"`
	Ts           int64     `json:"ts"`
	Device       Device    `json:"device"`
	TotalVolume  float64   `json:"-"` // (vHi*65536) + (vLo) + (vDec/1000)
	VHi          float64   `json:"vHi"`
	VLo          float64   `json:"vLo"`
	VDec         float64   `json:"vDec"`
	FlowRate     float64   `json:"-"` // ((fRateHi * 65536) + fRateLo)/1000
	FRateHi      float64   `json:"fRateHi"`
	FRateLo      float64   `json:"fRateLo"`
}

type PressureData struct {
	Measurement string  `json:"measurement"`
	Value       float64 `json:"value"`
	Connection  int     `json:"connection"`
	Enable      bool    `json:"enable"`
	HighLimit   float64 `json:"high_limit"`
	LowLimit    float64 `json:"low_limit"`
}

type SensorPressureData struct {
	Ts           int64          `json:"ts"`
	Device       Device         `json:"device"`
	Data         []PressureData `json:"data"`
	SerialNumber string         `json:"-"`
	Timestamp    time.Time      `json:"-"`
}

type MqttMessage struct {
	Topic   string
	Payload []byte
}
