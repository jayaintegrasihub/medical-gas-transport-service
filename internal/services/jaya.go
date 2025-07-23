package services

import (
	"errors"
	"fmt"
	"medical-gas-transport-service/config"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

type JSONTime time.Time

type Data struct {
	Device Device `json:"device"`
}

type Hospital struct {
	ID string `json:"id"`
}

type Bed struct {
	ID string `json:"id"`
}

type Device struct {
	ID                          string                      `json:"id"`
	SerialNumber                string                      `json:"serial_number"`
    DeviceType                  string                      `json:"deviceType"`
	Alias                       string                      `json:"alias"`
	Notes                       string                      `json:"notes"`
	InstallationPointFlow       InstallationPointFlow       `json:"installation_point_flow"`
	InstallationPointTank       InstallationPointTank       `json:"installation_point_tank"`
	InstallationPointPressure   InstallationPointPressure   `json:"installation_point_pressure"`
}

type DeviceResponse struct {
    Status  string `json:"status"`
    Data    struct {
        Device                      Device                    `json:"device,omitempty"`
        ID                          string                    `json:"id,omitempty"`
        SerialNumber                string                    `json:"serial_number,omitempty"`
        Alias                       string                    `json:"alias,omitempty"`
        Description                 string                    `json:"description,omitempty"`
        InstallationPointFlow       InstallationPointFlow     `json:"installation_point_flow,omitempty"`
        InstallationPointTank       InstallationPointTank     `json:"installation_point_tank,omitempty"`
        InstallationPointPressure   InstallationPointPressure `json:"installation_point_pressure,omitempty"`
        CreatedAt                   string                    `json:"created_at,omitempty"`
        UpdatedAt                   string                    `json:"updated_at,omitempty"`
    } `json:"data"`
}

type InstallationPointFlow struct {
    ID                string   `json:"id"`
    Hospital          string   `json:"hospital"`
    SerialNumber      string   `json:"serial_number"`
    Floor             *string  `json:"floor"`
    Building          *string  `json:"building"`
    Room              *string  `json:"room"`
    Bed               *string  `json:"bed"`
    InstallationName  string   `json:"installation_name"`
    InstalledAt       string   `json:"installed_at"`
    Device            string   `json:"device"`
}

type InstallationPointTank struct {
    ID                    string   `json:"id"`
    Hospital              string   `json:"hospital"`
    Tank                  string   `json:"tank"`
    SerialNumber          string   `json:"serial_number"`
    InstallationName      string   `json:"installation_name"`
    MinimumLevelThreshold int      `json:"minimum_level_threshold"`
    MaximumLevelThreshold int      `json:"maximum_level_threshold"`
    InstalledAt           string   `json:"installed_at"`
    Device                string   `json:"device"`
    DeviceThreshold       string   `json:"device_threshold"`
}

type InstallationPointPressure struct {
    ID                  string    `json:"id"`
    Hospital            string    `json:"hospital"`
    SerialNumber        string    `json:"serial_number"`
    InstallationName    string    `json:"installation_name"`
    Floor               *string   `json:"floor"`
    Building            *string   `json:"building"`
    Room                *string   `json:"room"`
    PressureUnit        string    `json:"pressure_unit"`
    InstalledAt         string    `json:"installed_at"`
    Device              string    `json:"device"`
    DeviceThreshold     string    `json:"device_threshold"`
}

type Tenant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type JayaProvisionResponse struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Status   string `json:"status"`
}
type Jaya struct {
	client *resty.Client
}

type TankConversion struct {
    Slope      float64 `json:"slope"`
    Intercept  float64 `json:"intercept"`
    InH2OMax   float64 `json:"in_h2o_max"`
    InH2OMin   float64 `json:"in_h2o_min"`
}

var ErrDeviceNotFound = errors.New("Device Not Found")

func NewJayaService(conf config.JayaApiConfig) *Jaya {
	client := resty.New()
	client.SetBaseURL(conf.URL)
	client.SetHeader("api-key", conf.Token)

	return &Jaya{client: client}
}

func (j *Jaya) GetDevice(serialNumber string) (*Device, error) {
    var rawResponse struct {
        Status string `json:"status"`
        Data   map[string]interface{} `json:"data"`
    }
    
    resp, err := j.client.R().SetResult(&rawResponse).Get("/devices/serial_number/" + serialNumber)
    if err != nil {
        return nil, fmt.Errorf("error when request devices %s from jaya core. error: %w", serialNumber, err)
    }

    if resp.StatusCode() == 404 {
        return nil, ErrDeviceNotFound
    }

    if resp.StatusCode() != 200 {
        return nil, fmt.Errorf("failed to fetch device from Jaya API, status code: %d, body: %s", resp.StatusCode(), string(resp.Body()))
    }

    device := &Device{
        ID: getString(rawResponse.Data, "id"),
        SerialNumber: getString(rawResponse.Data, "serial_number"),
        DeviceType: getString(rawResponse.Data, "deviceType"),
        Alias: getString(rawResponse.Data, "alias"),
        Notes: getString(rawResponse.Data, "notes"),
    }
    
    if ipfData, ok := rawResponse.Data["installation_point_flow"].(map[string]interface{}); ok {
        var floor, building, room, bed *string
        if val, ok := ipfData["floor"]; ok {
            if strVal, ok := val.(string); ok && strVal != "" {
            floor = &strVal
            }
        }
        if val, ok := ipfData["building"]; ok {
            if strVal, ok := val.(string); ok && strVal != "" {
            building = &strVal
            }
        }
        if val, ok := ipfData["room"]; ok {
            if strVal, ok := val.(string); ok && strVal != "" {
            room = &strVal
            }
        }
        if val, ok := ipfData["bed"]; ok {
            if strVal, ok := val.(string); ok && strVal != "" {
            bed = &strVal
            }
        }
        device.InstallationPointFlow = InstallationPointFlow{
            ID:               getString(ipfData, "id"),
            Hospital:         getString(ipfData, "hospital"),
            SerialNumber:     getString(ipfData, "serial_number"),
            Floor:            floor,
            Building:         building,
            Room:             room,
            Bed:              bed,
            InstallationName: getString(ipfData, "installation_name"),
            InstalledAt:      getString(ipfData, "installed_at"),
            Device:           getString(ipfData, "device"),
        }
    }

    if iptData, ok := rawResponse.Data["installation_point_tank"].(map[string]interface{}); ok {
        device.InstallationPointTank = InstallationPointTank{
            ID:                    getString(iptData, "id"),
            Hospital:              getString(iptData, "hospital"),
            Tank:                  getString(iptData, "tank"),
            SerialNumber:          getString(iptData, "serial_number"),
            InstallationName:      getString(iptData, "installation_name"),
            MinimumLevelThreshold: getInt(iptData, "minimum_level_threshold"),
            MaximumLevelThreshold: getInt(iptData, "maximum_level_threshold"),
            InstalledAt:           getString(iptData, "installed_at"),
            Device:                getString(iptData, "device"),
            DeviceThreshold:       getString(iptData, "device_threshold"),
        }
    }

    if ippData, ok := rawResponse.Data["installation_point_pressure"].(map[string]interface{}); ok {
        var floor, building, room *string
        if val, ok := ippData["floor"]; ok {
            if strVal, ok := val.(string); ok && strVal != "" {
            floor = &strVal
            }
        }
        if val, ok := ippData["building"]; ok {
            if strVal, ok := val.(string); ok && strVal != "" {
            building = &strVal
            }
        }
        if val, ok := ippData["room"]; ok {
            if strVal, ok := val.(string); ok && strVal != "" {
            room = &strVal
            }
        }
        device.InstallationPointPressure = InstallationPointPressure{
            ID:                getString(ippData, "id"),
            Hospital:          getString(ippData, "hospital"),
            SerialNumber:      getString(ippData, "serial_number"),
            InstallationName:  getString(ippData, "installation_name"),
            Floor:             floor,
            Building:          building,
            Room:              room,
            PressureUnit:      getString(ippData, "pressure_unit"),
            InstalledAt:       getString(ippData, "installed_at"),
            Device:            getString(ippData, "device"),
            DeviceThreshold:   getString(ippData, "device_threshold"),
        }
    }
    
    return device, nil
}

func (j *Jaya) GetConversionTable(serialNumber string) ([]TankConversion, error) {
    var rawResponse struct {
        Status string           `json:"status"`
        Data   struct {
            TankConversionTable []TankConversion `json:"tank_conversion_table"`
        } `json:"data"`
    }
    resp, err := j.client.R().SetResult(&rawResponse).Get("/tank-conversion-table/" + serialNumber + "/formula")
    if err != nil {
        return nil, err
    }

    if resp.StatusCode() != 200 {
        return nil, errors.New(string(resp.Body()))
    }

    return rawResponse.Data.TankConversionTable, nil
}

func getString(data map[string]interface{}, key string) string {
    if val, ok := data[key]; ok {
        if strVal, ok := val.(string); ok {
            return strVal
        }
    }
    return ""
}

func (j *Jaya) Provision(id string) (*JayaProvisionResponse, error) {
	var body interface{} = map[string]interface{}{"serialNumber": id}

	resp, err := j.client.R().SetBody(body).SetResult(JayaProvisionResponse{}).Post("/provisioning")
	if err != nil {
		return nil, fmt.Errorf("error when request provisioning %s from jaya core. error: %w", id, err)
	}

	if resp.StatusCode() != 200 {
		return nil, errors.New(string(resp.Body()))
	}

	result := resp.Result().(*JayaProvisionResponse)
	return result, nil
}

func getInt(data map[string]interface{}, key string) int {
    if val, ok := data[key]; ok {
        switch v := val.(type) {
        case float64:
            return int(v)
        case int:
            return v
        case string:
            if i, err := strconv.Atoi(v); err == nil {
                return i
            }
        }
    }
    return 0
}

func (j *JSONTime) UnmarshalJSON(b []byte) error {
    s := strings.Trim(string(b), "\"")
    if s == "null" || s == "" || s == "0001-01-01T00:00:00Z" {
        *j = JSONTime(time.Time{})
        return nil
    }
    
    t, err := time.Parse(time.RFC3339, s)
    if err != nil {
        t, err = time.Parse("2006-01-02T15:04:05.000Z", s)
        if err != nil {
            return err
        }
    }
    
    *j = JSONTime(t)
    return nil
}
