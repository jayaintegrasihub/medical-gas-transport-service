package services

import (
	"errors"
	"fmt"
	"strings"
	"medical-gas-transport-service/config"
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
	ID                  string              `json:"id"`
	SerialNumber        string              `json:"serial_number"`
	Alias               string              `json:"alias"`
	Description         string              `json:"description"`
	InstallationPointFlow InstallationPointFlow `json:"installation_point_flow"`
	InstallationPointTank interface{}         `json:"installation_point_tank"`
	CreatedAt            JSONTime           `json:"created_at"`
	UpdatedAt            JSONTime           `json:"updated_at"`
}

type DeviceResponse struct {
    Status string `json:"status"`
    Data   struct {
        Device Device `json:"device,omitempty"`
        ID string `json:"id,omitempty"`
        SerialNumber string `json:"serial_number,omitempty"`
        Alias string `json:"alias,omitempty"`
        Description string `json:"description,omitempty"`
        InstallationPointFlow InstallationPointFlow `json:"installation_point_flow,omitempty"`
        InstallationPointTank interface{} `json:"installation_point_tank,omitempty"`
        CreatedAt string `json:"created_at,omitempty"`
        UpdatedAt string `json:"updated_at,omitempty"`
    } `json:"data"`
}

type InstallationPointFlow struct {
    ID string `json:"id"`
    Hospital string `json:"hospital"`
    SerialNumber string `json:"serial_number"`
    Floor *string `json:"floor"`
    Building *string `json:"building"`
    Room *string `json:"room"`
    Bed string `json:"bed"`
    InstallationCode string `json:"installation_code"`
    InstallationName string `json:"installation_name"`
    InstallationType string `json:"installation_type"`
    InstalledAt string `json:"installed_at"`
    LastMaintenanceDate string `json:"last_maintenance_date"`
    Device string `json:"device"`
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

var ErrDeviceNotFound = errors.New("Device Not Found")

func NewJayaService(conf config.JayaApiConfig) *Jaya {
	client := resty.New()
	client.SetBaseURL(conf.URL)
	// client.SetHeader("api-key", conf.Token)

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
        Alias: getString(rawResponse.Data, "alias"),
        Description: getString(rawResponse.Data, "description"),
    }
    
    if ipfData, ok := rawResponse.Data["installation_point_flow"].(map[string]interface{}); ok {
        device.InstallationPointFlow = InstallationPointFlow{
            ID: getString(ipfData, "id"),
            Hospital: getString(ipfData, "hospital"),
            SerialNumber: getString(ipfData, "serial_number"),
            Bed: getString(ipfData, "bed"),
            InstallationCode: getString(ipfData, "installation_code"),
            InstallationName: getString(ipfData, "installation_name"),
            InstallationType: getString(ipfData, "installation_type"),
        }
    }
    
    return device, nil
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
