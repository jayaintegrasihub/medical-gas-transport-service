package services

import (
	"errors"
	"fmt"
	"jaya-transport-service/config"
	"time"

	"github.com/go-resty/resty/v2"
)

type DeviceResponse struct {
	Status string `json:"status"`
	Data   Data   `json:"data"`
}

type Data struct {
	Device Device `json:"device"`
}

type Device struct {
	ID           string            `json:"id"`
	SerialNumber string            `json:"serialNumber"`
	Alias        string            `json:"alias"`
	Description  string            `json:"description"`
	Type         string            `json:"type"`
	Group        map[string]string `json:"group"` // Flexible group field
	CreatedAt    time.Time         `json:"createdAt"`
	UpdatedAt    time.Time         `json:"updatedAt"`
	Tenant       Tenant            `json:"tenant"`
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
	client.SetHeader("api-key", conf.Token)

	return &Jaya{client: client}
}

func (j *Jaya) GetDevice(id string) (*Device, error) {
	resp, err := j.client.R().SetResult(DeviceResponse{}).Get("/service-connector/" + id)
	if err != nil {
		return nil, fmt.Errorf("error when request devices %s from jaya core. error: %w", id, err)
	}

	if resp.StatusCode() != 200 {
		return nil, errors.New(string(resp.Body()))
	}

	result := resp.Result().(*DeviceResponse)
	return &result.Data.Device, nil
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
