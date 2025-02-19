package services

import (
	"context"
	"fmt"
	"jaya-transport-service/config"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type InfluxClient struct {
	Client   influxdb2.Client
	WriteApi api.WriteAPI
}

func NewInfluxClient(ctx context.Context, conf config.InfluxDBConfig) (*InfluxClient, error) {
	client := influxdb2.NewClientWithOptions(conf.URL, conf.Token,
		influxdb2.DefaultOptions().
			SetUseGZip(true))

	_, err := client.Health(ctx)
	if err != nil {
		return nil, fmt.Errorf("error when cek health influxdb, server url: %s. error: %w", conf.URL, err)
	}

	writeApi := client.WriteAPI(conf.Org, conf.Bucket)
	return &InfluxClient{Client: client, WriteApi: writeApi}, nil
}
