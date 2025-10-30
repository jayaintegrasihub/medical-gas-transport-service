package config

import (
	"github.com/caarlos0/env/v11"
)

type Config struct {
	MQTT     		MQTTConfig
	JayaApi  		JayaApiConfig
	Redis    		RedisConfig
	TimescaleDB TimescaleDBConfig
}

type MQTTConfig struct {
	Broker   string `env:"MQTT_BROKER,required"`
	ClientID string `env:"MQTT_CLIENT_ID"`
	Topic    string `env:"MQTT_TOPIC"`
	Username string `env:"MQTT_USERNAME,required"`
	Password string `env:"MQTT_PASSWORD,required"`
}

type JayaApiConfig struct {
	URL   string `env:"JAYA_URL,required"`
	Token string `env:"JAYA_TOKEN,required"`
}

type RedisConfig struct {
	URL      string `env:"REDIS_URL,required"`
	Password string `env:"REDIS_PASSWORD,required"`
	Username string `env:"REDIS_USERNAME,required"`
	DB       int 	`env:"REDIS_DB,required"`
}

type TimescaleDBConfig struct {
	User     string `env:"TIMESCALEDB_USER,required"`
	Password string `env:"TIMESCALEDB_PASSWORD,required"`
	Host     string `env:"TIMESCALEDB_HOST,required"`
	Port     string `env:"TIMESCALEDB_PORT,required"`
	DBName   string `env:"TIMESCALEDB_DB_NAME,required"`
	SSLMode  string `env:"TIMESCALEDB_SSL_MODE"`
}

func LoadConfig() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	
	return cfg, nil
}