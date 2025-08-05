package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	MQTT     		MQTTConfig
	JayaApi  		JayaApiConfig
	Redis    		RedisConfig
	TimescaleDB TimescaleDBConfig
}

type MQTTConfig struct {
	Broker   string
	ClientID string
	Topic    string
	Username string
	Password string
}

type JayaApiConfig struct {
	URL   string
	Token string
}

type RedisConfig struct {
	URL      string
	Password string
	Username string
	DB       int
}

type TimescaleDBConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	DBName   string
	SSLMode  string
	Enabled  bool
}

func LoadConfig() *Config {
	viper.SetConfigFile(".env")
	viper.ReadInConfig()

	return &Config{
		MQTT: MQTTConfig{
			Broker:   viper.GetString("MQTT_BROKER"),
			ClientID: viper.GetString("MQTT_CLIENT_ID"),
			Topic:    viper.GetString("MQTT_TOPIC"),
			Username: viper.GetString("MQTT_USERNAME"),
			Password: viper.GetString("MQTT_PASSWORD"),
		},
		JayaApi: JayaApiConfig{
			URL:   viper.GetString("JAYA_URL"),
			Token: viper.GetString("JAYA_TOKEN"),
		},
		Redis: RedisConfig{
			URL:      viper.GetString("REDIS_URL"),
			Password: viper.GetString("REDIS_PASSWORD"),
			Username: viper.GetString("REDIS_USERNAME"),
			DB:       viper.GetInt("REDIS_DB"),
		},
		TimescaleDB: TimescaleDBConfig{
			User:     viper.GetString("TIMESCALEDB_USER"),
			Password: viper.GetString("TIMESCALEDB_PASSWORD"),
			Host: 		viper.GetString("TIMESCALEDB_HOST"),
			Port:     viper.GetString("TIMESCALEDB_PORT"),
			DBName:   viper.GetString("TIMESCALEDB_DB_NAME"),
			SSLMode:  viper.GetString("TIMESCALEDB_SSL_MODE"),
			Enabled:  viper.GetBool("TIMESCALEDB_ENABLED"),
		},
	}
}