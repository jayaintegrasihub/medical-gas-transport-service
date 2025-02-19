package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"jaya-transport-service/config"
	"log"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type MqttClient struct {
	Client *autopaho.ConnectionManager
}

func NewMqttClient(ctx context.Context, cfg config.MQTTConfig) (*MqttClient, error) {
	u, err := url.Parse(cfg.Broker)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL: %w", err)
	}

	clientID, err := generateRandomClientID(8)
	if err != nil {
		return nil, fmt.Errorf("error generating client ID: %w", err)
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		ConnectUsername:               cfg.Username,
		ConnectPassword:               []byte(cfg.Password),
		KeepAlive:                     60,
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         0,
		OnConnectError:                func(err error) { fmt.Printf("Error whilst attempting connection: %s\n", err) },
		ClientConfig: paho.ClientConfig{
			ClientID:      clientID,
			OnClientError: func(err error) { log.Panicf("Client error: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					fmt.Printf("Server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					fmt.Printf("Server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}

	c, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}

	if err = c.AwaitConnection(ctx); err != nil {
		panic(err)
	}

	return &MqttClient{Client: c}, nil
}

func generateRandomClientID(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "jaya-transport-service-" + hex.EncodeToString(bytes), nil
}

func DisconnectMQTTClient(c *autopaho.ConnectionManager) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := c.Disconnect(ctx)
	if err != nil {
		fmt.Printf("Error disconnecting MQTT client: %s\n", err)
	} else {
		log.Println("MQTT client disconnected successfully")
	}
}
