package internal

import (
	"context"
	"log"
	"strings"
	"time"
	"fmt"
	"sync/atomic"

	"medical-gas-transport-service/config"
	"medical-gas-transport-service/internal/services"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type Service struct {
	ctx             context.Context
	mqttClient      *services.MqttClient
	redisClient     *services.Redis
	jayaClient      *services.Jaya
	timescaleClient *services.TimescaleClient
	cfg             *config.Config
	messageChan     chan MqttMessage
	messageCount    atomic.Int64
}

func NewService(ctx context.Context, mqttClient *services.MqttClient, redisClient *services.Redis, jayaClient *services.Jaya, timescaleClient *services.TimescaleClient, cfg *config.Config) *Service {
	return &Service{
		ctx:             ctx,
		mqttClient:      mqttClient,
		redisClient:     redisClient,
		jayaClient:      jayaClient,
		timescaleClient: timescaleClient,
		cfg:             cfg,
		messageChan: make(chan MqttMessage, 1000),
	}
}

func (s *Service) Start() {
	s.subscribeToMQTT()
	s.addPublishHandler()
	s.startWorkerPool(10)

	go func() {
		ticker := time.NewTicker(time.Second * 15)
		for range ticker.C {
			count := s.messageCount.Swap(0)
			log.Printf("Messages processed per 15 second: %d", count)
		}
	}()
}

func (s *Service) subscribeToMQTT() {
	s.mqttClient.Client.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			// {Topic: "$share/g1/JI/v2/#", QoS: 0},
			{Topic: "$share/g1/provisioning", QoS: 0},
			{Topic: "$share/g1/JI/v2/+/level", QoS: 0},
			{Topic: "$share/g1/JI/v2/+/flow", QoS: 0},
			{Topic: "$share/g1/JI/v2/+/pressure", QoS: 0},
			{Topic: "$share/g1/JI/v2/+/filling", QoS: 0},
		},
	})
}

func (s *Service) addPublishHandler() {
	s.mqttClient.Client.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		s.messageChan <- MqttMessage{
			Topic:   pr.Packet.Topic,
			Payload: pr.Packet.Payload,
		}
		return true, nil
	})
}


func (s *Service) startWorkerPool(n int) {
	for i := 0; i < n; i++ {
		go s.processMessages()
	}
}

func (s *Service) processMessages() {
	for msg := range s.messageChan {
		s.messageCount.Add(1)

		topic := msg.Topic
		payload := msg.Payload

		switch {
			case topic == "provisioning":
				s.HandleProvisioning(payload)
			case strings.HasPrefix(topic, "JI/v2/") && strings.HasSuffix(topic, "/filling"):
				s.HandleFilling(topic, payload)
			case strings.HasPrefix(topic, "JI/v2/"):
				s.HandleSensorData(topic, payload)
			default:
				log.Printf("Unknown topic: %s", topic)
		}
	}
}

func extractSerialNumberFromTopic(topic string) (string, error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 4 {
		return "", fmt.Errorf("invalid topic format: %s", topic)
	}
	return parts[2], nil
}

func (s *Service) writeToTimescaleDB(query string, args ...interface{}) error {
	_, err := s.timescaleClient.DB.ExecContext(s.ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error writing data to TimescaleDB: %w", err)
	}
	return nil
}