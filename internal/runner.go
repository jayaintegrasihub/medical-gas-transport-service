package internal

import (
	"context"
	"log"
	"strings"
	"time"
	"fmt"
	"sync/atomic"
	"github.com/lib/pq"

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

func (s *Service) writeToTimescaleDBWithRetry(query string, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	_, err := s.timescaleClient.DB.ExecContext(ctx, query, args...)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok {
			switch pqErr.Code {
			case "23505":
				log.Printf("Duplicate key detected, skipping insert: %v", pqErr.Detail)
				return nil
			case "23503":
				log.Printf("Foreign key violation: %v", pqErr.Detail)
				return fmt.Errorf("foreign key violation: %w", err)
			case "23502":
				log.Printf("Not null violation: %v", pqErr.Detail)
				return fmt.Errorf("not null violation: %w", err)
			case "23514":
				log.Printf("Check constraint violation: %v", pqErr.Detail)
				return fmt.Errorf("check constraint violation: %w", err)
			}
		}
		
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("Database query timeout: %v", err)
			return fmt.Errorf("database query timeout: %w", err)
		}
		
		return fmt.Errorf("database error: %w", err)
	}
	
	return nil
}

func (s *Service) isDuplicateRecord(tableName, serialNumber string, timestamp time.Time) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE time = $1 AND serial_number = $2 LIMIT 1", tableName)
	var exists int
	err := s.timescaleClient.DB.QueryRowContext(ctx, query, timestamp, serialNumber).Scan(&exists)
	
	return err == nil && exists == 1
}