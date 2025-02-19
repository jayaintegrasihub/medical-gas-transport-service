package main

import (
	"context"
	"fmt"
	"jaya-transport-service/config"
	"jaya-transport-service/internal"
	"jaya-transport-service/internal/services"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const LOGO = `
    ___  ________      ___    ___ ________     
   |\  \|\   __  \    |\  \  /  /|\   __  \    
   \ \  \ \  \|\  \   \ \  \/  / | \  \|\  \   
 __ \ \  \ \   __  \   \ \    / / \ \   __  \  
|\  \\_\  \ \  \ \  \   \/  /  /   \ \  \ \  \ 
\ \________\ \__\ \__\__/  / /      \ \__\ \__\
 \|________|\|__|\|__|\___/ /        \|__|\|__|
                     \|___|/                                                            

`

const SERVICENAME = "Jaya Transport Service"
const VERSION = "v2.0.0"

func main() {
	fmt.Print(LOGO + SERVICENAME + " " + VERSION + "\n\n")

	// Load the configuration
	cfg := config.LoadConfig()

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling to gracefully shut down on interrupt
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	// Create MQTT client
	log.Printf("Setup MQTT Service")
	mqttClient, err := services.NewMqttClient(ctx, cfg.MQTT)
	if err != nil {
		log.Fatalf("Error creating MQTT client: %v", err)
	}

	// Create InfluxDB client
	log.Printf("Setup Influxdb Service")
	influxClient, err := services.NewInfluxClient(ctx, cfg.InfluxDB)
	if err != nil {
		log.Fatalf("Error creating InfluxDB client: %v", err)
	}

	// Create Redis client
	log.Printf("Setup Redis Service")
	redisClient, err := services.NewRedisClient(cfg.Redis)
	if err != nil {
		log.Fatalf("Error creating Redis client: %v", err)
	}

	// Create Jaya client
	log.Printf("Setup Jaya Service")
	jayaClient := services.NewJayaService(cfg.JayaApi)

	// Create Timescaledb client
	log.Printf("Setup Timescaledb Service")
	timescaleClient, err := services.NewTimescaleClient(ctx, cfg.TimescaleDB)
	if err != nil {
		log.Fatalf("Error creating Timescaledb client: %v", err)
	}

	// Start the service
	svc := internal.NewService(ctx, mqttClient, influxClient, redisClient, jayaClient, timescaleClient, cfg)
	svc.Start()

	log.Println("Service started. Waiting for shutdown signal.")

	// Wait for context cancellation
	<-ctx.Done()
	services.DisconnectMQTTClient(mqttClient.Client)
	log.Println("Shutting down service...")
}
