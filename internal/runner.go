package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"medical-gas-transport-service/config"
	"medical-gas-transport-service/internal/services"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	ctx             context.Context
	mqttClient      *services.MqttClient
	influxClient    *services.InfluxClient
	redisClient     *services.Redis
	jayaClient      *services.Jaya
	timescaleClient *services.TimescaleClient
	cfg             *config.Config
}

func NewService(ctx context.Context, mqttClient *services.MqttClient, influxClient *services.InfluxClient, redisClient *services.Redis, jayaClient *services.Jaya, timescaleClient *services.TimescaleClient, cfg *config.Config) *Service {
	return &Service{
		ctx:             ctx,
		mqttClient:      mqttClient,
		influxClient:    influxClient,
		redisClient:     redisClient,
		jayaClient:      jayaClient,
		timescaleClient: timescaleClient,
		cfg:             cfg,
	}
}

func (s *Service) Start() {
	s.subscribeToMQTT()
	s.addPublishHandler()
}

func (s *Service) subscribeToMQTT() {
	s.mqttClient.Client.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{Topic: "$share/g1/JI/v2/#", QoS: 1},
			{Topic: "$share/g1/provisioning", QoS: 1},
			{Topic: "$share/g1/JI/v2/+/level", QoS: 1},
			{Topic: "$share/g1/JI/v2/+/flow", QoS: 1},
		},
	})
}

func (s *Service) addPublishHandler() {
	s.mqttClient.Client.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		topic := pr.Packet.Topic
		payload := pr.Packet.Payload

		switch {
		case topic == "provisioning":
			s.handleProvisioning(payload)
		case strings.HasPrefix(topic, "JI/v2/") && strings.HasSuffix(topic, "/level"):
			s.handleOxygenLevel(topic, payload)
		case strings.HasPrefix(topic, "JI/v2/") && strings.HasSuffix(topic, "/flow"):
			s.handleOxygenFlow(topic, payload)
		default:
			s.handleDeviceData(topic, payload)
		}
		return true, nil
	})
}

func extractSerialNumberFromTopic(topic string) (string, error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 4 {
		return "", fmt.Errorf("invalid topic format: %s", topic)
	}
	return parts[2], nil
}

func (s *Service) handleOxygenLevel(topic string, payload []byte) {
	serialNumber, err := extractSerialNumberFromTopic(topic)
	if err != nil {
		log.Printf("Error extracting serial number: %v", err)
		return
	}

    device, err := s.getDeviceFromService(serialNumber)
    if err != nil {
        log.Printf("Error getting device info: %v", err)
    }

	var levelData OxygenLevelData
	if err := json.Unmarshal(payload, &levelData); err != nil {
		log.Printf("Error unmarshaling oxygen level data: %v", err)
		return
	}

	levelData.SerialNumber = serialNumber
	levelData.Timestamp = time.Unix(levelData.Ts, 0)

	if s.cfg.TimescaleDB.Enabled {
		query := `
			INSERT INTO sensor_level (
				time, serial_number, level, device_uptime, device_temp, 
				device_hum, device_long, device_lat, device_rssi, device_hw_ver, device_fw_ver, 
				device_rd_ver, device_model, solar_batt_temp, solar_batt_level, solar_batt_status, 
				solar_device_status, solar_load_status, solar_e_gen, solar_e_com,
				hospital_id, device_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
		`

        var hospitalID, deviceID string
        if device != nil {
			hospitalID = device.InstallationPointFlow.Hospital
            deviceID = device.ID
        }

		err = s.writeToTimescaleDB(query,
			levelData.Timestamp,
			levelData.SerialNumber,
			levelData.Level,
			levelData.Device.DeviceUptime,
			levelData.Device.DeviceTemp,
			levelData.Device.DeviceHum,
			levelData.Device.DeviceLong,
			levelData.Device.DeviceLat,
			levelData.Device.DeviceRSSI,
			levelData.Device.DeviceHWVer,
			levelData.Device.DeviceFWVer,
			levelData.Device.DeviceRDVer,
			levelData.Device.DeviceModel,
			levelData.Solar.SolarBattTemp,
			levelData.Solar.SolarBattLevel,
			pq.Array(levelData.Solar.SolarBattStatus),
			pq.Array(levelData.Solar.SolarDeviceStatus),
			pq.Array(levelData.Solar.SolarLoadStatus),
			pq.Array(levelData.Solar.SolarEGen),
			pq.Array(levelData.Solar.SolarECom),
			hospitalID,
			deviceID,
		)

		if err != nil {
			log.Printf("Error writing oxygen level data to TimescaleDB: %v", device)
			return
		}
	} else {
		tags := map[string]string{
			"serial_number": levelData.SerialNumber,
		}

		fields := map[string]interface{}{
			"level":            levelData.Level,
			"device_uptime":    levelData.Device.DeviceUptime,
			"device_temp":      levelData.Device.DeviceTemp,
			"device_hum":       levelData.Device.DeviceHum,
			"device_rssi":      levelData.Device.DeviceRSSI,
			"solar_batt_temp":  levelData.Solar.SolarBattTemp,
			"solar_batt_level": levelData.Solar.SolarBattLevel,
		}

		point := influxdb2.NewPoint("oxygen_level", tags, fields, levelData.Timestamp)
		s.writeToInfluxDB("oxygen_metrics", point)
	}

	event := map[string]interface{}{
		"level":  levelData,
	}
	eventJSON, _ := json.Marshal(event)
	s.redisClient.Rdb.Publish(s.ctx, "oxygen:updates", eventJSON)
	log.Println("Data processed and published:", levelData)

	log.Printf("Successfully stored oxygen level data for device %s", levelData.SerialNumber)
}

func (s *Service) handleOxygenFlow(topic string, payload []byte) {
	serialNumber, err := extractSerialNumberFromTopic(topic)
	if err != nil {
		log.Printf("Error extracting serial number: %v", err)
		return
	}

    device, err := s.getDeviceFromService(serialNumber)
    if err != nil {
        log.Printf("Error getting device info: %v", err)
	}

	var flowData OxygenFlowData
	if err := json.Unmarshal(payload, &flowData); err != nil {
		log.Printf("Error unmarshaling oxygen flow data: %v", err)
		return
	}

	flowData.SerialNumber = serialNumber
	flowData.Timestamp = time.Unix(flowData.Ts, 0)

	if s.cfg.TimescaleDB.Enabled {
		query := `
			INSERT INTO sensor_flow (
				time, serial_number, flow_rate, device_uptime, device_temp, 
				device_hum, device_long, device_lat, device_rssi, device_hw_ver, device_fw_ver, 
				device_rd_ver, device_model,
				hospital_id, device_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		`
		var hospitalID, deviceID string
        if device != nil {
            hospitalID = device.InstallationPointFlow.Hospital
            deviceID = device.ID
        }

		err = s.writeToTimescaleDB(query,
			flowData.Timestamp,
			flowData.SerialNumber,
			flowData.FlowRate,
			flowData.Device.DeviceUptime,
			flowData.Device.DeviceTemp,
			flowData.Device.DeviceHum,
			flowData.Device.DeviceLong,
			flowData.Device.DeviceLat,
			flowData.Device.DeviceRSSI,
			flowData.Device.DeviceHWVer,
			flowData.Device.DeviceFWVer,
			flowData.Device.DeviceRDVer,
			flowData.Device.DeviceModel,
			hospitalID,
			deviceID,
		)

		if err != nil {
			log.Printf("Error writing oxygen flow data to TimescaleDB: %v", err)
			return
		}
	} else {

		tags := map[string]string{
			"serial_number": flowData.SerialNumber,
		}

		fields := map[string]interface{}{
			"flow_rate":        flowData.FlowRate,
			"device_uptime":    flowData.Device.DeviceUptime,
			"device_temp":      flowData.Device.DeviceTemp,
			"device_hum":       flowData.Device.DeviceHum,
			"device_rssi":      flowData.Device.DeviceRSSI,
		}

		point := influxdb2.NewPoint("oxygen_flow", tags, fields, flowData.Timestamp)
		s.writeToInfluxDB("oxygen_metrics", point)
	}

	event := map[string]interface{}{
		"flow":  flowData,
	}
	eventJSON, _ := json.Marshal(event)
	s.redisClient.Rdb.Publish(s.ctx, "oxygen:updates", eventJSON)
	log.Println("Data processed and published:", flowData)

	log.Printf("Successfully stored oxygen flow data for device %s", flowData.SerialNumber)
}

func (s *Service) handleProvisioning(payload []byte) {
	var provisionRequest ProvisionRequest
	if err := json.Unmarshal(payload, &provisionRequest); err != nil {
		log.Printf("error unmarshaling JSON: %v\n", err)
		return
	}

	result, err := s.jayaClient.Provision(provisionRequest.SerialNumber)
	if err != nil {
		log.Printf("error provisioning device %s: %v", provisionRequest.SerialNumber, err)
		return
	}

	response := ProvisionResponse{
		Pattern: "provisioning/" + provisionRequest.SerialNumber + "/response",
		Data: ProvisionResponseData{
			Username: result.Username,
			Password: result.Password,
			Status:   result.Status,
		},
	}
	log.Printf("Received provisioning request from %s", provisionRequest.SerialNumber)
	if p, err := json.Marshal(response); err == nil {
		s.mqttClient.Client.Publish(s.ctx, &paho.Publish{
			Topic:   response.Pattern,
			QoS:     2,
			Payload: p,
		})
	} else {
		log.Printf("error building JSON: %v", err)
	}
}

func (s *Service) handleDeviceData(topic string, payload []byte) {
	t, err := extractTopic(topic)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	device, err := s.getDeviceFromCacheOrService(t.deviceId)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	switch t.subject {
	case "gatewayhealth", "nodehealth":
		s.handleHealthData(t, payload, device)
	default:
		s.handleNodeData(t, payload, device)
	}
}

func (s *Service) getDeviceFromCacheOrService(serialNumber string) (*services.Device, error) {
	result, err := s.redisClient.Rdb.Get(s.ctx, "device/"+serialNumber).Result()
	if err == redis.Nil {
		device, err := s.jayaClient.GetDevice(serialNumber)
		if err != nil {
			return nil, fmt.Errorf("error getting device from service: %w", err)
		}
		log.Printf("Device not found in cache, fetched from service: %s", serialNumber)

		if jsonDevice, err := json.Marshal(device); err == nil {
			s.redisClient.Rdb.Set(s.ctx, "device/"+serialNumber, jsonDevice, 3*time.Hour)
		}
		return device, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting device from Redis: %w", err)
	}

	var device services.Device
	if err := json.Unmarshal([]byte(result), &device); err != nil {
		return nil, fmt.Errorf("error parsing device JSON: %w", err)
	}

	return &device, nil
}

func (s *Service) getDeviceFromService(serialNumber string) (*services.Device, error) {
	device, err := s.jayaClient.GetDevice(serialNumber)
	if err != nil {
		return nil, fmt.Errorf("error getting device from service: %w", err)
	}
	return device, nil
}

func (s *Service) handleHealthData(t *eventTopic, payload []byte, device *services.Device) {
	var healthData DeviceHealth
	if err := json.Unmarshal(payload, &healthData); err != nil {
		log.Printf("error unmarshaling health data: %v", err)
		return
	}

	// device.Group["device"] = t.deviceId
	// device.Group["gateway"] = t.gatewayId
	fields := StructToMapReflect(healthData)
	delete(fields, "ts")

	if healthData.Modules != nil {
		for _, module := range healthData.Modules {
			fields[module.Name] = module.Status
		}
	}
	delete(fields, "modules")

	// point := influxdb2.NewPoint("deviceshealth", device.Group, fields, time.Unix(int64(healthData.Ts), 0))
	// s.writeToInfluxDB(device.Tenant.Name, point)

	log.Printf("Received device health data from %s", t.deviceId)
}

func (s *Service) handleNodeData(t *eventTopic, payload []byte, device *services.Device) {
	var nodeData NodeIOData
	if err := json.Unmarshal(payload, &nodeData); err != nil {
		log.Printf("Error unmarshaling node data: %v", err)
		return
	}

	// device.Group["device"] = t.deviceId
	// device.Group["gateway"] = t.gatewayId
	// fields, err := convertToMap(nodeData.Data)
	// if err != nil {
	// 	log.Printf("Error converting node data to map: %v", err)
	// 	return
	// }

	// point := influxdb2.NewPoint(device.Type, device.Group, fields, time.Unix(int64(nodeData.Ts), 0))
	// s.writeToInfluxDB(device.Tenant.Name, point)

	log.Printf("Received node data from %s", t.deviceId)
}

func (s *Service) writeToInfluxDB(bucket string, point *write.Point) {
	writeApi := s.influxClient.Client.WriteAPI(s.cfg.InfluxDB.Org, bucket)
	writeApi.WritePoint(point)
}

func (s *Service) writeToTimescaleDB(query string, args ...interface{}) error {
	_, err := s.timescaleClient.DB.ExecContext(s.ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error writing data to TimescaleDB: %w", err)
	}
	return nil
}
