package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
	"sync/atomic"

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
	messageChan 	chan MqttMessage
	messageCount 	atomic.Int64
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
		messageChan: make(chan MqttMessage, 1000),
	}
}

func (s *Service) Start() {
	s.subscribeToMQTT()
	s.addPublishHandler()
	s.startWorkerPool(5)

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
			s.handleProvisioning(payload)
		case strings.HasPrefix(topic, "JI/v2/") && strings.HasSuffix(topic, "/level"):
			s.handleSensorLevel(topic, payload)
		case strings.HasPrefix(topic, "JI/v2/") && strings.HasSuffix(topic, "/flow"):
			s.handleSensorFlow(topic, payload)
		case strings.HasPrefix(topic, "JI/v2/") && strings.HasSuffix(topic, "/pressure"):
			s.handleSensorPressure(topic, payload)
		default:
			s.handleDeviceData(topic, payload)
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

func (s *Service) handleSensorLevel(topic string, payload []byte) {
	serialNumber, err := extractSerialNumberFromTopic(topic)
	if err != nil {
		log.Printf("Error extracting serial number: %v", err)
		return
	}

	device, err := s.getDeviceFromService(serialNumber)
	if err != nil {
		log.Printf("Error getting device info: %v", err)
	}

	var levelData SensorLevelData
	if err := json.Unmarshal(payload, &levelData); err != nil {
		log.Printf("Error unmarshaling sensor level data: %v", err)
		return
	}

	levelData.SerialNumber = serialNumber
	levelData.Timestamp = time.Unix(levelData.Ts, 0)

	const slope = 42.84814815 // slope of the linear equation
	const intercept = -267.5185185 // intercept of the linear equation
	const kgToMetersCubics = 0.001 // conversion factor from kg to cubic meters
	// formula = y = mx + c

	var LevelInKilograms, LevelInMetersCubics float64
	if levelData.Level == 0 {
		LevelInKilograms = 0
		LevelInMetersCubics = 0
	} else {
		LevelInKilograms = (levelData.Level * slope) + intercept
		LevelInMetersCubics = LevelInKilograms * kgToMetersCubics
	}

	if s.cfg.TimescaleDB.Enabled {
		query := `
			INSERT INTO sensor_level (
				time, serial_number, level, level_kg, level_meter_cubic, device_uptime, device_temp, 
				device_hum, device_long, device_lat, device_rssi, device_hw_ver, device_fw_ver, 
				device_rd_ver, device_model, device_mem_usage, solar_batt_temp, solar_batt_level, solar_batt_volt, solar_batt_status, 
				solar_device_status, solar_load_status, solar_e_gen, solar_e_com,
				hospital_id, device_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
		`

		var hospitalID, deviceID string
		if device != nil {
			hospitalID = device.InstallationPointTank.Hospital
			deviceID = device.ID
		}

		err = s.writeToTimescaleDB(query,
			levelData.Timestamp,
			levelData.SerialNumber,
			levelData.Level,
			LevelInKilograms,
			LevelInMetersCubics,
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
			levelData.Device.DeviceMemUsage,
			levelData.Solar.SolarBattTemp,
			levelData.Solar.SolarBattLevel,
			levelData.Solar.SolarBattVolt,
			pq.Array(levelData.Solar.SolarBattStatus),
			pq.Array(levelData.Solar.SolarDeviceStatus),
			pq.Array(levelData.Solar.SolarLoadStatus),
			pq.Array(levelData.Solar.SolarEGen),
			pq.Array(levelData.Solar.SolarECom),
			hospitalID,
			deviceID,
		)

		if err != nil {
			log.Printf("Error writing sensor level data to TimescaleDB: %v", err)
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
			"device_mem_usage": levelData.Device.DeviceMemUsage,
			"solar_batt_temp":  levelData.Solar.SolarBattTemp,
			"solar_batt_level": levelData.Solar.SolarBattLevel,
			"solar_batt_volt": levelData.Solar.SolarBattVolt,
		}

		point := influxdb2.NewPoint("oxygen_level", tags, fields, levelData.Timestamp)
		s.writeToInfluxDB("oxygen_metrics", point)
	}

	event := map[string]interface{}{
		"level": levelData,
	}
	eventJSON, _ := json.Marshal(event)
	s.redisClient.Rdb.Publish(s.ctx, "oxygen:updates", eventJSON)
	log.Println("Data processed and published:", levelData)

	log.Printf("Successfully stored sensor level data for device %s", levelData.SerialNumber)
}

func (s *Service) handleSensorFlow(topic string, payload []byte) {
	serialNumber, err := extractSerialNumberFromTopic(topic)
	if err != nil {
		log.Printf("Error extracting serial number: %v", err)
		return
	}

	device, err := s.getDeviceFromService(serialNumber)
	if err != nil {
		log.Printf("Error getting device info: %v", err)
	}

	var flowData SensorFlowData
	if err := json.Unmarshal(payload, &flowData); err != nil {
		log.Printf("Error unmarshaling sensor flow data: %v", err)
		return
	}

	flowData.SerialNumber = serialNumber
	flowData.Timestamp = time.Unix(flowData.Ts, 0)

	totalVolume := (flowData.VHi * 65536) + (flowData.VLo) + (flowData.VDec / 1000)
	flowRate := ((flowData.FRateHi * 65536) + flowData.FRateLo) / 1000

	if s.cfg.TimescaleDB.Enabled {
		query := `
			INSERT INTO sensor_flow (
				time, serial_number, total_volume, volume_high, volume_low, volume_decimal,
				flow_rate, flow_rate_high, flow_rate_low, device_uptime, device_temp, 
				device_hum, device_long, device_lat, device_rssi, device_hw_ver, device_fw_ver, 
				device_rd_ver, device_model, hospital_id, device_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		`
		var hospitalID, deviceID string
		if device != nil {
			hospitalID = device.InstallationPointFlow.Hospital
			deviceID = device.ID
		}

		err = s.writeToTimescaleDB(query,
			flowData.Timestamp,
			flowData.SerialNumber,
			totalVolume,
			flowData.VHi,
			flowData.VLo,
			flowData.VDec,
			flowRate,
			flowData.FRateHi,
			flowData.FRateLo,
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
			log.Printf("Error writing sensor flow data to TimescaleDB: %v", err)
			return
		}
	} else {

		tags := map[string]string{
			"serial_number": flowData.SerialNumber,
		}

		fields := map[string]interface{}{
			"total_volume":   totalVolume,
			"volume_high":    flowData.VHi,
			"volume_low":     flowData.VLo,
			"volume_decimal": flowData.VDec,
			"flow_rate":      flowRate,
			"flow_rate_high": flowData.FRateHi,
			"flow_rate_low":  flowData.FRateLo,
			"device_uptime":  flowData.Device.DeviceUptime,
			"device_temp":    flowData.Device.DeviceTemp,
			"device_hum":     flowData.Device.DeviceHum,
			"device_rssi":    flowData.Device.DeviceRSSI,
		}

		point := influxdb2.NewPoint("oxygen_flow", tags, fields, flowData.Timestamp)
		s.writeToInfluxDB("oxygen_metrics", point)
	}

	event := map[string]interface{}{
		"flow": flowData,
	}
	eventJSON, _ := json.Marshal(event)
	s.redisClient.Rdb.Publish(s.ctx, "oxygen:updates", eventJSON)
	log.Println("Data processed and published:", flowData)

	log.Printf("Successfully stored sensor flow data for device %s", flowData.SerialNumber)
}

func (s *Service) handleSensorPressure(topic string, payload []byte) {
	serialNumber, err := extractSerialNumberFromTopic(topic)
	if err != nil {
		log.Printf("Error extracting serial number: %v", err)
		return
	}

	device, err := s.getDeviceFromService(serialNumber)
	if err != nil {
		log.Printf("Error getting device info: %v", err)
	}

	var pressureData SensorPressureData
	if err := json.Unmarshal(payload, &pressureData); err != nil {
		log.Printf("Error unmarshaling sensor pressure data: %v", err)
		return
	}

	pressureData.SerialNumber = serialNumber
	pressureData.Timestamp = time.Unix(pressureData.Ts, 0)

	if s.cfg.TimescaleDB.Enabled {
		var (
			nitrousOxidePressure, nitrousOxideHighLimit, nitrousOxideLowLimit                float64
			oxygenPressure, oxygenHighLimit, oxygenLowLimit                                  float64
			medicalAirPressure, medicalAirHighLimit, medicalAirLowLimit                      float64
			vacuumPressure, vacuumHighLimit, vacuumLowLimit                                  float64
			nitrousOxideConnection, oxygenConnection, medicalAirConnection, vacuumConnection int
			nitrousOxideEnable, oxygenEnable, medicalAirEnable, vacuumEnable                 bool
		)

		for _, data := range pressureData.Data {
			switch data.Measurement {
			case "nitrous oxide":
				nitrousOxidePressure = data.Value
				nitrousOxideConnection = data.Connection
				nitrousOxideEnable = data.Enable
				nitrousOxideHighLimit = data.HighLimit
				nitrousOxideLowLimit = data.LowLimit
			case "oxygen":
				oxygenPressure = data.Value
				oxygenConnection = data.Connection
				oxygenEnable = data.Enable
				oxygenHighLimit = data.HighLimit
				oxygenLowLimit = data.LowLimit
			case "medical air":
				medicalAirPressure = data.Value
				medicalAirConnection = data.Connection
				medicalAirEnable = data.Enable
				medicalAirHighLimit = data.HighLimit
				medicalAirLowLimit = data.LowLimit
			case "vacuum":
				vacuumPressure = data.Value
				vacuumConnection = data.Connection
				vacuumEnable = data.Enable
				vacuumHighLimit = data.HighLimit
				vacuumLowLimit = data.LowLimit
			}
		}

		query := `
			INSERT INTO sensor_pressure (
				time, serial_number, 
				nitrous_oxide_value, nitrous_oxide_connection, nitrous_oxide_enable, 
				nitrous_oxide_high_limit, nitrous_oxide_low_limit,
				oxygen_value, oxygen_connection, oxygen_enable, 
				oxygen_high_limit, oxygen_low_limit,
				medical_air_value, medical_air_connection, medical_air_enable, 
				medical_air_high_limit, medical_air_low_limit,
				vacuum_value, vacuum_connection, vacuum_enable, 
				vacuum_high_limit, vacuum_low_limit,
				device_uptime, device_temp, device_hum, device_long, device_lat, 
				device_rssi, device_hw_ver, device_fw_ver, device_rd_ver, device_model,
				hospital_id, device_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34)
		`

		var hospitalID, deviceID string
		if device != nil {
			hospitalID = device.InstallationPointPressure.Hospital
			deviceID = device.ID
		}

		err = s.writeToTimescaleDB(query,
			pressureData.Timestamp,
			pressureData.SerialNumber,
			nitrousOxidePressure,
			nitrousOxideConnection,
			nitrousOxideEnable,
			nitrousOxideHighLimit,
			nitrousOxideLowLimit,
			oxygenPressure,
			oxygenConnection,
			oxygenEnable,
			oxygenHighLimit,
			oxygenLowLimit,
			medicalAirPressure,
			medicalAirConnection,
			medicalAirEnable,
			medicalAirHighLimit,
			medicalAirLowLimit,
			vacuumPressure,
			vacuumConnection,
			vacuumEnable,
			vacuumHighLimit,
			vacuumLowLimit,
			pressureData.Device.DeviceUptime,
			pressureData.Device.DeviceTemp,
			pressureData.Device.DeviceHum,
			pressureData.Device.DeviceLong,
			pressureData.Device.DeviceLat,
			pressureData.Device.DeviceRSSI,
			pressureData.Device.DeviceHWVer,
			pressureData.Device.DeviceFWVer,
			pressureData.Device.DeviceRDVer,
			pressureData.Device.DeviceModel,
			hospitalID,
			deviceID,
		)

		if err != nil {
			log.Printf("Error writing sensor pressure data to TimescaleDB: %v", err)
			return
		}
	} else {
		tags := map[string]string{
			"serial_number": pressureData.SerialNumber,
		}

		fields := map[string]interface{}{
			"device_uptime": pressureData.Device.DeviceUptime,
			"device_temp":   pressureData.Device.DeviceTemp,
			"device_hum":    pressureData.Device.DeviceHum,
			"device_rssi":   pressureData.Device.DeviceRSSI,
		}

		// Add pressure values for each gas type
		for _, data := range pressureData.Data {
			gasType := strings.ReplaceAll(data.Measurement, " ", "_")
			fields[gasType+"_pressure"] = data.Value
			fields[gasType+"_connection"] = data.Connection
			fields[gasType+"_enable"] = data.Enable
			fields[gasType+"_high_limit"] = data.HighLimit
			fields[gasType+"_low_limit"] = data.LowLimit
		}

		point := influxdb2.NewPoint("oxygen_pressure", tags, fields, pressureData.Timestamp)
		s.writeToInfluxDB("oxygen_metrics", point)
	}

	event := map[string]interface{}{
		"pressure": pressureData,
	}
	eventJSON, _ := json.Marshal(event)
	s.redisClient.Rdb.Publish(s.ctx, "oxygen:updates", eventJSON)
	log.Println("Pressure data processed and published:", pressureData)

	log.Printf("Successfully stored sensor pressure data for device %s", pressureData.SerialNumber)
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
