package internal

import (
	"log"
	"fmt"
	"time"
	"strings"
	"encoding/json"
	
	"medical-gas-transport-service/internal/services"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func (s *Service) HandleSensorData(topic string, payload []byte) {
	switch {
		case strings.HasSuffix(topic, "/level"):
			s.handleSensorLevel(topic, payload)
		case strings.HasSuffix(topic, "/flow"):
			s.handleSensorFlow(topic, payload)
		case strings.HasSuffix(topic, "/pressure"):
			s.handleSensorPressure(topic, payload)
		default:
			log.Printf("Unknown topic: %s", topic)
	}	
}

func (s *Service) handleSensorLevel(topic string, payload []byte) {
	serialNumber, err := extractSerialNumberFromTopic(topic)
	if err != nil {
		log.Printf("Error extracting serial number: %v", err)
		return
	}

	device, err := s.getDeviceFromCacheOrService(serialNumber)
	if err != nil {
		log.Printf("Error getting device info: %v", err)
		return
	}
	
	if device == nil {
		log.Printf("Device not found for serial number: %s", serialNumber)
		return
	}		

	var levelData SensorLevelData
	if err := json.Unmarshal(payload, &levelData); err != nil {
		log.Printf("Error unmarshaling sensor level data: %v", err)
		return
	}

	if levelData.Level < 0 {
		log.Printf("Invalid level data: %v", levelData.Level)
		return
	}

	levelData.SerialNumber = serialNumber
	levelData.Timestamp = time.Unix(levelData.Ts, 0)

	if s.isDuplicateRecord("sensor_level", serialNumber, levelData.Timestamp) {
		log.Printf("Duplicate record detected for device %s at %v, skipping", serialNumber, levelData.Timestamp)
		return
	}

	if s.isDuplicateRecord("sensor_level", serialNumber, levelData.Timestamp) {
		log.Printf("Duplicate record detected for device %s at %v, skipping", serialNumber, levelData.Timestamp)
		return
	}

	conversionTable, err := s.getConversionTableWithCache(serialNumber)
	if err != nil {
		log.Printf("Error getting conversion table: %v", err)
		return
	}

	slope := 42.84814815
	intercept := -267.5185185
	kgToMetersCubics := 1.29

	for i := 0; i < len(conversionTable)-1; i++ {
		if (conversionTable[i].InH2OMin <= levelData.Level) && (levelData.Level <= conversionTable[i].InH2OMax) {
			slope = conversionTable[i].Slope
			intercept = conversionTable[i].Intercept
			break
		}
	}

	var LevelInKilograms, LevelInMetersCubics float64
	if levelData.Level == 0 {
		LevelInKilograms = 0
		LevelInMetersCubics = 0
	} else {
		LevelInKilograms = (levelData.Level * slope) + intercept
		LevelInMetersCubics = LevelInKilograms / kgToMetersCubics
	}

	query := `
		INSERT INTO sensor_level (
			time, serial_number, level, level_kg, level_meter_cubic, device_uptime, device_temp, 
			device_hum, device_long, device_lat, device_rssi, device_hw_ver, device_fw_ver, 
			device_rd_ver, device_model, device_mem_usage, device_reset_reason, solar_batt_temp, 
			solar_batt_level, solar_batt_volt, solar_batt_status, solar_device_status, 
			solar_load_status, solar_e_gen, solar_e_com
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
		ON CONFLICT (time, serial_number) DO NOTHING
	`

	err = s.writeToTimescaleDBWithRetry(query,
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
		levelData.Device.DeviceResetReason,
		levelData.Solar.SolarBattTemp,
		levelData.Solar.SolarBattLevel,
		levelData.Solar.SolarBattVolt,
		pq.Array(levelData.Solar.SolarBattStatus),
		pq.Array(levelData.Solar.SolarDeviceStatus),
		pq.Array(levelData.Solar.SolarLoadStatus),
		pq.Array(levelData.Solar.SolarEGen),
		pq.Array(levelData.Solar.SolarECom),
	)

	redisData := map[string]interface{}{
		"timestamp":        levelData.Timestamp,
		"serial_number":    levelData.SerialNumber,
		"level":           levelData.Level,
		"level_kg":        LevelInKilograms,
		"level_m3":        LevelInMetersCubics,
		"device_uptime":   levelData.Device.DeviceUptime,
		"device_temp":     levelData.Device.DeviceTemp,
		"device_hum":      levelData.Device.DeviceHum,
		"device_long":     levelData.Device.DeviceLong,
		"device_lat":      levelData.Device.DeviceLat,
		"device_rssi":     levelData.Device.DeviceRSSI,
		"device_hw_ver":   levelData.Device.DeviceHWVer,
		"device_fw_ver":   levelData.Device.DeviceFWVer,
		"device_rd_ver":   levelData.Device.DeviceRDVer,
		"device_model":    levelData.Device.DeviceModel,
		"device_mem_usage": levelData.Device.DeviceMemUsage,
		"device_reset_reason": levelData.Device.DeviceResetReason,
		"solar_batt_temp": levelData.Solar.SolarBattTemp,
		"solar_batt_level": levelData.Solar.SolarBattLevel,
		"solar_batt_volt": levelData.Solar.SolarBattVolt,
		"solar_batt_status": levelData.Solar.SolarBattStatus,
		"solar_device_status": levelData.Solar.SolarDeviceStatus,
		"solar_load_status": levelData.Solar.SolarLoadStatus,
		"solar_e_gen":     levelData.Solar.SolarEGen,
		"solar_e_com":     levelData.Solar.SolarECom,
	}

	if err != nil {
		log.Printf("Error writing sensor level data to TimescaleDB: %v", err)
		return
	}

	// Only publish if insert was successful
	event := map[string]interface{}{
		"serial_number"	: serialNumber,
		"data"					: redisData,
	}
	if eventJSON, err := json.Marshal(event); err == nil {
		s.redisClient.Rdb.Publish(s.ctx, "sensor:level", eventJSON)
		log.Printf("Successfully stored and published sensor level data for device %s", serialNumber)
	}
}

func (s *Service) handleSensorFlow(topic string, payload []byte) {
	serialNumber, err := extractSerialNumberFromTopic(topic)
	if err != nil {
		log.Printf("Error extracting serial number: %v", err)
		return
	}

	device, err := s.getDeviceFromCacheOrService(serialNumber)
	if err != nil {
		log.Printf("Error getting device info: %v", err)
		return
	}

	if device == nil {
		log.Printf("Device not found for serial number: %s", serialNumber)
		return
	}

	var flowData SensorFlowData
	if err := json.Unmarshal(payload, &flowData); err != nil {
		log.Printf("Error unmarshaling sensor flow data: %v", err)
		return
	}

	flowData.SerialNumber = serialNumber
	flowData.Timestamp = time.Unix(flowData.Ts, 0)

	// Check for duplicate before processing
	if s.isDuplicateRecord("sensor_flow", serialNumber, flowData.Timestamp) {
		log.Printf("Duplicate record detected for device %s at %v, skipping", serialNumber, flowData.Timestamp)
		return
	}

	totalVolume := (flowData.VHi * 65536) + (flowData.VLo) + (flowData.VDec / 1000)
	flowRate := ((flowData.FRateHi * 65536) + flowData.FRateLo) / 1000

	query := `
		INSERT INTO sensor_flow (
			time, serial_number, total_volume, volume_high, volume_low, volume_decimal,
			flow_rate, flow_rate_high, flow_rate_low, device_uptime, device_temp, 
			device_hum, device_long, device_lat, device_rssi, device_hw_ver, device_fw_ver, 
			device_rd_ver, device_model, device_reset_reason
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
		ON CONFLICT (time, serial_number) DO NOTHING
	`

	err = s.writeToTimescaleDBWithRetry(query,
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
		flowData.Device.DeviceResetReason,
	)

	if err != nil {
		log.Printf("Error writing sensor flow data to TimescaleDB: %v", err)
		return
	}

	// Only publish if insert was successful
	event := map[string]interface{}{
		"serial_number"	: serialNumber,
		"data"					: flowData,
	}
	if eventJSON, err := json.Marshal(event); err == nil {
		s.redisClient.Rdb.Publish(s.ctx, "sensor:flow", eventJSON)
		log.Printf("Successfully stored and published sensor flow data for device %s", serialNumber)
	}
}

func (s *Service) handleSensorPressure(topic string, payload []byte) {
	serialNumber, err := extractSerialNumberFromTopic(topic)
	if err != nil {
		log.Printf("Error extracting serial number: %v", err)
		return
	}

	device, err := s.getDeviceFromCacheOrService(serialNumber)
	if err != nil {
		log.Printf("Error getting device info: %v", err)
		return
	}

	if device == nil {
		log.Printf("Device not found for serial number: %s", serialNumber)
		return
	}

	var pressureData SensorPressureData
	if err := json.Unmarshal(payload, &pressureData); err != nil {
		log.Printf("Error unmarshaling sensor pressure data: %v", err)
		return
	}

	pressureData.SerialNumber = serialNumber
	pressureData.Timestamp = time.Unix(pressureData.Ts, 0)

	// Check for duplicate before processing
	if s.isDuplicateRecord("sensor_pressure", serialNumber, pressureData.Timestamp) {
		log.Printf("Duplicate record detected for device %s at %v, skipping", serialNumber, pressureData.Timestamp)
		return
	}

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
			device_rssi, device_hw_ver, device_fw_ver, device_rd_ver, device_model, device_reset_reason
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33)
		ON CONFLICT (time, serial_number) DO NOTHING
	`

	err = s.writeToTimescaleDBWithRetry(query,
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
		pressureData.Device.DeviceResetReason,
	)

	if err != nil {
		log.Printf("Error writing sensor pressure data to TimescaleDB: %v", err)
		return
	}

	// Only publish if insert was successful
	event := map[string]interface{}{
		"serial_number"	: serialNumber,
		"data"					: pressureData,
	}
	if eventJSON, err := json.Marshal(event); err == nil {
		s.redisClient.Rdb.Publish(s.ctx, "sensor:pressure", eventJSON)
		log.Printf("Successfully stored and published sensor pressure data for device %s", serialNumber)
	}
}

func (s *Service) getDeviceFromCacheOrService(serialNumber string) (*services.Device, error) {
	result, err := s.redisClient.Rdb.Get(s.ctx, "device/"+serialNumber).Result()
	if err == redis.Nil {
		device, err := s.jayaClient.GetDevice(serialNumber)
		if err != nil {
			log.Printf("Error getting device from service for serial %s: %v", serialNumber, err)
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

func (s *Service) getConversionTableWithCache(serialNumber string) ([]services.TankConversion, error) {
	cacheKey := "conversion_table/" + serialNumber
	result, err := s.redisClient.Rdb.Get(s.ctx, cacheKey).Result()
	if err == redis.Nil {
		table, err := s.jayaClient.GetConversionTable(serialNumber)
		if err != nil {
			return nil, fmt.Errorf("error getting conversion table from service: %w", err)
		}
		log.Printf("Conversion table not found in cache, fetched from service: %s", serialNumber)

		if jsonTable, err := json.Marshal(table); err == nil {
			s.redisClient.Rdb.Set(s.ctx, cacheKey, jsonTable, 3*time.Hour)
		}
		return table, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting conversion table from Redis: %w", err)
	}

	var table []services.TankConversion
	if err := json.Unmarshal([]byte(result), &table); err != nil {
		return nil, fmt.Errorf("error parsing conversion table JSON: %w", err)
	}

	return table, nil
}