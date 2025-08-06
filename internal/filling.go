
package internal

import (
	"log"
	"fmt"
	"time"
	"encoding/json"

	"github.com/eclipse/paho.golang/paho"
	nanoid "github.com/matoous/go-nanoid/v2"
)

func (s *Service) HandleFilling(topic string, payload []byte) {
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

	var fillingData FillingPayload
	if err := json.Unmarshal(payload, &fillingData); err != nil {
		log.Printf("Error parsing filling payload: %v", err)
		return
	}

	var responsePayload FillingResponsePayload
	var skipWrite bool

	fillingData.SerialNumber = serialNumber
	fillingData.Timestamp = time.Unix(fillingData.Ts, 0)
	fillingData.State = fillingData.FillingState == 1

	conversionTable, err := s.getConversionTableWithCache(serialNumber)
	if err != nil {
		log.Printf("Error getting conversion table: %v", err)
		return
	}

	slope := 42.84814815
	intercept := -267.5185185
	kgToMetersCubics := 1.29

	for i := 0; i < len(conversionTable)-1; i++ {
		if (conversionTable[i].InH2OMin <= fillingData.Level) && (fillingData.Level <= conversionTable[i].InH2OMax) {
			slope = conversionTable[i].Slope
			intercept = conversionTable[i].Intercept
			break
		}
	}

	var LevelInKilograms, LevelInMetersCubics float64
	if fillingData.Level == 0 {
		LevelInKilograms = 0
		LevelInMetersCubics = 0
	} else {
		LevelInKilograms = (fillingData.Level * slope) + intercept
		LevelInMetersCubics = LevelInKilograms / kgToMetersCubics
	}

	var NanoID string

	if fillingData.NanoID != "" {
		NanoID = fillingData.NanoID
		skipWrite = false
	} else {
		if fillingData.State {
			NanoID, err = Generate(fillingData.Timestamp, serialNumber)
			skipWrite = false
			if err != nil {
				log.Printf("Error generating NanoID: %v", err)
				return
			}
		} else {
			responsePayload = FillingResponsePayload{
				Status:    "fail",
				Timestamp: fillingData.Ts,
				NanoID:    NanoID,
			}
			skipWrite = true
			log.Printf("Filling state is false and NanoID is empty, skipping write for device %s", serialNumber)
		}
	}
	
	if !skipWrite {
		query := `
			INSERT INTO filling_transaction (
				time, serial_number, nano_id, level, level_kg, level_meter_cubic,
				state 
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`
		
		err = s.writeToTimescaleDB(query,
			fillingData.Timestamp,
			fillingData.SerialNumber,
			NanoID,
			fillingData.Level,
			LevelInKilograms,
			LevelInMetersCubics,
			fillingData.State,
		)
		
		if err != nil {
			responsePayload = FillingResponsePayload{
				Status:    "fail",
				Timestamp: fillingData.Ts,
				NanoID:    NanoID,
			}
			log.Printf("Error writing sensor level data to TimescaleDB: %v", err)
		} else {
			responsePayload = FillingResponsePayload{
				Status:    "success",
				Timestamp: fillingData.Ts,
				NanoID:    NanoID,
			}
		}

		log.Printf("Successfully stored filling data for device %s", fillingData.SerialNumber)
	} else {
		log.Printf("Skipping write to TimescaleDB for device %s", serialNumber)
	}

	responseTopic := fmt.Sprintf("JI/v2/%s/filling-response", serialNumber)

	if response, err := json.Marshal(responsePayload); err == nil {
		s.mqttClient.Client.Publish(s.ctx, &paho.Publish{
			Topic:   responseTopic,
			QoS:     2,
			Payload: response,
		})
	} else {
		log.Printf("Error building JSON: %v", err)
	}

}

func Generate(timestamp time.Time, serialNumber string) (string, error) {
	alphabet := fmt.Sprintf("%s%s", timestamp.Format("20060102150405"), serialNumber)
	return nanoid.Generate(alphabet, 12)
}