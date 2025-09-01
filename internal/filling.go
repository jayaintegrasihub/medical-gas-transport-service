
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
	var flag string

	fillingData.SerialNumber = serialNumber
	fillingData.Timestamp = time.Unix(fillingData.Ts, 0)
	fillingData.State = fillingData.FillingState == 1

	conversionTable, err := s.getConversionTableWithCache(serialNumber)
	if err != nil {
		log.Printf("Error getting conversion table: %v", err)
		return
	}

	if len(conversionTable) == 0 {
		log.Printf("Error empty conversion table for device %s", serialNumber)
		return
	}

	slope := 42.84814815
	intercept := -267.5185185
	kgToMetersCubics := 0.777

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
		LevelInMetersCubics = LevelInKilograms * kgToMetersCubics
	}

	var NanoID string

	if fillingData.NanoID != "" {
		NanoID = fillingData.NanoID
		skipWrite = false
	} else {
		if fillingData.State {
			var generateErr error
			maxRetries := 3
			for retry := 0; retry < maxRetries; retry++ {
				NanoID, generateErr = Generate(fillingData.Timestamp, serialNumber)
				if generateErr == nil {
					break
				}
				log.Printf("Retry %d: Error generating NanoID for device %s: %v", retry+1, serialNumber, generateErr)
				time.Sleep(time.Millisecond * 100)
			}
			
			if generateErr != nil {
				log.Printf("Error generating NanoID after %d retries for device %s: %v", maxRetries, serialNumber, generateErr)
				return
			}

			if NanoID == "" {
				log.Printf("Error generated NanoID is empty for device %s", serialNumber)
				return
			}

			skipWrite = false
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
		if fillingData.State {
			existingQuery := `
				SELECT nano_id FROM filling_transaction 
				WHERE serial_number = $1 AND state = true AND flag = 'unclosed'
				AND nano_id NOT IN (
					SELECT nano_id FROM filling_transaction 
					WHERE serial_number = $1 AND state = false
				)
			`
			
			rows, err := s.timescaleClient.DB.QueryContext(s.ctx, existingQuery, serialNumber)
			if err != nil {
				log.Printf("Error checking existing open transactions for device %s: %v", serialNumber, err)
				responsePayload = FillingResponsePayload{
					Status:    "fail",
					Timestamp: fillingData.Ts,
					NanoID:    NanoID,
				}
				skipWrite = true
			} else {
				var existingNanoIDs []string
				for rows.Next() {
					var nanoID string
					if err := rows.Scan(&nanoID); err != nil {
						log.Printf("Error scanning nano_id: %v", err)
						continue
					}
					existingNanoIDs = append(existingNanoIDs, nanoID)
				}
				rows.Close()
				
				if len(existingNanoIDs) > 0 {
					updateQuery := `
						UPDATE filling_transaction 
						SET flag = 'invalid' 
						WHERE serial_number = $1 AND state = true AND flag = 'unclosed'
						AND nano_id NOT IN (
							SELECT nano_id FROM filling_transaction 
							WHERE serial_number = $1 AND state = false
						)
					`
					_, err := s.timescaleClient.DB.ExecContext(s.ctx, updateQuery, serialNumber)
					if err != nil {
						log.Printf("Error marking existing unclosed transactions as invalid for device %s: %v", serialNumber, err)
						responsePayload = FillingResponsePayload{
							Status:    "fail",
							Timestamp: fillingData.Ts,
							NanoID:    NanoID,
						}
						skipWrite = true
					} else {
						log.Printf("Marked %d existing unclosed transactions as invalid for device %s (duplicate case)", len(existingNanoIDs), serialNumber)
					}
				}
			}

			flag = "unclosed"
		} else {
			// For false state, check if there's an active transaction with this nano_id
			activeQuery := `
				SELECT COUNT(*) FROM filling_transaction 
				WHERE serial_number = $1 AND nano_id = $2 AND state = true AND flag = 'unclosed'
			`
			var activeCount int
			err := s.timescaleClient.DB.QueryRowContext(s.ctx, activeQuery, serialNumber, NanoID).Scan(&activeCount)
			if err != nil {
				log.Printf("Error checking active transaction for device %s: %v", serialNumber, err)
				responsePayload = FillingResponsePayload{
					Status:    "fail",
					Timestamp: fillingData.Ts,
					NanoID:    NanoID,
				}
				skipWrite = true
			} else if activeCount == 0 {
				log.Printf("No active transaction found for nano_id %s on device %s, cannot close", NanoID, serialNumber)
				responsePayload = FillingResponsePayload{
					Status:    "fail",
					Timestamp: fillingData.Ts,
					NanoID:    NanoID,
				}
				skipWrite = true
			}
			
			if !skipWrite {
				existsQuery := `
					SELECT COUNT(*) FROM filling_transaction 
					WHERE serial_number = $1 AND nano_id = $2 AND state = false
				`
				var count int
				err := s.timescaleClient.DB.QueryRowContext(s.ctx, existsQuery, serialNumber, NanoID).Scan(&count)
				if err != nil {
					log.Printf("Error checking existing data for device %s: %v", serialNumber, err)
					responsePayload = FillingResponsePayload{
						Status:    "fail",
						Timestamp: fillingData.Ts,
						NanoID:    NanoID,
					}
					skipWrite = true
				} else if count > 0 {
					log.Printf("Duplicate closing transaction for nano_id %s on device %s, skipping", NanoID, serialNumber)
					responsePayload = FillingResponsePayload{
						Status:    "fail",
						Timestamp: fillingData.Ts,
						NanoID:    NanoID,
					}
					skipWrite = true
				} else {
					updateQuery := `
						UPDATE filling_transaction 
						SET flag = 'closed' 
						WHERE serial_number = $1 AND nano_id = $2 AND state = true AND flag = 'unclosed'
					`
					_, err := s.timescaleClient.DB.ExecContext(s.ctx, updateQuery, serialNumber, NanoID)
					if err != nil {
						log.Printf("Error updating open transaction flag to closed for device %s: %v", serialNumber, err)
						responsePayload = FillingResponsePayload{
							Status:    "fail",
							Timestamp: fillingData.Ts,
							NanoID:    NanoID,
						}
						skipWrite = true
					} else {
						log.Printf("Updated open transaction flag to 'closed' for nano_id %s on device %s", NanoID, serialNumber)
					}
				}
			}

			flag = ""
		}
	}
	
	if !skipWrite {
		if fillingData.State {
			query := `
				INSERT INTO filling_transaction (
					time, serial_number, nano_id, level, level_kg, level_meter_cubic,
					state, flag
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`
			
			err = s.writeToTimescaleDB(query,
				fillingData.Timestamp,
				fillingData.SerialNumber,
				NanoID,
				fillingData.Level,
				LevelInKilograms,
				LevelInMetersCubics,
				fillingData.State,
				flag,
			)
		} else {
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
		}
		
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
			log.Printf("Successfully stored filling data for device %s", fillingData.SerialNumber)
		}

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
		log.Printf("Error marshaling response JSON for device %s: %v", serialNumber, err)
	}

}

func Generate(timestamp time.Time, serialNumber string) (string, error) {
	alphabet := fmt.Sprintf("%s%s", timestamp.Format("20060102150405"), serialNumber)
	return nanoid.Generate(alphabet, 12)
}