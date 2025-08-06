package internal
import (
	"encoding/json"
	"log"

	"github.com/eclipse/paho.golang/paho"
)

func (s *Service) HandleProvisioning(payload []byte) {
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