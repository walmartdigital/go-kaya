package kafkaconnect

import (
	"github.com/walmartdigital/go-kaya/pkg/client"
)

// KafkaConnectClient ...
type KafkaConnectClient interface {
	Create(connector Connector) (*Response, error)
	Read(connector string) (*Response, error)
	Update(connector Connector) (*Response, error)
	Delete(connector string) (*Response, error)
}

// KafkaConnectClientFactory ...
type KafkaConnectClientFactory interface {
	Create(string, client.HTTPClientFactory) (KafkaConnectClient, error)
}
