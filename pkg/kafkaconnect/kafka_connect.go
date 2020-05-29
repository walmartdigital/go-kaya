package kafkaconnect

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/walmartdigital/go-kaya/pkg/client"

	// TODO: create an interface for this logging library
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("kafka-connect")

// ConnectorStatus ...
type ConnectorStatus struct {
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

// Status ...
type Status struct {
	Name      string          `json:"name"`
	Connector ConnectorStatus `json:"connector"`
	Tasks     []Task          `json:"tasks"`
}

// GetActiveTasksCount ...
func (s Status) GetActiveTasksCount() int {
	count := 0
	for _, t := range s.Tasks {
		if t.State == "RUNNING" {
			count++
		}
	}
	return count
}

// GetFailedTasksCount ...
func (s Status) GetFailedTasksCount() int {
	count := 0
	for _, t := range s.Tasks {
		if t.State == "FAILED" {
			count++
		}
	}
	return count
}

// Task ...
type Task struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace"`
}

// Connector ...
type Connector struct {
	Name   string           `json:"name"`
	Config *ConnectorConfig `json:"config"`
}

// ConnectorConfig ...
type ConnectorConfig struct {
	Name                         string `json:"name,omitempty" validate:"required,connectorname"`
	ConnectorClass               string `json:"connector.class" validate:"required,fqdn"`
	DocumentType                 string `json:"type.name" validate:"required,esdoctype"`
	Topics                       string `json:"topics" validate:"required,topiclist"`
	TopicIndexMap                string `json:"topic.index.map" validate:"required,topicindexmap"`
	BatchSize                    string `json:"batch.size" validate:"required,numeric"`
	ConnectionURL                string `json:"connection.url" validate:"required,url"`
	KeyIgnore                    string `json:"key.ignore" validate:"required,oneof=true false"`
	SchemaIgnore                 string `json:"schema.ignore,omitempty" validate:"required,oneof=true false"`
	Type                         string `json:"type,omitempty" validate:"omitempty,connectorname"`
	BehaviorOnMalformedDocuments string `json:"behavior.on.malformed.documents,omitempty"  validate:"required,oneof=ignore fail warn"`
}

// Response ...
type Response struct {
	Result  string      `json:"result"`
	Payload interface{} `json:"payload,omitempty"`
}

// Error ...
type Error struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

// Client ...
type Client struct {
	httpClient client.HTTPClient
}

// NewHTTPClient ...
func NewHTTPClient(url string, config client.HTTPClientConfig, factory client.HTTPClientFactory) (*client.HTTPClient, error) {
	c, err := initHTTPClient(url, config, factory)

	if err != nil {
		log.Error(err, "Error creating HTTP client")
		return nil, err
	}

	return &c, nil
}

func initHTTPClient(url string, config client.HTTPClientConfig, factory client.HTTPClientFactory) (client.HTTPClient, error) {
	if url == "" {
		return nil, errors.New("HTTP client base URL not provided")
	}

	client, err := factory.Create(url, config)

	if err != nil {
		return nil, err
	}

	return client, nil
}

// NewClient ...
func NewClient(kcHost string, config client.HTTPClientConfig, hcf client.HTTPClientFactory) (*Client, error) {
	k := new(Client)
	h, err := hcf.Create("http://"+kcHost, config)

	if err != nil {
		log.Error(err, "Error creating Kafka Connect client")
		return nil, err
	}

	k.httpClient = h
	return k, nil
}

// Create ...
func (kcc Client) Create(connector Connector) (*Response, error) {
	var kcError Error
	if govalidator.IsDNSName(connector.Name) {
		configBytes, err := json.Marshal(connector)
		if err != nil {
			return &Response{Result: "error"}, errors.New("Failed to serialize connector configuration")
		}
		status, body, err := kcc.httpClient.Post("/connectors", configBytes)
		if status == 201 {
			var createdConnector Connector
			err := json.Unmarshal(*body, &createdConnector)
			if err == nil {
				response := new(Response)
				response.Result = "success"
				response.Payload = createdConnector
				return response, nil
			}
			return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
		}
		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Create on Kafka Connect: %s", err.Error())
		}
		err = json.Unmarshal(*body, &kcError)
		if err == nil {
			response := new(Response)
			response.Result = "error"
			return response, fmt.Errorf("Received error from Kafka Connect (code:'%d', message:'%s')", kcError.ErrorCode, kcError.Message)
		}
		return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
	}
	return nil, errors.New("Malformed connector name")
}

// Read ...
func (kcc Client) Read(connector string) (*Response, error) {
	var config ConnectorConfig
	var kcError Error
	if govalidator.IsDNSName(connector) {
		status, body, err := kcc.httpClient.Get("/connectors/" + connector + "/config")
		if status == 200 {
			err := json.Unmarshal(*body, &config)
			if err == nil {
				response := new(Response)
				response.Result = "success"
				response.Payload = config
				return response, nil
			}
			return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
		}
		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Read on Kafka Connect: %s", err.Error())
		}
		err = json.Unmarshal(*body, &kcError)
		if err == nil {
			response := new(Response)
			response.Result = "error"
			return response, fmt.Errorf("Received error from Kafka Connect (code:'%d', message:'%s')", kcError.ErrorCode, kcError.Message)
		}
		return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
	}
	return nil, errors.New("Malformed connector name")
}

// Update ...
func (kcc Client) Update(connector Connector) (*Response, error) {
	var kcError Error
	if govalidator.IsDNSName(connector.Name) {
		status, body, err := kcc.httpClient.Get("/connectors/" + connector.Name)
		if status != 200 {
			return &Response{Result: "error"}, errors.New("Cannot update connector as it does not exist")
		}
		configBytes, err := json.Marshal(connector.Config)
		if err != nil {
			return &Response{Result: "error"}, errors.New("Failed to serialize connector configuration")
		}
		status, body, err = kcc.httpClient.Put("/connectors/"+connector.Name+"/config", configBytes)
		if status == 200 {
			var connector Connector
			err := json.Unmarshal(*body, &connector)
			if err == nil {
				response := new(Response)
				response.Result = "success"
				response.Payload = connector
				return response, nil
			}
			return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
		}
		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Update on Kafka Connect: %s", err.Error())
		}
		err = json.Unmarshal(*body, &kcError)
		if err == nil {
			response := new(Response)
			response.Result = "error"
			return response, fmt.Errorf("Received error from Kafka Connect (code:'%d', message:'%s')", kcError.ErrorCode, kcError.Message)
		}
		return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
	}
	return nil, errors.New("Malformed connector name")
}

// Delete ...
func (kcc Client) Delete(connector string) (*Response, error) {
	var kcError Error
	if govalidator.IsDNSName(connector) {
		status, body, err := kcc.httpClient.Delete("/connectors/" + connector)
		if status == 204 {
			response := new(Response)
			response.Result = "success"
			return response, nil
		}
		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Delete on Kafka Connect: %s", err.Error())
		}
		err = json.Unmarshal(*body, &kcError)
		if err == nil {
			response := new(Response)
			response.Result = "error"
			return response, fmt.Errorf("Received error from Kafka Connect (code:'%d', message:'%s')", kcError.ErrorCode, kcError.Message)
		}
		return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
	}
	return nil, errors.New("Malformed connector name")
}

// Status ...
func (kcc Client) Status(connector string) (*Response, error) {
	var kcError Error
	var connectorStatus Status
	if govalidator.IsDNSName(connector) {
		status, body, err := kcc.httpClient.Get("/connectors/" + connector + "/status")
		if status == 200 {
			err := json.Unmarshal(*body, &connectorStatus)
			if err == nil {
				response := new(Response)
				response.Result = "success"
				response.Payload = connectorStatus
				return response, nil
			}
			return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
		}
		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Status on Kafka Connect: %s", err.Error())
		}
		err = json.Unmarshal(*body, &kcError)
		if err == nil {
			response := new(Response)
			response.Result = "error"
			return response, fmt.Errorf("Received error from Kafka Connect (code:'%d', message:'%s')", kcError.ErrorCode, kcError.Message)
		}
		return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
	}
	return nil, errors.New("Malformed connector name")
}
