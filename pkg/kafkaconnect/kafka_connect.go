package kafkaconnect

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/asaskevich/govalidator"
	"github.com/google/go-cmp/cmp"
	"github.com/walmartdigital/go-kaya/pkg/client"
	"github.com/walmartdigital/go-kaya/pkg/utils/types"
	"go.uber.org/zap"

	// TODO: create an interface for this logging library
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("kafka-connect")

// ConnectorConfigComparer ...
var ConnectorConfigComparer cmp.Option

func init() {
	ConnectorConfigComparer = cmp.Comparer(func(a, b ConnectorConfig) bool {
		if (a == ConnectorConfig{}) && (b == ConnectorConfig{}) {
			return true
		} else if (a != ConnectorConfig{}) && (b != ConnectorConfig{}) {
			return a.Name == b.Name &&
				a.ConnectorClass == b.ConnectorClass &&
				a.DocumentType == b.DocumentType &&
				a.Topics == b.Topics &&
				a.TopicIndexMap == b.TopicIndexMap &&
				a.BatchSize == b.BatchSize &&
				a.ConnectionURL == b.ConnectionURL &&
				a.KeyIgnore == b.KeyIgnore &&
				a.SchemaIgnore == b.SchemaIgnore &&
				a.BehaviorOnMalformedDocuments == b.BehaviorOnMalformedDocuments &&
				a.ConnectionUsername == b.ConnectionUsername &&
				a.ConnectionPassword == b.ConnectionPassword &&
				a.Type == b.Type &&
				a.MaxInFlightRequests == b.MaxInFlightRequests &&
				a.MaxBufferedRecords == b.MaxBufferedRecords &&
				a.LingerMs == b.LingerMs &&
				a.FlushTimeoutMs == b.FlushTimeoutMs &&
				a.MaxRetries == b.MaxRetries &&
				a.RetryBackoffMs == b.RetryBackoffMs &&
				a.ConnectionCompression == b.ConnectionCompression &&
				a.ConnectionTimeoutMs == b.ConnectionTimeoutMs &&
				a.ReadTimeoutMs == b.ReadTimeoutMs &&
				a.TasksMax == b.TasksMax &&
				a.OffsetFlushTimeoutMs == b.OffsetFlushTimeoutMs &&
				a.HeartbeatIntervalMs == b.HeartbeatIntervalMs &&
				a.ValueConverterSchemasEnable == b.ValueConverterSchemasEnable &&
				a.ValueConverter == b.ValueConverter

		}
		return false
	})
}

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

// GetFailedTasks ...
func (s Status) GetFailedTasks() []int {
	var failed []int
	for i, t := range s.Tasks {
		if t.State == "FAILED" {
			failed = append(failed, i)
		}
	}
	return failed
}

// GetTaskCount ...
func (s Status) GetTaskCount() int {
	return len(s.Tasks)
}

// IsTaskFailed ...
func (s Status) IsTaskFailed(i int) (bool, error) {
	if i >= 0 && i < len(s.Tasks) {
		return s.Tasks[i].State == "FAILED", nil
	}
	return true, fmt.Errorf("Task index is out of bounds")
}

// IsConnectorFailed ...
func (s Status) IsConnectorFailed() bool {
	return s.Connector.State == "FAILED"
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
	Name                         string         `json:"name,omitempty" validate:"required,connectorname"`
	ConnectorClass               string         `json:"connector.class" validate:"required,fqdn"`
	DocumentType                 string         `json:"type.name" validate:"required,esdoctype"`
	Topics                       string         `json:"topics" validate:"required,topiclist"`
	TopicIndexMap                string         `json:"topic.index.map" validate:"required,topicindexmap"`
	ConnectionURL                string         `json:"connection.url" validate:"required,url"`
	ConnectionUsername           string         `json:"connection.username,omitempty"`
	ConnectionPassword           string         `json:"connection.password,omitempty"`
	KeyIgnore                    types.FlexBool `json:"key.ignore"`
	SchemaIgnore                 types.FlexBool `json:"schema.ignore,omitempty"`
	Type                         string         `json:"type,omitempty" validate:"omitempty,connectorname"`
	BehaviorOnMalformedDocuments string         `json:"behavior.on.malformed.documents,omitempty"  validate:"required,oneof=ignore fail warn"`
	BatchSize                    types.FlexInt  `json:"batch.size,omitempty"`
	MaxInFlightRequests          types.FlexInt  `json:"max.in.flight.requests,omitempty"`
	MaxBufferedRecords           types.FlexInt  `json:"max.buffered.records,omitempty"`
	LingerMs                     types.FlexInt  `json:"linger.ms,omitempty"`
	FlushTimeoutMs               types.FlexInt  `json:"flush.timeout.ms,omitempty"`
	MaxRetries                   types.FlexInt  `json:"max.retries,omitempty"`
	RetryBackoffMs               types.FlexInt  `json:"retry.backoff.ms,omitempty"`
	ConnectionCompression        types.FlexBool `json:"connection.compression,omitempty"`
	ConnectionTimeoutMs          types.FlexInt  `json:"connection.timeout.ms,omitempty"`
	ReadTimeoutMs                types.FlexInt  `json:"read.timeout.ms,omitempty"`
	TasksMax                     types.FlexInt  `json:"task.max,omitempty"`
	OffsetFlushTimeoutMs         types.FlexInt  `json:"offset.flush.timeout.ms,omitempty"`
	HeartbeatIntervalMs          types.FlexInt  `json:"heartbeat.interval.ms,omitempty"`
	ValueConverterSchemasEnable  types.FlexBool `json:"value.converter.schemas.enable,omitempty"`
	ValueConverter               string         `json:"value.converter,omitempty" validate:"omitempty,fqdn"`
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
		zap.L().Info("Sending POST to /connectors endpoint", zap.String("payload", string(configBytes)))

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
			response.Result = "notfound"
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

// GetStatus ...
func (kcc Client) GetStatus(connector string) (*Response, error) {
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

// RestartTask ...
func (kcc Client) RestartTask(connector string, taskID int) (*Response, error) {
	if govalidator.IsDNSName(connector) {
		endpoint := fmt.Sprintf("/connectors/%s/tasks/%d/restart", connector, taskID)
		status, _, err := kcc.httpClient.Post(endpoint, []byte{})

		if status == 204 {
			response := new(Response)
			response.Result = "success"
			return response, nil
		}
		response := new(Response)
		response.Result = "error"
		return response, err
	}
	return nil, errors.New("Malformed connector name")
}

// RestartConnector ...
func (kcc Client) RestartConnector(connector string) (*Response, error) {
	if govalidator.IsDNSName(connector) {
		endpoint := fmt.Sprintf("/connectors/%s/restart", connector)
		status, _, err := kcc.httpClient.Post(endpoint, []byte{})

		if status == 204 {
			response := new(Response)
			response.Result = "success"
			return response, nil
		}
		response := new(Response)
		response.Result = "error"
		return response, err
	}
	return nil, errors.New("Malformed connector name")
}
