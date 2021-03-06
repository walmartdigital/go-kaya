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
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
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

// HandleNonOKResponse manages any non HTTP 200 response code and conveys the corresponding
// error to the caller. Returns a response type 'unspecified' if the response code is
// specifically handled.
func HandleNonOKResponse(status int, body *[]byte) (*Response, error) {
	var err error
	var kcError Error

	if body != nil {
		err = json.Unmarshal(*body, &kcError)
	}

	if err != nil {
		return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
	}

	response := new(Response)
	switch status {
	case 404:
		response.Result = "notfound"
		return response, fmt.Errorf("Non HTTP 200 response (code:'%d', message:'%s')", kcError.ErrorCode, kcError.Message)
	case 409:
		response := new(Response)
		response.Result = "conflict"
		return response, fmt.Errorf("Non HTTP 200 response (code:'%d', message:'%s')", kcError.ErrorCode, kcError.Message)
	default:
		response := new(Response)
		response.Result = "unspecified"
		return response, fmt.Errorf("Received unhandled HTTP response (code:'%d', message:'%s')", kcError.ErrorCode, kcError.Message)
	}
}

// Create ...
func (kcc Client) Create(connector Connector) (*Response, error) {
	if govalidator.IsDNSName(connector.Name) {
		configBytes, err := json.Marshal(connector)
		if err != nil {
			return &Response{Result: "error"}, errors.New("Failed to serialize connector configuration")
		}

		status, body, err := kcc.httpClient.Post("/connectors", configBytes)

		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Create on Kafka Connect: %s", err.Error())
		}

		switch status {
		case 201:
			var createdConnector Connector
			err := json.Unmarshal(*body, &createdConnector)
			if err == nil {
				response := new(Response)
				response.Result = "success"
				response.Payload = createdConnector
				return response, nil
			}
			return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
		default:
			return HandleNonOKResponse(status, body)
		}
	}
	return nil, errors.New("Malformed connector name")
}

// Read gets a connector configuration from KafkaConnect. The returned payload contains
// contains the configuration of the connector.
func (kcc Client) Read(connector string) (*Response, error) {
	var config map[string]string
	if govalidator.IsDNSName(connector) {
		status, body, err := kcc.httpClient.Get("/connectors/" + connector + "/config")

		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Read on Kafka Connect: %s", err.Error())
		}

		switch status {
		case 200:
			err := json.Unmarshal(*body, &config)
			if err == nil {
				response := new(Response)
				response.Result = "success"
				response.Payload = config
				return response, nil
			}
			return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
		default:
			return HandleNonOKResponse(status, body)
		}
	}
	return nil, errors.New("Malformed connector name")
}

// Update updates an existing connector's configuration. Due to Kadfka Connect's behavior, which
// creates a connector if the connector does not exist, the function sends a GET first in order
// to determine whether the connector already exists or not.
func (kcc Client) Update(connector Connector) (*Response, error) {
	if govalidator.IsDNSName(connector.Name) {
		status, body, err := kcc.httpClient.Get("/connectors/" + connector.Name)

		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Update on Kafka Connect: %s", err.Error())
		}

		if status != 200 {
			return HandleNonOKResponse(status, body)
		}

		configBytes, err := json.Marshal(connector.Config)

		if err != nil {
			return &Response{Result: "error"}, errors.New("Failed to serialize connector configuration")
		}

		status, body, err = kcc.httpClient.Put("/connectors/"+connector.Name+"/config", configBytes)

		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Update on Kafka Connect: %s", err.Error())
		}

		switch status {
		case 200:
			var connector Connector
			err := json.Unmarshal(*body, &connector)
			if err == nil {
				response := new(Response)
				response.Result = "success"
				response.Payload = connector
				return response, nil
			}
			return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
		default:
			return HandleNonOKResponse(status, body)
		}
	}
	return nil, errors.New("Malformed connector name")
}

// Delete ...
func (kcc Client) Delete(connector string) (*Response, error) {
	if govalidator.IsDNSName(connector) {
		status, body, err := kcc.httpClient.Delete("/connectors/" + connector)

		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing Delete on Kafka Connect: %s", err.Error())
		}

		switch status {
		case 204:
			response := new(Response)
			response.Result = "success"
			return response, nil
		default:
			return HandleNonOKResponse(status, body)
		}
	}
	return nil, errors.New("Malformed connector name")
}

// GetStatus ...
func (kcc Client) GetStatus(connector string) (*Response, error) {
	var connectorStatus Status
	if govalidator.IsDNSName(connector) {
		status, body, err := kcc.httpClient.Get("/connectors/" + connector + "/status")

		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing GetStatus on Kafka Connect: %s", err.Error())
		}

		switch status {
		case 200:
			err := json.Unmarshal(*body, &connectorStatus)
			if err == nil {
				response := new(Response)
				response.Result = "success"
				response.Payload = connectorStatus
				return response, nil
			}
			return &Response{Result: "error"}, errors.New("Failed to deserialize Kafka Connect response")
		default:
			return HandleNonOKResponse(status, body)
		}
	}
	return nil, errors.New("Malformed connector name")
}

// RestartTask ...
func (kcc Client) RestartTask(connector string, taskID int) (*Response, error) {
	if govalidator.IsDNSName(connector) {
		endpoint := fmt.Sprintf("/connectors/%s/tasks/%d/restart", connector, taskID)
		status, body, err := kcc.httpClient.Post(endpoint, []byte{})

		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing RestartTask on Kafka Connect: %s", err.Error())
		}

		switch status {
		case 204:
			response := new(Response)
			response.Result = "success"
			return response, nil
		default:
			return HandleNonOKResponse(status, body)
		}
	}
	return nil, errors.New("Malformed connector name")
}

// RestartConnector ...
func (kcc Client) RestartConnector(connector string) (*Response, error) {
	if govalidator.IsDNSName(connector) {
		endpoint := fmt.Sprintf("/connectors/%s/restart", connector)
		status, body, err := kcc.httpClient.Post(endpoint, []byte{})

		if err != nil {
			return &Response{Result: "error"}, fmt.Errorf("Error executing RestartConnector on Kafka Connect: %s", err.Error())
		}

		switch status {
		case 204:
			response := new(Response)
			response.Result = "success"
			return response, nil
		default:
			return HandleNonOKResponse(status, body)
		}
	}
	return nil, errors.New("Malformed connector name")
}
