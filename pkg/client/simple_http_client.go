package client

// Replace this by go-resty
import (
	"bytes"
	"io/ioutil"
	"net/http"
	"time"

	"go.uber.org/zap"
)

var sugar *zap.SugaredLogger

func init() {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	sugar = logger.Sugar()
}

// SimpleHTTPClient ...
type SimpleHTTPClient struct {
	baseURL string
}

// NewSimpleHTTPClient ...
func NewSimpleHTTPClient(url string) *SimpleHTTPClient {
	hc := new(SimpleHTTPClient)
	hc.baseURL = url
	return hc
}

// Get ...
func (shc SimpleHTTPClient) Get(endpoint string) (*string, *[]byte, error) {
	return shc.sendRequest("GET", endpoint, nil)
}

// Post ...
func (shc SimpleHTTPClient) Post(endpoint string, content []byte) (*string, *[]byte, error) {
	return shc.sendRequest("POST", endpoint, content)
}

// Put ...
func (shc SimpleHTTPClient) Put(endpoint string, content []byte) (*string, *[]byte, error) {
	return shc.sendRequest("PUT", endpoint, content)
}

// Delete ...
func (shc SimpleHTTPClient) Delete(endpoint string) (*string, *[]byte, error) {
	return shc.sendRequest("DELETE", endpoint, nil)
}

func (shc SimpleHTTPClient) sendRequest(verb string, endpoint string, content []byte) (*string, *[]byte, error) {
	req, err := http.NewRequest(verb, shc.baseURL+endpoint, bytes.NewBuffer(content))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: time.Second * 30,
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	return &resp.Status, &body, nil
}
