package client

import (
	"errors"
	"time"

	resty "github.com/go-resty/resty/v2"
)

// RestyClientFactory ...
type RestyClientFactory struct{}

// Create ...
func (f RestyClientFactory) Create(url string, config HTTPClientConfig) (HTTPClient, error) {
	r := NewRestyClient(url)

	switch config.AuthType {
	case TokenAuth:
		r.SetAuthToken(config.Creds["token"])
	case BasicAuth:
		r.SetBasicAuth(config.Creds["username"], config.Creds["password"])
	default:
		break
	}

	for key, value := range config.Headers {
		r.SetHeader(key, value)
	}

	r.SetRetryCount(config.RetryCount)
	r.SetRetryWaitTime(config.RetryWaitTime)
	r.SetRetryMaxWaitTime(config.RetryWaitMaxTime)

	if config.RetryConditionFunc != nil {
		r.AddRetryCondition(config.RetryConditionFunc)
	}

	if r == nil {
		return *r, errors.New("Error creating go-resty client")
	}
	return *r, nil
}

// RestyClient ...
type RestyClient struct {
	client  *resty.Client
	baseURL string
}

// NewRestyClient ...
func NewRestyClient(url string) *RestyClient {
	r := RestyClient{
		client:  resty.New(),
		baseURL: url,
	}
	return &r
}

// SetHeader ...
func (r RestyClient) SetHeader(key string, value string) {
	r.client.SetHeader(key, value)
}

// SetAuthToken ...
func (r RestyClient) SetAuthToken(token string) {
	r.client.SetAuthToken(token)
}

// SetRetryCount ...
func (r RestyClient) SetRetryCount(count int) {
	r.client.SetRetryCount(count)
}

// SetRetryWaitTime ...
func (r RestyClient) SetRetryWaitTime(d time.Duration) {
	r.client.SetRetryWaitTime(d)
}

// SetRetryMaxWaitTime ...
func (r RestyClient) SetRetryMaxWaitTime(d time.Duration) {
	r.client.SetRetryMaxWaitTime(d)
}

// AddRetryCondition ...
func (r RestyClient) AddRetryCondition(condition resty.RetryConditionFunc) {
	r.client.AddRetryCondition(condition)
}

// AddRetryCondition ...
func (r RestyClient) SetBasicAuth(username string, password string) {
	r.client.SetBasicAuth(username, password)
}

// Get ...
func (r RestyClient) Get(endpoint string) (int, *[]byte, error) {
	resp, err := r.client.R().Get(r.baseURL + endpoint)
	body := resp.Body()
	return resp.StatusCode(), &body, err
}

// Post ...
func (r RestyClient) Post(endpoint string, body []byte) (int, *[]byte, error) {
	resp, err := r.client.R().SetBody(body).Post(r.baseURL + endpoint)
	respBody := resp.Body()
	return resp.StatusCode(), &respBody, err
}

// Delete ...
func (r RestyClient) Delete(endpoint string) (int, *[]byte, error) {
	resp, err := r.client.R().Delete(r.baseURL + endpoint)
	respBody := resp.Body()
	return resp.StatusCode(), &respBody, err
}

// Delete ...
func (r RestyClient) Put(endpoint string, body []byte) (int, *[]byte, error) {
	resp, err := r.client.R().SetBody(body).Put(r.baseURL + endpoint)
	respBody := resp.Body()
	return resp.StatusCode(), &respBody, err
}
