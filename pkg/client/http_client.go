package client

import (
	"reflect"
	"time"

	resty "github.com/go-resty/resty/v2"
	"github.com/golang/mock/gomock"
)

type AuthType string

const (
	NoneAuth  AuthType = "None"
	TokenAuth AuthType = "Token"
	BasicAuth AuthType = "Basic"
)

// HTTPClientConfig ...
type HTTPClientConfig struct {
	Headers            map[string]string
	Creds              map[string]string
	AuthType           AuthType
	RetryCount         int
	RetryWaitTime      time.Duration
	RetryWaitMaxTime   time.Duration
	RetryConditionFunc resty.RetryConditionFunc
}

// MatchHTTPClientConfig ...
func MatchHTTPClientConfig(h HTTPClientConfig) gomock.Matcher {
	return &h
}

// Matches ...
func (h *HTTPClientConfig) Matches(x interface{}) bool {
	obj := x.(HTTPClientConfig)
	return h.AuthType == obj.AuthType &&
		reflect.DeepEqual(h.Headers, obj.Headers) &&
		reflect.DeepEqual(h.Creds, obj.Creds) &&
		h.RetryCount == obj.RetryCount &&
		h.RetryWaitTime == obj.RetryWaitTime &&
		h.RetryWaitMaxTime == obj.RetryWaitMaxTime
}

// String ...
func (h *HTTPClientConfig) String() string {
	return "Not all HTTPClientConfig object fields match"
}

// HTTPClient ...
type HTTPClient interface {
	Get(endpoint string) (int, *[]byte, error)
	Post(endpoint string, body []byte) (int, *[]byte, error)
	Put(endpoint string, body []byte) (int, *[]byte, error)
	Delete(endpoint string) (int, *[]byte, error)
}

// HTTPClientFactory ...
type HTTPClientFactory interface {
	Create(string, HTTPClientConfig) (HTTPClient, error)
}
