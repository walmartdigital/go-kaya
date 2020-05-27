package kafkaconnect_test

import (
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/walmartdigital/go-kaya/pkg/client"
	"github.com/walmartdigital/go-kaya/pkg/kafkaconnect"
	"github.com/walmartdigital/go-kaya/pkg/mocks"
)

var ctrl *gomock.Controller

func TestAll(t *testing.T) {
	ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	RegisterFailHandler(Fail)
	RunSpecs(t, "KafkaConnectClient")
}

var _ = Describe("New Client", func() {
	var (
		fakeHTTPClient        *mocks.MockHTTPClient
		fakeHTTPClientFactory *mocks.MockHTTPClientFactory
	)

	BeforeEach(func() {
		fakeHTTPClient = mocks.NewMockHTTPClient(ctrl)
		fakeHTTPClientFactory = mocks.NewMockHTTPClientFactory(ctrl)
		fakeHTTPClientFactory.EXPECT().Create("http://somehost", client.HTTPClientConfig{}).Return(
			fakeHTTPClient, nil,
		).Times(1)
	})

	It("should create a Kafka Connect client", func() {
		kcc, err := kafkaconnect.NewClient("somehost", client.HTTPClientConfig{}, fakeHTTPClientFactory)
		Expect(err).To(BeNil())
		Expect(kcc).NotTo(BeNil())
	})
})

var _ = Describe("Read from Kafka Connect", func() {
	var (
		fakeHTTPClient        *mocks.MockHTTPClient
		fakeHTTPClientFactory *mocks.MockHTTPClientFactory
		kafkaConnectConfig    kafkaconnect.ConnectorConfig
		kafkaConnectClient    *kafkaconnect.Client
	)

	BeforeEach(func() {
		fakeHTTPClient = mocks.NewMockHTTPClient(ctrl)
		fakeHTTPClientFactory = mocks.NewMockHTTPClientFactory(ctrl)
		fakeHTTPClientFactory.EXPECT().Create("http://somehost", client.HTTPClientConfig{}).Return(
			fakeHTTPClient, nil,
		).Times(1)
		kafkaConnectConfig = kafkaconnect.ConnectorConfig{
			Name:           "logging",
			ConnectorClass: "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
			DocumentType:   "log",
			Topics:         "_dumblogger.logs",
			TopicIndexMap:  "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
			BatchSize:      "100",
			ConnectionURL:  "http://elasticsearch-master.default.svc.cluster.local:9200",
			KeyIgnore:      "true",
			SchemaIgnore:   "true",
		}

		kafkaConnectClient, _ = kafkaconnect.NewClient("somehost", client.HTTPClientConfig{}, fakeHTTPClientFactory)
	})

	It("should read a connector configuration", func() {
		statusCode := 200
		responseBody, _ := json.Marshal(kafkaConnectConfig)

		fakeHTTPClient.EXPECT().Get("/connectors/logging/config").Return(
			statusCode,
			&responseBody,
			nil,
		).Times(1)

		resp, err2 := kafkaConnectClient.Read("logging")
		Expect(err2).To(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("success"))
		Expect(resp.Payload.(kafkaconnect.ConnectorConfig)).To(Equal(kafkaConnectConfig))
	})

	It("should throw an error because connector name is invalid", func() {
		resp, err2 := kafkaConnectClient.Read("/$%&")
		Expect(err2).NotTo(BeNil())
		Expect(resp).To(BeNil())
	})

	It("should throw an error because connector does not exist", func() {
		statusCode := 404
		kafkaConnectError := kafkaconnect.Error{ErrorCode: 404, Message: "Connector doesntexist not found"}
		responseBody, _ := json.Marshal(kafkaConnectError)

		fakeHTTPClient.EXPECT().Get("/connectors/doesntexist/config").Return(
			statusCode,
			&responseBody,
			nil,
		).Times(1)

		resp, err2 := kafkaConnectClient.Read("doesntexist")
		Expect(err2).NotTo(BeNil())
		Expect(resp.Result).To(Equal("error"))
	})
})

var _ = Describe("Create Kafka Connect connectors", func() {
	var (
		fakeHTTPClient        *mocks.MockHTTPClient
		fakeHTTPClientFactory *mocks.MockHTTPClientFactory
		sourceConnector       kafkaconnect.Connector
		resultConnector       kafkaconnect.Connector
		kafkaConnectClient    *kafkaconnect.Client
	)

	BeforeEach(func() {
		fakeHTTPClient = mocks.NewMockHTTPClient(ctrl)
		fakeHTTPClientFactory = mocks.NewMockHTTPClientFactory(ctrl)
		fakeHTTPClientFactory.EXPECT().Create("http://somehost", client.HTTPClientConfig{}).Return(
			fakeHTTPClient, nil,
		).Times(1)
		sourceConnector = kafkaconnect.Connector{
			Name: "logging",
			Config: &kafkaconnect.ConnectorConfig{
				ConnectorClass: "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				DocumentType:   "log",
				Topics:         "_dumblogger.logs",
				TopicIndexMap:  "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
				BatchSize:      "100",
				ConnectionURL:  "http://elasticsearch-master.default.svc.cluster.local:9200",
				KeyIgnore:      "true",
				SchemaIgnore:   "true",
			},
		}

		resultConnector = kafkaconnect.Connector{
			Name: "logging",
			Config: &kafkaconnect.ConnectorConfig{
				ConnectorClass: "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				DocumentType:   "log",
				Topics:         "_dumblogger.logs",
				TopicIndexMap:  "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
				BatchSize:      "100",
				ConnectionURL:  "http://elasticsearch-master.default.svc.cluster.local:9200",
				KeyIgnore:      "true",
				SchemaIgnore:   "true",
				Type:           "sink",
			},
		}

		kafkaConnectClient, _ = kafkaconnect.NewClient("somehost", client.HTTPClientConfig{}, fakeHTTPClientFactory)
	})

	It("should create a connector", func() {
		statusCode := 201
		reqBody, _ := json.Marshal(sourceConnector)
		respBody, _ := json.Marshal(resultConnector)

		fakeHTTPClient.EXPECT().Post("/connectors", reqBody).Return(
			statusCode,
			&respBody,
			nil,
		).Times(1)

		resp, err2 := kafkaConnectClient.Create(sourceConnector)
		Expect(err2).To(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("success"))
		Expect(resp.Payload.(kafkaconnect.Connector)).To(Equal(resultConnector))
	})

	It("should not create a connector because it already exists", func() {
		statusCode := 409
		kafkaConnectError := kafkaconnect.Error{ErrorCode: 409, Message: "Connector logging already exists"}
		responseBody, _ := json.Marshal(kafkaConnectError)
		reqBody, _ := json.Marshal(sourceConnector)

		fakeHTTPClient.EXPECT().Post("/connectors", reqBody).Return(
			statusCode,
			&responseBody,
			nil,
		).Times(1)

		resp, err2 := kafkaConnectClient.Create(sourceConnector)
		Expect(err2).NotTo(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("error"))
	})
})

var _ = Describe("Update Kafka Connect connectors", func() {
	var (
		fakeHTTPClient        *mocks.MockHTTPClient
		fakeHTTPClientFactory *mocks.MockHTTPClientFactory
		sourceConnector       kafkaconnect.Connector
		resultConnector       kafkaconnect.Connector
		kafkaConnectClient    *kafkaconnect.Client
	)

	BeforeEach(func() {
		fakeHTTPClient = mocks.NewMockHTTPClient(ctrl)
		fakeHTTPClientFactory = mocks.NewMockHTTPClientFactory(ctrl)
		fakeHTTPClientFactory.EXPECT().Create("http://somehost", client.HTTPClientConfig{}).Return(
			fakeHTTPClient, nil,
		).Times(1)
		sourceConnector = kafkaconnect.Connector{
			Name: "logging",
			Config: &kafkaconnect.ConnectorConfig{
				ConnectorClass: "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				DocumentType:   "log",
				Topics:         "_dumblogger.logs",
				TopicIndexMap:  "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
				BatchSize:      "100",
				ConnectionURL:  "http://elasticsearch-master.default.svc.cluster.local:9200",
				KeyIgnore:      "true",
				SchemaIgnore:   "true",
			},
		}
		resultConnector = kafkaconnect.Connector{
			Name: "logging",
			Config: &kafkaconnect.ConnectorConfig{
				ConnectorClass: "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				DocumentType:   "log",
				Topics:         "_dumblogger.logs",
				TopicIndexMap:  "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
				BatchSize:      "100",
				ConnectionURL:  "http://elasticsearch-master.default.svc.cluster.local:9200",
				KeyIgnore:      "true",
				SchemaIgnore:   "true",
				Type:           "sink",
			},
		}
		kafkaConnectClient, _ = kafkaconnect.NewClient("somehost", client.HTTPClientConfig{}, fakeHTTPClientFactory)
	})

	It("should update a connector", func() {
		statusCode := 200

		reqBody, _ := json.Marshal(sourceConnector.Config)
		respBody, _ := json.Marshal(resultConnector)

		fakeHTTPClient.EXPECT().Get("/connectors/logging").Return(
			statusCode,
			&reqBody,
			nil,
		).Times(1)

		fakeHTTPClient.EXPECT().Put("/connectors/logging/config", reqBody).Return(
			statusCode,
			&respBody,
			nil,
		).Times(1)

		resp, err2 := kafkaConnectClient.Update(sourceConnector)
		Expect(err2).To(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("success"))
		Expect(resp.Payload.(kafkaconnect.Connector)).To(Equal(resultConnector))
	})

	It("should not update a connector because it doesn't exists", func() {
		statusCode := 404
		kafkaConnectError := kafkaconnect.Error{ErrorCode: 404, Message: "Connector logging not found"}
		responseBody, _ := json.Marshal(kafkaConnectError)

		fakeHTTPClient.EXPECT().Get("/connectors/logging").Return(
			statusCode,
			&responseBody,
			nil,
		).Times(1)

		resp, err2 := kafkaConnectClient.Update(sourceConnector)
		Expect(err2).NotTo(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("error"))
	})
})

var _ = Describe("Delete Kafka Connect connectors", func() {
	var (
		fakeHTTPClient        *mocks.MockHTTPClient
		fakeHTTPClientFactory *mocks.MockHTTPClientFactory
		kafkaConnectClient    *kafkaconnect.Client
	)

	BeforeEach(func() {
		fakeHTTPClient = mocks.NewMockHTTPClient(ctrl)
		fakeHTTPClientFactory = mocks.NewMockHTTPClientFactory(ctrl)
		fakeHTTPClientFactory.EXPECT().Create("http://somehost", client.HTTPClientConfig{}).Return(
			fakeHTTPClient, nil,
		).Times(1)
		kafkaConnectClient, _ = kafkaconnect.NewClient("somehost", client.HTTPClientConfig{}, fakeHTTPClientFactory)
	})

	It("should delete a connector", func() {
		statusCode := 204

		fakeHTTPClient.EXPECT().Delete("/connectors/logging").Return(
			statusCode,
			nil,
			nil,
		).Times(1)

		resp, err := kafkaConnectClient.Delete("logging")
		Expect(err).To(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("success"))
	})

	It("should not delete a connector because it doesn't exists", func() {
		statusCode := 404
		kafkaConnectError := kafkaconnect.Error{ErrorCode: 404, Message: "Connector logging not found"}
		responseBody, _ := json.Marshal(kafkaConnectError)

		fakeHTTPClient.EXPECT().Delete("/connectors/logging").Return(
			statusCode,
			&responseBody,
			nil,
		).Times(1)

		resp, err2 := kafkaConnectClient.Delete("logging")
		Expect(err2).NotTo(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("error"))
	})
})
