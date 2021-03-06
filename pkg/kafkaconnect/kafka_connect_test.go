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
		kafkaConnectConfig    map[string]string
		kafkaConnectClient    *kafkaconnect.Client
	)

	BeforeEach(func() {
		fakeHTTPClient = mocks.NewMockHTTPClient(ctrl)
		fakeHTTPClientFactory = mocks.NewMockHTTPClientFactory(ctrl)
		fakeHTTPClientFactory.EXPECT().Create("http://somehost", client.HTTPClientConfig{}).Return(
			fakeHTTPClient, nil,
		).Times(1)
		kafkaConnectConfig = map[string]string{
			"name":            "logging",
			"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
			"type.name":       "log",
			"topics":          "_dumblogger.logs",
			"topic.index.map": "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
			"batch.size":      "100",
			"connection.url":  "http://elasticsearch-master.default.svc.cluster.local:9200",
			"key.ignore":      "true",
			"schema.ignore":   "true",
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
		Expect(resp.Payload.(map[string]string)).To(Equal(kafkaConnectConfig))
	})

	It("should get a connector status", func() {
		task := kafkaconnect.Task{
			ID:       0,
			State:    "RUNNING",
			WorkerID: "somenode",
		}

		status := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "RUNNING",
				WorkerID: "somenode",
			},
			Tasks: []kafkaconnect.Task{
				task,
			},
		}

		statusCode := 200
		responseBody, err := json.Marshal(status)

		_ = err

		fakeHTTPClient.EXPECT().Get("/connectors/logging/status").Return(
			statusCode,
			&responseBody,
			nil,
		).Times(1)

		resp, err := kafkaConnectClient.GetStatus("logging")
		Expect(err).To(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("success"))
		Expect(resp.Payload.(kafkaconnect.Status)).To(Equal(status))
	})

	It("should get correct number of failed and active tasks from a Status", func() {
		task0 := kafkaconnect.Task{
			ID:       0,
			State:    "RUNNING",
			WorkerID: "somenode",
		}

		task1 := kafkaconnect.Task{
			ID:       0,
			State:    "FAILED",
			WorkerID: "somenode",
		}

		status0 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "RUNNING",
				WorkerID: "somenode",
			},
			Tasks: []kafkaconnect.Task{
				task0,
				task1,
			},
		}

		status1 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "RUNNING",
				WorkerID: "somenode",
			},
			Tasks: []kafkaconnect.Task{
				task0,
				task0,
			},
		}

		status2 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "RUNNING",
				WorkerID: "somenode",
			},
			Tasks: []kafkaconnect.Task{
				task1,
				task1,
			},
		}

		Expect(status0.GetActiveTasksCount()).To(Equal(1))
		Expect(status0.GetFailedTasks()).To(Equal([]int{1}))
		Expect(status1.GetActiveTasksCount()).To(Equal(2))
		Expect(status1.GetFailedTasks()).To(BeEmpty())
		Expect(status2.GetActiveTasksCount()).To(Equal(0))
		Expect(status2.GetFailedTasks()).To(Equal([]int{0, 1}))
	})

	It("should get the number of tasks in a Status", func() {
		task0 := kafkaconnect.Task{
			ID:       0,
			State:    "RUNNING",
			WorkerID: "somenode",
		}

		status0 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "RUNNING",
				WorkerID: "somenode",
			},
		}

		status1 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "RUNNING",
				WorkerID: "somenode",
			},
			Tasks: []kafkaconnect.Task{
				task0,
			},
		}

		status2 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "RUNNING",
				WorkerID: "somenode",
			},
			Tasks: []kafkaconnect.Task{
				task0,
				task0,
			},
		}

		Expect(status0.GetTaskCount()).To(Equal(0))
		Expect(status1.GetTaskCount()).To(Equal(1))
		Expect(status2.GetTaskCount()).To(Equal(2))
	})

	It("should inform whether a task is in FAILED state", func() {
		task0 := kafkaconnect.Task{
			ID:       0,
			State:    "RUNNING",
			WorkerID: "somenode",
		}

		task1 := kafkaconnect.Task{
			ID:       0,
			State:    "FAILED",
			WorkerID: "somenode",
		}

		status1 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "RUNNING",
				WorkerID: "somenode",
			},
			Tasks: []kafkaconnect.Task{
				task0,
				task1,
			},
		}

		b0, err0 := status1.IsTaskFailed(0)
		b1, err1 := status1.IsTaskFailed(1)
		b2, err2 := status1.IsTaskFailed(2)

		Expect(b0).To(Equal(false))
		Expect(err0).To(BeNil())
		Expect(b1).To(Equal(true))
		Expect(err1).To(BeNil())
		Expect(b2).To(Equal(true))
		Expect(err2).NotTo(BeNil())
	})

	It("should indicate whether a connector is failed", func() {
		status1 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "PAUSED",
				WorkerID: "somenode",
			},
		}

		status2 := kafkaconnect.Status{
			Name: "blah",
			Connector: kafkaconnect.ConnectorStatus{
				State:    "FAILED",
				WorkerID: "somenode",
			},
		}

		Expect(status1.IsConnectorFailed()).To(Equal(false))
		Expect(status2.IsConnectorFailed()).To(Equal(true))
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
		Expect(resp.Result).To(Equal("notfound"))
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
			Config: map[string]string{
				"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"type.name":       "log",
				"topics":          "_dumblogger.logs",
				"topic.index.map": "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
				"batch.size":      "100",
				"connection.url":  "http://elasticsearch-master.default.svc.cluster.local:9200",
				"key.ignore":      "true",
				"schema.ignore":   "true",
			},
		}

		resultConnector = kafkaconnect.Connector{
			Name: "logging",
			Config: map[string]string{
				"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"type.name":       "log",
				"topics":          "_dumblogger.logs",
				"topic.index.map": "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
				"batch.size":      "100",
				"connection.url":  "http://elasticsearch-master.default.svc.cluster.local:9200",
				"key.ignore":      "true",
				"schema.ignore":   "true",
				"type":            "sink",
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
		Expect(resp.Result).To(BeIdenticalTo("conflict"))
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
			Config: map[string]string{
				"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"type.name":       "log",
				"topics":          "_dumblogger.logs",
				"topic.index.map": "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
				"batch.size":      "100",
				"connection.url":  "http://elasticsearch-master.default.svc.cluster.local:9200",
				"key.ignore":      "true",
				"schema.ignore":   "true",
			},
		}
		resultConnector = kafkaconnect.Connector{
			Name: "logging",
			Config: map[string]string{
				"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"type.name":       "log",
				"topics":          "_dumblogger.logs",
				"topic.index.map": "_dumblogger.logs:<logs-pd-dumblogger-{now/d}>",
				"batch.size":      "100",
				"connection.url":  "http://elasticsearch-master.default.svc.cluster.local:9200",
				"key.ignore":      "true",
				"schema.ignore":   "true",
				"type":            "sink",
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
		Expect(resp.Result).To(BeIdenticalTo("notfound"))
	})

	It("should not update a connector because Kafka Connect returns a conflict", func() {
		statusCode := 409
		kafkaConnectError := kafkaconnect.Error{ErrorCode: 409, Message: "Conflict"}
		responseBody, _ := json.Marshal(kafkaConnectError)

		emptymapbytes, _ := json.Marshal(map[string]string{})

		fakeHTTPClient.EXPECT().Get("/connectors/logging").Return(
			200,
			&[]byte{},
			nil,
		).Times(1)

		fakeHTTPClient.EXPECT().Put("/connectors/logging/config", emptymapbytes).Return(
			statusCode,
			&responseBody,
			nil,
		).Times(1)

		emptyConnector := kafkaconnect.Connector{
			Name:   "logging",
			Config: map[string]string{},
		}

		resp, err2 := kafkaConnectClient.Update(emptyConnector)
		Expect(err2).NotTo(BeNil())
		Expect(resp.Result).To(BeIdenticalTo("conflict"))
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
		Expect(resp.Result).To(BeIdenticalTo("notfound"))
	})
})
