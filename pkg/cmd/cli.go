package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	flag "github.com/spf13/pflag"
	"github.com/walmartdigital/go-kaya/pkg/client"
	"github.com/walmartdigital/go-kaya/pkg/kafkaconnect"
	"go.uber.org/zap"
)

func readConfig(file string) *kafkaconnect.Connector {
	var config kafkaconnect.Connector

	content, err0 := ioutil.ReadFile(file)
	if err0 != nil {
		zap.L().Error(err0.Error())
		return nil
	}

	err1 := json.Unmarshal(content, &config)

	if err1 != nil {
		zap.L().Error(err1.Error())
		return nil
	}

	return &config
}

func main() {
	var host string
	var configFile string
	var action string
	var connector string
	var taskID int

	var logger *zap.Logger
	var err error
	logLevel := os.Getenv("LOG_LEVEL")

	if logLevel == "DEBUG" {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}

	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)
	zap.L().Debug("Logger initialized, writing to stdout")

	flag.StringVarP(&host, "addr", "a", "", "Kafka Connect address in the form of <host:port>")
	flag.StringVarP(&configFile, "file", "f", "", "Path to connector config file")
	flag.StringVarP(&action, "cmd", "c", "", "Action to perform against Kafka Connect instance")
	flag.StringVarP(&connector, "name", "n", "", "Connector name on which to perform action")
	flag.IntVarP(&taskID, "taskId", "t", 0, "Task ID to restart")

	flag.Parse()

	zap.L().Debug("Parsed command line flag 'host': " + host)
	zap.L().Debug("Parsed command line flag 'file': " + configFile)
	zap.L().Debug("Parsed command line flag 'action': " + action)
	zap.L().Debug("Parsed command line flag 'connector':" + connector)

	config := client.HTTPClientConfig{
		Headers:            map[string]string{"Content-type": "application/json"},
		AuthType:           client.NoneAuth,
		RetryCount:         3,
		RetryWaitTime:      1 * time.Second,
		RetryWaitMaxTime:   30 * time.Second,
		RetryConditionFunc: nil,
	}

	client, _ := kafkaconnect.NewClient(host, config, client.RestyClientFactory{})

	switch action {
	case "read":
		response, err := client.Read(connector)
		if err != nil {
			zap.L().Error(err.Error())
		} else {
			bytes, _ := json.Marshal(response)
			zap.L().Info(string(bytes))
		}
	case "status":
		response, err := client.GetStatus(connector)
		if err != nil {
			zap.L().Error(err.Error())
		} else {
			bytes, _ := json.Marshal(response)
			zap.L().Info(string(bytes))
		}
	case "create":
		if configFile == "" {
			zap.L().Error("If action is 'create', a configuration file is required")
			return
		}
		config := readConfig(configFile)
		if config != nil {
			response, err := client.Create(*config)
			if err != nil {
				zap.L().Error(err.Error())
				return
			}
			if response.Result == "success" {
				zap.L().Info("Connector created successfully")

				bytes, err1 := json.Marshal(response.Payload)
				if err1 != nil {
					zap.L().Error(err1.Error())
					return
				}
				zap.L().Info(string(bytes))
			}
		}
	case "update":
		if configFile == "" {
			zap.L().Error("If action is 'create', a configuration file is required")
			return
		}
		config := readConfig(configFile)
		if config != nil {
			response, err := client.Update(*config)
			if err != nil {
				zap.L().Error(err.Error())
				return
			}
			if response.Result == "success" {
				zap.L().Info("Connector updated successfully")

				bytes, err1 := json.Marshal(response.Payload)
				if err1 != nil {
					zap.L().Error(err1.Error())
					return
				}
				zap.L().Info(string(bytes))
			}
		}
	case "delete":
		response, err := client.Delete(connector)
		if err != nil {
			zap.L().Error(err.Error())
		}
		if response.Result == "success" {
			zap.L().Info("Connector deleted successfully")
		} else {
			zap.L().Info("Could not delete connector")
		}
	case "restart-connector":
		response, err := client.RestartConnector(connector)
		if err != nil {
			zap.L().Error(err.Error())
		}
		if response.Result == "success" {
			zap.L().Info("Connector restarted successfully")
		} else {
			zap.L().Info("Could not restart connector")
		}
	case "restart-task":
		response, err := client.RestartTask(connector, taskID)
		if err != nil {
			zap.L().Error(err.Error())
		}
		if response.Result == "success" {
			zap.L().Info("Task restarted successfully")
		} else {
			zap.L().Info("Could not restart task")
		}
	default:
		zap.L().Fatal("Invalid action requested")
	}
}
