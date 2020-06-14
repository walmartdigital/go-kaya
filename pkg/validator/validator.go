package validator

import (
	"fmt"
	"regexp"

	"gopkg.in/go-playground/validator.v9"
)

var validationMap = map[string]string{
	"name":            "connectorname",
	"connector.class": "fqdn",
}

func validateConnectorName(name string) bool {
	r, err := regexp.Compile("^[A-Za-z0-9\\._-]{1,255}$")
	if err != nil {
		panic(err)
	}
	return r.MatchString(name)
}

func validateESDocType(name string) bool {
	r, err := regexp.Compile("^[A-Za-z0-9_-]{1,63}$")
	if err != nil {
		panic(err)
	}
	return r.MatchString(name)
}

func validateKafkaTopicName(name string) bool {
	r, err := regexp.Compile("^[A-Za-z0-9\\._-]{1,255}$")
	if err != nil {
		panic(err)
	}
	return r.MatchString(name)
}

func validateKafkaTopicList(name string) bool {
	r, err := regexp.Compile("^[A-Za-z0-9\\._-]{1,255}([,]{1}[A-Za-z0-9\\._-]{1,255})*$")
	if err != nil {
		panic(err)
	}
	return r.MatchString(name)
}

func validateTopicIndexMap(name string) bool {
	r, err := regexp.Compile("^([A-Za-z0-9\\._-]{1,255}([:]{1}[A-Za-z0-9\\<\\>\\{\\}\\/\\._-]{1,255}))([,]{1}([A-Za-z0-9\\._-]{1,255}([:]{1}[A-Za-z0-9\\<\\>\\{\\}\\/\\._-]{1,255})))*$")
	if err != nil {
		panic(err)
	}
	return r.MatchString(name)
}

// Validator ...
type Validator struct {
	LastError error
	validator *validator.Validate
}

// ValidateMap ...
func (v *Validator) ValidateMap(input map[string]string) (bool, error) {
	err := v.validator.Var(input, "connectorconfigmap")
	if err != nil {
		return false, v.LastError
	}
	return true, nil
}

// New ...
func New() *Validator {
	var err error

	v := validator.New()

	instance := Validator{
		validator: v,
	}

	err = v.RegisterValidation("connectorname", func(fl validator.FieldLevel) bool {
		return validateConnectorName(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	err = v.RegisterValidation("kafkatopic", func(fl validator.FieldLevel) bool {
		return validateKafkaTopicName(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	err = v.RegisterValidation("esdoctype", func(fl validator.FieldLevel) bool {
		return validateESDocType(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	err = v.RegisterValidation("topiclist", func(fl validator.FieldLevel) bool {
		return validateKafkaTopicList(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	err = v.RegisterValidation("topicindexmap", func(fl validator.FieldLevel) bool {
		return validateTopicIndexMap(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	err = v.RegisterValidation("connectorconfigmap", func(fl validator.FieldLevel) bool {
		m, ok := fl.Field().Interface().(map[string]interface{})
		if !ok {
			instance.LastError = fmt.Errorf("Not map[string]string")
			return false
		}

		for k, val := range validationMap {
			if v.Var(m[k], val) != nil {
				instance.LastError = fmt.Errorf("Field %s failed validation", k)
				return false
			}
		}

		return true
	})

	if err != nil {
		panic(err)
	}

	return &instance
}
