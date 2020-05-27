package validator

import (
	"regexp"

	"gopkg.in/go-playground/validator.v9"
)

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

func New() *validator.Validate {
	var err error
	v := validator.New()
	err = v.RegisterValidation("connectorname", func(fl validator.FieldLevel) bool {
		return validateConnectorName(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	_ = v.RegisterValidation("kafkatopic", func(fl validator.FieldLevel) bool {
		return validateKafkaTopicName(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	_ = v.RegisterValidation("esdoctype", func(fl validator.FieldLevel) bool {
		return validateESDocType(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	_ = v.RegisterValidation("topiclist", func(fl validator.FieldLevel) bool {
		return validateKafkaTopicList(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	_ = v.RegisterValidation("topicindexmap", func(fl validator.FieldLevel) bool {
		return validateTopicIndexMap(fl.Field().String())
	})

	if err != nil {
		panic(err)
	}

	return v
}
