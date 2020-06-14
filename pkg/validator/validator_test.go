package validator_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/walmartdigital/go-kaya/pkg/validator"
)

func TestAll(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Validator")
}

var _ = Describe("Validator tests", func() {
	var (
		v *validator.Validator
	)

	BeforeEach(func() {
		v = validator.New()
		_ = v
	})

	It("should validate successfully due to a valid configuration", func() {
		config := map[string]string{
			"name":            "amida.logging",
			"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
			"topic.index.map": "dumblogger-logs<logs-pd-dumblogger-{now/d}>,_ims.logs:<logs-pd-ims-{now/d}>,_amida.logs:<logs-pd-amida-{now/d}>,_osiris.logs:<logs-pd-osiris-{now/d}>,_midas.logs:<logs-pd-midas-{now/d}>,_kimun.logs:<logs-pd-kimun-{now/d}>",
		}
		ok, err := v.ValidateMap(config)
		Expect(ok).To(Equal(true))
		Expect(err).To(BeNil())
	})

	It("should fail validation due to an invalid connector name", func() {
		config := map[string]string{
			"name":            "amida.logging$%&/(",
			"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
			"topic.index.map": "dumblogger-logs<logs-pd-dumblogger-{now/d}>,_ims.logs:<logs-pd-ims-{now/d}>,_amida.logs:<logs-pd-amida-{now/d}>,_osiris.logs:<logs-pd-osiris-{now/d}>,_midas.logs:<logs-pd-midas-{now/d}>,_kimun.logs:<logs-pd-kimun-{now/d}>",
		}
		ok, err := v.ValidateMap(config)
		Expect(ok).To(Equal(false))
		Expect(err).NotTo(BeNil())
	})

	It("should fail validation due to a missing connector name", func() {
		config := map[string]string{
			"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
			"topic.index.map": "dumblogger-logs<logs-pd-dumblogger-{now/d}>,_ims.logs:<logs-pd-ims-{now/d}>,_amida.logs:<logs-pd-amida-{now/d}>,_osiris.logs:<logs-pd-osiris-{now/d}>,_midas.logs:<logs-pd-midas-{now/d}>,_kimun.logs:<logs-pd-kimun-{now/d}>",
		}
		ok, err := v.ValidateMap(config)
		Expect(ok).To(Equal(false))
		Expect(err).NotTo(BeNil())
	})

	It("should fail validation due to an invalid connector class", func() {
		config := map[string]string{
			"name":            "amida.logging$%&/(",
			"connector.class": "io.confluent.connect.elasticsearch.%/$(",
			"topic.index.map": "dumblogger-logs<logs-pd-dumblogger-{now/d}>,_ims.logs:<logs-pd-ims-{now/d}>,_amida.logs:<logs-pd-amida-{now/d}>,_osiris.logs:<logs-pd-osiris-{now/d}>,_midas.logs:<logs-pd-midas-{now/d}>,_kimun.logs:<logs-pd-kimun-{now/d}>",
		}
		ok, err := v.ValidateMap(config)
		Expect(ok).To(Equal(false))
		Expect(err).NotTo(BeNil())
	})

	It("should fail validation due to a missing connector class", func() {
		config := map[string]string{
			"name":            "amida.logging",
			"topic.index.map": "dumblogger-logs<logs-pd-dumblogger-{now/d}>,_ims.logs:<logs-pd-ims-{now/d}>,_amida.logs:<logs-pd-amida-{now/d}>,_osiris.logs:<logs-pd-osiris-{now/d}>,_midas.logs:<logs-pd-midas-{now/d}>,_kimun.logs:<logs-pd-kimun-{now/d}>",
		}
		ok, err := v.ValidateMap(config)
		Expect(ok).To(Equal(false))
		Expect(err).NotTo(BeNil())
	})

})
