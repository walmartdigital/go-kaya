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
	})

	It("should return an error due to an invalid configuration", func() {
		config := map[string]string{}{
			"key":             "yesvsdvsdfvdfvafadsca",
			"key2":            "anyway1234",
			"topic.index.map": "dumblogger-logs<logs-pd-dumblogger-{now/d}>,_ims.logs:<logs-pd-ims-{now/d}>,_amida.logs:<logs-pd-amida-{now/d}>,_osiris.logs:<logs-pd-osiris-{now/d}>,_midas.logs:<logs-pd-midas-{now/d}>,_kimun.logs:<logs-pd-kimun-{now/d}>",
		}
		ok, err := v.ValidateMap(config)
		Expect(ok).To(Equal(false))
		Expect(err).NotTo(BeNil())
	})
})

