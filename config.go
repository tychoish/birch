package mongonet

import (
	"crypto/x509"
	"fmt"
	"time"

	"github.com/mongodb/grip/level"
)

type SSLPair struct {
	CertFile string
	KeyFile  string
}

type ProxyConfig struct {
	BindHost string
	BindPort int

	MongoHost          string
	MongoPort          int
	MongoSSL           bool
	MongoRootCAs       *x509.CertPool
	MongoSSLSkipVerify bool

	UseSSL  bool
	SSLKeys []SSLPair

	LogLevel level.Priority

	InterceptorFactory ProxyInterceptorFactory
	ConnectionPoolHook ConnectionHook
	TCPKeepAlivePeriod time.Duration // set to 0 for no keep alives
}

func NewProxyConfig(bindHost string, bindPort int, mongoHost string, mongoPort int) ProxyConfig {
	return ProxyConfig{
		BindHost:  bindHost,
		BindPort:  bindPort,
		MongoHost: mongoHost,
		MongoPort: mongoPort,
		LogLevel:  level.Priority(0),
	}
}

func (pc *ProxyConfig) MongoAddress() string {
	return fmt.Sprintf("%s:%d", pc.MongoHost, pc.MongoPort)
}
