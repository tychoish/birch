package mongonet

import "fmt"

type SSLPair struct {
	CertFile string
	KeyFile  string
}

type ProxyConfig struct {
	BindHost string
	BindPort int

	MongoHost string
	MongoPort int
	MongoSSL  bool

	UseSSL  bool
	SSLKeys []SSLPair

	VerboseLevel int

	InterceptorFactory ProxyInterceptorFactory
}

func NewProxyConfig(bindHost string, bindPort int, mongoHost string, mongoPort int) ProxyConfig {
	return ProxyConfig{
		bindHost,
		bindPort,
		mongoHost,
		mongoPort,
		false, // MongoSSL
		false, // UseSSL
		nil,
		0,   // VerboseLevel
		nil, // InterceptorFactory
	}
}

func (pc *ProxyConfig) MongoAddress() string {
	return fmt.Sprintf("%s:%d", pc.MongoHost, pc.MongoPort)
}
