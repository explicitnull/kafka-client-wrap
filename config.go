package kafkacommon

import (
	"errors"
	"time"
)

const (
	defaultWriteBatchSize    = 50
	defaultWriteBatchTimeout = 100 * time.Millisecond

	// All retries need 50 seconds by default (assuming default kafka 10s WriteTimeout is used)
	defaultMaxRetryCount      = 3
	defaultRetryJitterPercent = 10
	defaultRetryTimeout       = 5 * time.Second
)

type Config struct {
	Addresses               []string      `env:"ADDRESSES" yaml:"addresses"`
	WriteTopic              string        `env:"WRITE_TOPIC" yaml:"write_topic"`
	WriteBatchSize          int           `env:"WRITE_BATCH_SIZE" yaml:"write_batch_size"`
	WriteBatchTimeout       time.Duration `env:"WRITE_BATCH_TIMEOUT" yaml:"write_batch_timeout"`
	WriteMaxRetryCount      uint64        `env:"WRITE_MAX_RETRY_COUNT" yaml:"write_max_retry_count"`
	WriteRetryJitterPercent uint64        `env:"WRITE_RETRY_JITTER_PERCENT" yaml:"write_retry_jitter_percent"`
	WriteRetryTimeout       time.Duration `env:"WRITE_RETRY_TIMEOUT" yaml:"write_retry_timeout"`
}

func (c *Config) WithDefaults() (*Config, error) {
	if c == nil {
		return nil, errors.New("config is nil")
	}

	if len(c.Addresses) == 0 {
		return nil, errors.New("expected at least one broker address")
	}

	if c.WriteBatchSize == 0 {
		c.WriteBatchSize = defaultWriteBatchSize
	}

	if c.WriteBatchTimeout == 0 {
		c.WriteBatchTimeout = defaultWriteBatchTimeout
	}

	if c.WriteMaxRetryCount == 0 {
		c.WriteMaxRetryCount = defaultMaxRetryCount
	}

	if c.WriteRetryJitterPercent == 0 {
		c.WriteRetryJitterPercent = defaultRetryJitterPercent
	}

	if c.WriteRetryTimeout == 0 {
		c.WriteRetryTimeout = defaultRetryTimeout
	}

	return c, nil
}
