package kafkacommon

import (
	"context"

	"github.com/explicitnull/promcommon"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/sethvargo/go-retry"
)

type Client struct {
	config  *Config
	writer  *kafka.Writer
	metrics promcommon.BrokerIncrementObserver
}

func NewClient(config *Config, metrics promcommon.BrokerIncrementObserver) (*Client, error) {
	config, err := config.WithDefaults()
	if err != nil {
		return nil, err
	}

	return &Client{
		config: config,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(config.Addresses...),
			RequiredAcks: kafka.RequireOne,
			Topic:        config.WriteTopic,
			BatchSize:    config.WriteBatchSize,
			BatchTimeout: config.WriteBatchTimeout,
		},

		metrics: metrics,
	}, nil
}

func (c *Client) Write(ctx context.Context, message kafka.Message) error {
	retrier := retry.WithJitterPercent(
		c.config.WriteRetryJitterPercent,
		retry.WithMaxRetries(c.config.WriteMaxRetryCount,
			retry.NewFibonacci(c.config.WriteRetryTimeout),
		),
	)

	retryable := func(ctx context.Context) error {
		c.metrics.IncBrokerMessagesWrites(message.Topic, promcommon.Attempt)

		timer := c.metrics.NewBrokerWriteTimer(message.Topic)
		defer timer.ObserveDuration()

		err := c.writer.WriteMessages(ctx, message)
		if err != nil {
			c.metrics.IncBrokerMessagesWrites(message.Topic, promcommon.Failure)

			return errors.Wrap(retry.RetryableError(err), "write messages")
		}

		return nil
	}

	err := retry.Do(ctx, retrier, retryable)
	if err != nil {
		return errors.Wrap(err, "all retries failed")
	}
	return nil
}
