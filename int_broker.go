package kafkacommon

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Broker interface {
	Write(ctx context.Context, messages []kafka.Message) error
}
