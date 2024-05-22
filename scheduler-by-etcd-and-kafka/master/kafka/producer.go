package kafka

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

type Producer interface {
	Produce(topic string, key string, value []byte)
}

type ProducerImpl struct {
	logger     *zap.Logger
	brokers    []string
	maxRetries int
	wg         *sync.WaitGroup
	producer   sarama.AsyncProducer
}

var (
	kafkaFailedMessageCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "kafka",
		Name:      "failed_message_count",
		Help:      "The failed kafka message count",
	})
)

func NewProducer(logger *zap.Logger, brokers []string, maxRetries int) *ProducerImpl {
	return &ProducerImpl{
		logger:     logger,
		brokers:    brokers,
		maxRetries: maxRetries,
		wg:         &sync.WaitGroup{},
	}
}

func (p *ProducerImpl) Start(ctx context.Context) error {
	p.logger.Info("kafka producer is going to start")
	config := sarama.NewConfig()
	config.Producer.Retry.Max = p.maxRetries
	config.Producer.Return.Successes = false
	producer, err := sarama.NewAsyncProducer(p.brokers, config)
	if err != nil {
		return err
	}
	p.producer = producer
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case err := <-p.producer.Errors():
				p.logger.Warn("kafka error", zap.Error(err))
				kafkaFailedMessageCount.Inc()
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (p *ProducerImpl) Stop() {
	p.logger.Info("kafka producer is going to stop")
	p.wg.Wait()
}

func (p *ProducerImpl) Produce(topic string, key string, value []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if len(key) > 0 {
		msg.Key = sarama.StringEncoder(key)
	}
	p.producer.Input() <- msg
}
