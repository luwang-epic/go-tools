package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Consumer struct {
	wg      *sync.WaitGroup
	client  sarama.ConsumerGroup
	handler *Handler
	topics  []string
	logger  *zap.Logger
}

func NewConsumer(logger *zap.Logger, brokers []string, topics []string, handler *Handler) *Consumer {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	client, err := sarama.NewConsumerGroup(brokers, "alarm-task-group", config)
	if err != nil {
		logger.Panic("fail to connect to kafka", zap.Error(err))
	}
	logger.Info("created kafka client", zap.Strings("brokers", brokers), zap.Strings("topics", topics))
	return &Consumer{
		wg:      &sync.WaitGroup{},
		client:  client,
		handler: handler,
		topics:  topics,
		logger:  logger,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("kafka consumer is going to start")
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side re-balance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.client.Consume(ctx, c.topics, c.handler); err != nil {
				c.logger.Warn("fail to consume kafka", zap.Error(err))
				time.Sleep(1 * time.Second)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			c.handler.ready = make(chan bool)
		}
	}()

	<-c.handler.ready // Wait till the service has been set up
	c.logger.Info("kafka consumer up and running")
}

func (c *Consumer) Stop() {
	c.logger.Info("kafka consumer is going to stop")
	c.wg.Wait()
	if err := c.client.Close(); err != nil {
		c.logger.Warn("fail to close kafka client", zap.Error(err))
	}
}
