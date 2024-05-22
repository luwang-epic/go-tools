package cron

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Service interface {
	Peek() int64
	Run(ctx context.Context) error
	WakeCh() chan bool
}

type Scheduler struct {
	logger   *zap.Logger
	wg       *sync.WaitGroup
	services []Service
}

func NewScheduler(logger *zap.Logger) *Scheduler {
	return &Scheduler{
		logger: logger,
		wg:     &sync.WaitGroup{},
	}
}

func (s *Scheduler) Schedule(service Service) {
	s.services = append(s.services, service)
}

func (s *Scheduler) Start(ctx context.Context) {
	s.logger.Info("Cron scheduler is going to start")
	for _, service := range s.services {
		s.schedule(ctx, service)
	}
}

func (s *Scheduler) Stop() {
	s.logger.Info("cron scheduler is going to stop")
	s.wg.Wait()
	s.logger.Info("cron scheduler is stopped")
}

func (s *Scheduler) schedule(ctx context.Context, service Service) {
	s.logger.Info(fmt.Sprintf("start to schedule %T", service))
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		wake := service.WakeCh()
		for {
			now := time.Now()
			next := service.Peek()
			if next <= 0 {
				next = now.Add(5 * time.Minute).UnixMilli()
			}
			if next > now.UnixMilli() {
				select {
				case <-wake:
					continue
				case <-time.After(time.Duration(next-now.UnixMilli()) * time.Millisecond):
					break
				case <-ctx.Done():
					return
				}
			}
			if err := service.Run(ctx); err != nil {
				select {
				case <-time.After(3 * time.Second):
					// If some error occurs, wait for a while to retry.
				case <-ctx.Done():
					return
				}
			}
		}
	}()
}
