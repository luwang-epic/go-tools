package service

import (
	"context"

	"go.uber.org/zap"
)

type LeaderService struct {
	logger *zap.Logger
}

func NewLeaderService(logger *zap.Logger) *LeaderService {
	return &LeaderService{logger: logger}
}

func (ls *LeaderService) Start(ctx context.Context) {
	ls.logger.Info("leader service start", zap.String("name", "leader"))
}

func (ls *LeaderService) Stop() {
	ls.logger.Info("leader service stop", zap.String("name", "leader"))
}
