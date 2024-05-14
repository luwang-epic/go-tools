package controller

import (
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GrpcApp struct {
	logger *zap.Logger
	wg     *sync.WaitGroup
	serv   *grpc.Server
}

func NewGrpcApp(logger *zap.Logger) *GrpcApp {
	return &GrpcApp{
		logger: logger,
		wg:     &sync.WaitGroup{},
		serv:   grpc.NewServer(),
	}
}

func (app *GrpcApp) ServiceRegistrar() grpc.ServiceRegistrar {
	return app.serv
}

func (app *GrpcApp) Serve(port int) {
	app.logger.Info("grpc server is going to start", zap.Int("port", port))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		app.logger.Panic("fail to listen for grpc server", zap.Error(err))
	}
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		if err := app.serv.Serve(lis); err != nil {
			app.logger.Panic("fail to start grpc server", zap.Error(err))
		}
	}()
}

func (app *GrpcApp) Stop() {
	app.logger.Info("grpc server is going to stop")
	app.serv.Stop()
	app.wg.Wait()
}
