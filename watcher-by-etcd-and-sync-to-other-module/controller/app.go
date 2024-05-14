package controller

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type App struct {
	logger      *zap.Logger
	srv         *http.Server
	controllers []Controller
	middlewares []gin.HandlerFunc
}

type Controller interface {
	Route(r *gin.Engine)
}

func NewApp(logger *zap.Logger) *App {
	return &App{
		logger: logger,
	}
}

func (app *App) Serve(port int) {
	app.logger.Info("http server is going to start", zap.Int("port", port))
	r := gin.New()
	// Install middlewares.
	r.Use(app.middlewares...)
	// Install metrics
	r.GET("/metrics", app.metricsHandler)
	// Install controllers.
	for _, c := range app.controllers {
		c.Route(r)
	}
	app.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: r,
	}
	go func() {
		if err := app.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			app.logger.Panic("fail to start http server", zap.Error(err))
		}
	}()
}

func (app *App) ServeTLS(port int, cert string, key string) {
	app.logger.Info("https server is going to start", zap.Int("port", port))
	r := gin.New()
	// Install middlewares.
	r.Use(app.middlewares...)
	// Install controllers.
	for _, c := range app.controllers {
		c.Route(r)
	}
	app.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: r,
	}
	go func() {
		if err := app.srv.ListenAndServeTLS(cert, key); err != nil && err != http.ErrServerClosed {
			app.logger.Panic("fail to start https server", zap.Error(err))
		}
	}()
}

func (app *App) AddController(c Controller) {
	app.controllers = append(app.controllers, c)
}

func (app *App) AddMiddleware(m gin.HandlerFunc) {
	app.middlewares = append(app.middlewares, m)
}

func (app *App) metricsHandler(c *gin.Context) {
	promhttp.Handler().ServeHTTP(c.Writer, c.Request)
}

func (app *App) Shutdown(timeout time.Duration) {
	// Shutdown the http server with a timeout of 30 seconds.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	app.logger.Info("http server is going to stop")
	if err := app.srv.Shutdown(ctx); err != nil {
		app.logger.Warn("shutdown the server forcefully", zap.Error(err))
	} else {
		app.logger.Info("shutdown the server gracefully")
	}
}
