package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"

	"go-tools/watcher-by-etcd-and-sync-to-other-module/auth-as-other-module/index"
	"go-tools/watcher-by-etcd-and-sync-to-other-module/auth-as-other-module/watching"
)

type config struct {
	Logging struct {
		LogToStderr bool   `yaml:"logToStderr"`
		Filename    string `yaml:"filename"`
		MaxSize     int    `yaml:"maxSize"`
		MaxBackups  int    `yaml:"maxBackups"`
		MaxAge      int    `yaml:"maxAge"`
		Level       string `yaml:"level"`
	} `yaml:"logging"`
	Watching struct {
		Host          string `yaml:"host"`
		UserKeyPrefix string `yaml:"userKeyPrefix"`
	} `yaml:"watching"`
}

// main 其他的模块，例如权限，需要同步用户信息
func main() {
	var configPrefix string
	var env string
	flag.StringVar(&configPrefix, "config-prefix", "config", "prefix of config file")
	flag.StringVar(&env, "env", "local", "environment")
	flag.Parse()

	configPath := fmt.Sprintf("watcher-by-etcd-and-sync-to-other-module/auth-as-other-module/config/%s.%s.yml", configPrefix, env)
	bytes, err := os.ReadFile(configPath)
	if err != nil {
		log.Panicf("fail to read config=%s, err=%v", configPath, err)
	}
	var cfg config
	if err = yaml.Unmarshal(bytes, &cfg); err != nil {
		log.Panicf("fail to load conf as yaml, err=%v", err)
	}

	// Init logger
	logger, err := newLogger(&cfg)
	if err != nil {
		log.Panicf("fail to new logger, err=%v", err)
	}
	defer func() {
		_ = logger.Sync() // flushes buffer, if any
	}()

	// The context is used to quit all goroutines gracefully.
	_, cancel := context.WithCancel(context.Background())

	// Start watcher.
	watcher := watching.NewWatcher(logger, cfg.Watching.Host, []string{cfg.Watching.UserKeyPrefix})
	alarmPolicyIndex := index.NewUserIndex(logger, cfg.Watching.UserKeyPrefix)
	watcher.AddWatcher(alarmPolicyIndex)
	watcher.Start()

	// Wait for interrupt signal to GRACEFULLY shut down the server.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Shutdown all services.
	cancel()
}

func newLogger(cfg *config) (*zap.Logger, error) {
	if cfg.Logging.LogToStderr {
		return zap.NewDevelopment()
	}
	if len(cfg.Logging.Filename) == 0 {
		return nil, fmt.Errorf("log filename is required")
	}
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   cfg.Logging.Filename,
		MaxSize:    cfg.Logging.MaxSize,
		MaxBackups: cfg.Logging.MaxBackups,
		MaxAge:     cfg.Logging.MaxAge,
	})
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000")
	level := zap.InfoLevel
	if cfg.Logging.Level == "debug" {
		level = zap.DebugLevel
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		w,
		level,
	)
	return zap.New(core), nil
}
