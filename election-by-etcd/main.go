package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"

	"go-tools/election-by-etcd/election"
	"go-tools/election-by-etcd/service"
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
	Etcd struct {
		Endpoints              []string `yaml:"endpoints"`
		DialTimeoutMilliSecond int      `yaml:"dialTimeoutMilliSecond"`
	} `yaml:"etcd"`
	Election struct {
		Key string `yaml:"key"`
	} `yaml:"election"`
}

// main the function where execution of the program begins
func main() {
	var configPrefix string
	var env string
	flag.StringVar(&configPrefix, "config-prefix", "config", "prefix of config file")
	flag.StringVar(&env, "env", "local", "environment")
	flag.Parse()

	configPath := fmt.Sprintf("election-by-etcd/config/%s.%s.yml", configPrefix, env)
	bytes, err := os.ReadFile(configPath)
	if err != nil {
		log.Panicf("fail to read config=%s, err=%v", configPath, err)
	}
	var cfg config
	if err = yaml.Unmarshal(bytes, &cfg); err != nil {
		log.Panicf("fail to load conf as yaml, err=%v", err)
	}
	assignDefaults(&cfg)

	// Init logger
	logger, err := newLogger(&cfg)
	if err != nil {
		log.Panicf("fail to new logger, err=%v", err)
	}
	defer func() {
		_ = logger.Sync() // flushes buffer, if any
	}()

	// The context is used to quit all goroutines gracefully.
	ctx, cancel := context.WithCancel(context.Background())

	// Init etcd models.
	dialTimeout := time.Duration(cfg.Etcd.DialTimeoutMilliSecond) * time.Millisecond
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		logger.Panic("fail to new etcd client", zap.Error(err))
	}
	defer func() { _ = etcdClient.Close() }()

	// new leader service
	ls := service.NewLeaderService(logger)

	// Start leader election.
	elector := election.NewElector(logger, etcdClient, cfg.Election.Key)
	elector.RegisterLeaderRoutine(ls)
	elector.Start(ctx)

	// Wait for interrupt signal to GRACEFULLY shut down the server.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Shutdown all services.
	cancel()
	elector.Stop()
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

func assignDefaults(cfg *config) {
	assignStringDefault(&cfg.Logging.Level, "info")
	assignStringDefault(&cfg.Logging.Filename, "log/election-by-etcd.log")
	assignIntDefault(&cfg.Logging.MaxSize, 100)
	assignIntDefault(&cfg.Logging.MaxAge, 7)
	assignIntDefault(&cfg.Logging.MaxBackups, 3)

	assignIntDefault(&cfg.Etcd.DialTimeoutMilliSecond, 10000)
}

func assignStringDefault(k *string, v string) {
	if len(*k) == 0 {
		*k = v
	}
}

func assignIntDefault(k *int, v int) {
	if *k == 0 {
		*k = v
	}
}
