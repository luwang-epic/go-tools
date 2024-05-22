package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"
	"icode.baidu.com/baidu/bce/go-common-lib/log"

	"go-tools/scheduler-by-etcd-and-kafka/master/cron"
	"go-tools/scheduler-by-etcd-and-kafka/master/index"
	"go-tools/scheduler-by-etcd-and-kafka/master/kafka"
	"go-tools/scheduler-by-etcd-and-kafka/master/model"
	alarm "go-tools/scheduler-by-etcd-and-kafka/master/task"
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
		Endpoints               []string `yaml:"endpoints"`
		Username                string   `yaml:"username,omitempty"`
		Password                string   `yaml:"password,omitempty"`
		EnableTLS               bool     `yaml:"enableTLS,omitempty"`
		CaFile                  string   `yaml:"ca_file,omitempty"`
		CertFile                string   `yaml:"cert_file,omitempty"`
		KeyFile                 string   `yaml:"key_file,omitempty"`
		DialTimeoutMilliSecond  int      `yaml:"dialTimeoutMilliSecond"`
		QueryTimeoutMilliSecond int      `yaml:"queryTimeoutMilliSecond"`
	} `yaml:"etcd"`
	Model struct {
		WatchBaseBatch       int    `yaml:"watchBaseBatch"`
		WatchPrefix          string `yaml:"watchPrefix"`
		AlarmPolicyKeyPrefix string `yaml:"alarmPolicyKeyPrefix"`
	} `yaml:"model"`
	Kafka struct {
		Brokers  []string `yaml:"brokers"`
		Topics   []string `yaml:"topics"`
		Producer struct {
			MaxRetries int `yaml:"maxRetries"`
		} `yaml:"producer"`
	}
	AlarmTaskManager struct {
		Topic string `yaml:"topic"`
	} `yaml:"alarmTaskManager"`
}

func main() {
	var configPrefix string
	var env string
	flag.StringVar(&configPrefix, "config-prefix", "config", "prefix of config file")
	flag.StringVar(&env, "env", "local", "environment")
	flag.Parse()

	configPath := fmt.Sprintf("scheduler-by-etcd-and-kafka/master/config/%s.%s.yml", configPrefix, env)
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
	ctx, cancel := context.WithCancel(context.Background())

	// Init etcd.
	dialTimeout := time.Duration(cfg.Etcd.DialTimeoutMilliSecond) * time.Millisecond
	queryTimeout := time.Duration(cfg.Etcd.QueryTimeoutMilliSecond) * time.Millisecond
	var etcdTLS *tls.Config
	if cfg.Etcd.EnableTLS {
		logger.Info("etcd tls enabled")
		pemData, err := os.ReadFile(cfg.Etcd.CaFile)
		if err != nil {
			logger.Panic("fail to new etcd client", zap.Error(err))
		}
		roots := x509.NewCertPool()
		if ok := roots.AppendCertsFromPEM(pemData); !ok {
			logger.Panic("fail to new etcd client", zap.Error(err))
		}
		cert, err := tls.LoadX509KeyPair(cfg.Etcd.CertFile, cfg.Etcd.KeyFile)
		if err != nil {
			logger.Panic("fail to new etcd client", zap.Error(err))
		}
		etcdTLS = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      roots,
		}
	}
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: dialTimeout,
		Username:    cfg.Etcd.Username,
		Password:    cfg.Etcd.Password,
		TLS:         etcdTLS,
	})
	if err != nil {
		logger.Panic("fail to new etcd client", zap.Error(err))
	}
	defer func() { _ = etcdClient.Close() }()
	etcdWatcher := model.NewEtcdWatcher(logger, etcdClient, cfg.Model.WatchPrefix, cfg.Model.WatchBaseBatch)
	em := model.NewEtcdModel(logger, etcdClient, queryTimeout, etcdWatcher)
	logger.Info("connected to etcd", zap.Strings("endpoints", cfg.Etcd.Endpoints))

	// Init models.
	alarmPolicyModel := model.NewAlarmPolicyModel(logger, em, cfg.Model.AlarmPolicyKeyPrefix)

	// Init indexes.
	alarmIndex := index.NewAlarmPolicyIndex()

	// Init kafka producer.
	kafkaProducer := kafka.NewProducer(logger, cfg.Kafka.Brokers, cfg.Kafka.Producer.MaxRetries)
	if err := kafkaProducer.Start(ctx); err != nil {
		logger.Panic("fail to start kafka producer", zap.Error(err))
	}

	// Init alarm task manager.
	alarmTaskManager := alarm.NewTaskManager(logger, alarmPolicyModel, alarmIndex, kafkaProducer, cfg.AlarmTaskManager.Topic)

	// Add watchers of alarm policies.
	alarmPolicyModel.AddWatcher(alarmIndex)
	alarmIndex.AddWatcher(alarmTaskManager)
	modelGroup := etcdWatcher.AddGroup()
	modelGroup.AddWatcher(cfg.Model.AlarmPolicyKeyPrefix, alarmPolicyModel)
	etcdWatcher.Start(ctx)

	// Init cron scheduler.
	cronScheduler := cron.NewScheduler(logger)
	cronScheduler.Schedule(alarmTaskManager)
	go cronScheduler.Start(ctx)

	// Wait for interrupt signal to GRACEFULLY shut down the server.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Shutdown all services.
	cancel()
	etcdWatcher.Stop()
	kafkaProducer.Stop()
	cronScheduler.Stop()
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
