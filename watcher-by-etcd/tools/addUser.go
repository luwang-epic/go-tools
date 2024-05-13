package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hashicorp/go-uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"

	"go-tools/watcher-by-etcd/pb"
	"go-tools/watcher-by-etcd/prom"
	"go-tools/watcher-by-etcd/watcher"
)

type config struct {
	Application struct {
		Port      int `yaml:"port"`
		AdminPort int `yaml:"adminPort"`
	} `yaml:"application"`
	Watching struct {
		Port                  int     `yaml:"port"`
		Batch                 int     `yaml:"batch"`
		CompactionThreshold   float64 `yaml:"compactionThreshold"`
		CompactionIntervalMin int     `yaml:"compactionIntervalMin"`
	} `yaml:"watching"`
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
		WatchBaseBatch int    `yaml:"watchBaseBatch"`
		WatchPrefix    string `yaml:"watchPrefix"`
		UserPrefix     string `yaml:"userPrefix"`
	} `yaml:"model"`
}

// main 用户添加一个用户，用来验证监听机制是否生效
func main() {
	var configPrefix string
	var env string
	flag.StringVar(&configPrefix, "config-prefix", "config", "prefix of config file")
	flag.StringVar(&env, "env", "local", "environment")
	flag.Parse()

	configPath := fmt.Sprintf("watcher-by-etcd/config/%s.%s.yml", configPrefix, env)
	bytes, err := os.ReadFile(configPath)
	if err != nil {
		log.Panicf("fail to read config=%s, err=%v", configPath, err)
	}
	var cfg config
	if err = yaml.Unmarshal(bytes, &cfg); err != nil {
		log.Panicf("fail to load conf as yaml, err=%v", err)
	}
	assignDefaults(&cfg)
	prom.ServerStartTime.Set(float64(time.Now().Unix()))

	// Init logger
	logger, err := newLogger(&cfg)
	if err != nil {
		log.Panicf("fail to new logger, err=%v", err)
	}
	defer func() {
		_ = logger.Sync() // flushes buffer, if any
	}()

	// The context is used to quit all goroutines gracefully.
	ctx, _ := context.WithCancel(context.Background())

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

	// 监听etcd的变化，同步给其他
	etcdWatcher := watcher.NewEtcdWatcher(logger, etcdClient, cfg.Model.WatchPrefix, cfg.Model.WatchBaseBatch)
	em := watcher.NewEtcdModel(logger, etcdClient, queryTimeout, etcdWatcher)
	logger.Info("connected to etcd", zap.Strings("endpoints", cfg.Etcd.Endpoints))

	// Init models.
	userModel := watcher.NewUserModel(logger, em, cfg.Model.UserPrefix)

	name, _ := uuid.GenerateUUID()
	user := &pb.UserProto{
		Name: name,
		Age:  18,
	}
	_, err = userModel.Add(ctx, user)
	if err != nil {
		logger.Error("add user error", zap.Error(err))
	} else {
		logger.Info("add user success", zap.Any("user", user))
	}
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
	assignIntDefault(&cfg.Model.WatchBaseBatch, 200)
	assignStringDefault(&cfg.Model.WatchPrefix, "/wang/tools/watcher")
	assignStringDefault(&cfg.Model.UserPrefix, "/wang/tools/watcher/user")
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
