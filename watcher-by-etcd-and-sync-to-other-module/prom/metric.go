package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ServerStartTime = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "server",
		Name:      "start_time",
		Help:      "Start timestamp of the server",
	})
)
