// pkg/metrics/metrics.go
package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// ActiveSubscriptions tracks the number of active subscriptions.
	ActiveSubscriptions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "active_subscriptions",
		Help: "Current number of active subscriptions.",
	})
	// (You can add more metrics such as update rates or gRPC latency here.)
)

func init() {
	prometheus.MustRegister(ActiveSubscriptions)
}

// StartHTTPServer starts an HTTP server to serve Prometheus metrics.
func StartHTTPServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Printf("Starting Prometheus metrics server on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("Error starting metrics HTTP server: %v", err)
		}
	}()
}
