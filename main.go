package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"net/http"
	"regexp"
)

type MetricsReport struct {
	Header struct {
		JobID              string `json:"job-id"`
		ExecEnvContainerID string `json:"exec-env-container-id"`
		SamzaVersion       string `json:"samza-version"`
		JobName            string `json:"job-name"`
		Host               string `json:"host"`
		ResetTime          int64  `json:"reset-time"`
		ContainerName      string `json:"container-name"`
		Source             string `json:"source"`
		Time               int64  `json:"time"`
		Version            string `json:"version"`
	} `json:"header"`
	Metrics map[string]map[string]interface{} `json:"metrics"`
}

func recordMetrics(mr *MetricsReport, metricsRegistry map[string]prometheus.Metric) {
	prefix := fmt.Sprintf("%s_%s_%s", mr.Header.JobName, mr.Header.JobID, mr.Header.ContainerName)
	if mr.Header.ContainerName != mr.Header.Source {
		prefix = fmt.Sprintf("%s_%s", prefix, mr.Header.Source)
	}

	for group, metrics := range mr.Metrics {
		for metricName, val := range metrics {
			fqn := fmt.Sprintf("%s_%s_%s", prefix, group, metricName)
			if regex, err := regexp.Compile("[^a-zA-Z0-9_]"); err == nil {
				fqn = regex.ReplaceAllString(fqn, "_")
			} else {
				log.Fatal(err)
			}

			switch val := val.(type) {
			case json.Number:
				fval, err := val.Float64()
				if err != nil {
					log.Fatal(err)
				}
				var gauge prometheus.Gauge
				if g, found := metricsRegistry[fqn]; found {
					if g, ok := g.(prometheus.Gauge); ok {
						gauge = g
					} else {
						log.Fatal("Metrics cannot switch between types")
					}
				} else {
					gauge = promauto.NewGauge(prometheus.GaugeOpts{Name: fqn})
					metricsRegistry[fqn] = gauge
				}
				gauge.Set(fval)
			case bool:
				fmt.Printf("Won't report bool metric: %s - %v", fqn, val)
			}
		}
	}
}

func consumeMetricsStream() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "samza-metrics-collector",
		"auto.offset.reset": "latest",
	})

	if err != nil {
		panic(err)
	}

	if err := c.SubscribeTopics([]string{"metrics"}, nil); err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	metricsRegistry := map[string]prometheus.Metric{}

	for {
		if msg, err := c.ReadMessage(-1); err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			metricsReport := &MetricsReport{}

			d := json.NewDecoder(bytes.NewReader(msg.Value))
			d.UseNumber()
			if err := d.Decode(metricsReport); err != nil {
				log.Fatal(err)
			}
			recordMetrics(metricsReport, metricsRegistry)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func main() {
	go consumeMetricsStream()

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
