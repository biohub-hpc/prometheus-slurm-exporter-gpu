/* Copyright 2017 Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type QueueMetrics struct {
	pending     float64
	pending_dep float64
	running     float64
	suspended   float64
	completing  float64
	configuring float64
}

// Returns the scheduler metrics
func QueueGetMetrics() *QueueMetrics {
	return ParseQueueMetrics(QueueData())
}

func ParseQueueMetrics(input []byte) *QueueMetrics {
	var qm QueueMetrics
	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if strings.Contains(line, ",") {
			splitted := strings.Split(line, ",")
			state := splitted[1]
			switch state {
			case "PENDING":
				qm.pending++
				if len(splitted) > 2 && splitted[2] == "Dependency" {
					qm.pending_dep++
				}
			case "RUNNING":
				qm.running++
			case "SUSPENDED":
				qm.suspended++
			case "COMPLETING":
				qm.completing++
			case "CONFIGURING":
				qm.configuring++
			}
		}
	}
	return &qm
}

// Execute the squeue command and return its output
func QueueData() []byte {
	return GetCached("squeue_queue")
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm queue metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewQueueCollector() *QueueCollector {
	return &QueueCollector{
		pending:     prometheus.NewDesc("slurm_queue_pending", "Pending jobs in queue", nil, nil),
		pending_dep: prometheus.NewDesc("slurm_queue_pending_dependency", "Pending jobs because of dependency in queue", nil, nil),
		running:     prometheus.NewDesc("slurm_queue_running", "Running jobs in the cluster", nil, nil),
		suspended:   prometheus.NewDesc("slurm_queue_suspended", "Suspended jobs in the cluster", nil, nil),
		completing:  prometheus.NewDesc("slurm_queue_completing", "Completing jobs in the cluster", nil, nil),
		configuring: prometheus.NewDesc("slurm_queue_configuring", "Configuring jobs in the cluster", nil, nil),
	}
}

type QueueCollector struct {
	pending     *prometheus.Desc
	pending_dep *prometheus.Desc
	running     *prometheus.Desc
	suspended   *prometheus.Desc
	completing  *prometheus.Desc
	configuring *prometheus.Desc
}

func (qc *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- qc.pending
	ch <- qc.pending_dep
	ch <- qc.running
	ch <- qc.suspended
	ch <- qc.completing
	ch <- qc.configuring
}

func (qc *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	qm := QueueGetMetrics()
	ch <- prometheus.MustNewConstMetric(qc.pending, prometheus.GaugeValue, qm.pending)
	ch <- prometheus.MustNewConstMetric(qc.pending_dep, prometheus.GaugeValue, qm.pending_dep)
	ch <- prometheus.MustNewConstMetric(qc.running, prometheus.GaugeValue, qm.running)
	ch <- prometheus.MustNewConstMetric(qc.suspended, prometheus.GaugeValue, qm.suspended)
	ch <- prometheus.MustNewConstMetric(qc.completing, prometheus.GaugeValue, qm.completing)
	ch <- prometheus.MustNewConstMetric(qc.configuring, prometheus.GaugeValue, qm.configuring)
}
