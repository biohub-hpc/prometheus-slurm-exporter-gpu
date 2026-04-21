/* Copyright 2017-2020 Victor Penso, Matteo Dessalvi, Joeri Hermans

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
        "flag"
        "net/http"
        "time"
        "github.com/prometheus/client_golang/prometheus"
        "github.com/prometheus/client_golang/prometheus/promhttp"
        "github.com/prometheus/common/log"
)

// Collector registration moved to main() to use CachingCollector

var listenAddress = flag.String(
        "listen-address",
        ":8080",
        "The address to listen on for HTTP requests.")

var gpuAcct = flag.Bool(
        "gpus-acct",
        false,
        "Enable GPUs accounting")

var jobHistory = flag.Bool(
        "job-history",
        false,
        "Enable historical job metrics (may impact performance)")

var cacheSeconds = flag.Int(
        "cache-interval",
        30,
        "Interval in seconds between background Slurm command refreshes")

func main() {
        flag.Parse()

        // Initialize and fill the Slurm command cache before serving.
        // This runs all Slurm commands in parallel and caches the output.
        slurmCache = NewSlurmCache()
        log.Infof("Performing initial cache refresh (all Slurm commands in parallel)...")
        slurmCache.RefreshAll(*gpuAcct)
        log.Infof("Initial cache refresh complete")

        // Build the list of all collectors
        collectors := []prometheus.Collector{
                NewAccountsCollector(),   // from accounts.go
                NewCPUsCollector(),       // from cpus.go
                NewNodesCollector(),      // from nodes.go
                NewNodeCollector(),       // from node.go
                NewPartitionsCollector(), // from partitions.go
                NewQueueCollector(),      // from queue.go
                NewSchedulerCollector(),  // from scheduler.go
                NewFairShareCollector(),  // from sshare.go
                NewUsersCollector(),      // from users.go
                NewGresCollector(),       // from gres.go
        }
        if *gpuAcct {
                collectors = append(collectors, NewGPUsCollector()) // from gpus.go
        }

        // Wrap all collectors in a CachingCollector that pre-builds metrics
        // in the background so Prometheus scrapes are instant.
        cachingCollector := NewCachingCollector(collectors...)
        prometheus.MustRegister(cachingCollector)

        // Initial metrics parse (runs all Collect methods once)
        log.Infof("Performing initial metrics parse...")
        cachingCollector.Refresh()
        log.Infof("Initial metrics parse complete")

        // Start background refresh: fetch raw data then parse metrics
        interval := time.Duration(*cacheSeconds) * time.Second
        go func() {
                ticker := time.NewTicker(interval)
                defer ticker.Stop()
                for range ticker.C {
                        slurmCache.RefreshAll(*gpuAcct)
                        cachingCollector.Refresh()
                }
        }()

        // Only enable job history if flag is set (since it can be expensive)
        if *jobHistory {
                log.Info("Job history metrics enabled - this may impact performance")
        }

        // The Handler function provides a default handler to expose metrics
        // via an HTTP server. "/metrics" is the usual endpoint for that.
        log.Infof("Starting Server: %s", *listenAddress)
        log.Infof("GPUs Accounting: %t", *gpuAcct)
        log.Infof("Job History: %t", *jobHistory)
        log.Infof("Cache Interval: %ds", *cacheSeconds)
        http.Handle("/metrics", promhttp.Handler())
        log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
