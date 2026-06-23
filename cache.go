/* Copyright 2024 Chan Zuckerberg Biohub

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
	"io/ioutil"
	"os/exec"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

// CacheEntry holds the raw output of a single Slurm command
type CacheEntry struct {
	Output     []byte
	LastUpdate time.Time
	Err        error
}

// SlurmCache is a thread-safe cache for Slurm command outputs
type SlurmCache struct {
	mu      sync.RWMutex
	entries map[string]*CacheEntry
}

// CmdSpec defines a Slurm command to be cached
type CmdSpec struct {
	Key  string
	Cmd  string
	Args []string
}

// Observability metrics for the cache itself
var (
	cacheRefreshDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "slurm_exporter_cache_refresh_seconds",
		Help: "Time taken for the last complete cache refresh",
	})
	cacheRefreshTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurm_exporter_cache_refresh_total",
		Help: "Total number of cache refresh cycles completed",
	})
	cacheRefreshErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "slurm_exporter_cache_refresh_errors_total",
		Help: "Total command failures during cache refresh",
	}, []string{"command"})
	cacheLastRefreshTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "slurm_exporter_cache_last_refresh_timestamp",
		Help: "Unix timestamp of last successful cache refresh",
	})
)

func init() {
	prometheus.MustRegister(cacheRefreshDuration)
	prometheus.MustRegister(cacheRefreshTotal)
	prometheus.MustRegister(cacheRefreshErrors)
	prometheus.MustRegister(cacheLastRefreshTimestamp)
}

// Global cache instance, initialized in main()
var slurmCache *SlurmCache

// allCommands returns the complete list of Slurm commands to cache.
// When gpuEnabled is true, GPU-specific commands are included.
func allCommands(gpuEnabled bool) []CmdSpec {
	cmds := []CmdSpec{
		// sinfo commands
		{Key: "sinfo_cpus", Cmd: "sinfo", Args: []string{"-h", "-o", "%C"}},
		{Key: "sinfo_nodes", Cmd: "sinfo", Args: []string{"-h", "-o", "%D,%T"}},
		{Key: "sinfo_node", Cmd: "sinfo", Args: []string{"-h", "-N", "-O",
			"NodeList,AllocMem,Memory,CPUsState,StateLong,Gres:30,GresUsed:30,Partition"}},
		{Key: "sinfo_partitions", Cmd: "sinfo", Args: []string{"-h", "-o%R,%C"}},
		{Key: "sinfo_node_partition_map", Cmd: "sinfo", Args: []string{"-h", "-N", "-o", "%N %P"}},
		{Key: "sinfo_gres", Cmd: "sinfo", Args: []string{"-h", "-N", "-O",
			"NodeHost,Gres:50,GresUsed:50,StateLong"}},

		// squeue commands
		{Key: "squeue_accounts", Cmd: "squeue", Args: []string{"-a", "-r", "-h",
			"-o", "%A|%a|%T|%C"}},
		{Key: "squeue_partitions_pending", Cmd: "squeue", Args: []string{"-a", "-r", "-h",
			"-o%P", "--states=PENDING"}},
		{Key: "squeue_partitions_waittime", Cmd: "squeue", Args: []string{"-h", "-r",
			"-t", "RUNNING", "--Format",
			"jobid,username,partition,eligibletime,starttime,tres-alloc:100"}},
		{Key: "squeue_queue", Cmd: "squeue", Args: []string{"-a", "-r", "-h",
			"-o", "%A,%T,%r", "--states=all"}},
		{Key: "squeue_users_basic", Cmd: "squeue", Args: []string{"-a", "-r", "-h",
			"-o", "%A|%u|%T|%C"}},
		{Key: "squeue_users_complete", Cmd: "squeue", Args: []string{"-a", "-r", "-h",
			"-o", "%i|%u|%P|%j|%T|%M|%L|%S|%p|%q|%N"}},
		{Key: "squeue_users_tres", Cmd: "squeue", Args: []string{"-a", "-r", "-h",
			"--Format=JobID:20,tres-alloc:100"}},

		// Other commands
		{Key: "sdiag", Cmd: "sdiag", Args: []string{}},
		{Key: "sshare", Cmd: "sshare", Args: []string{"-n", "-P", "-o", "account,fairshare"}},
	}

	if gpuEnabled {
		cmds = append(cmds, []CmdSpec{
			{Key: "sinfo_gpu_mapping", Cmd: "sinfo", Args: []string{"-h", "-o", "%N %G"}},
			{Key: "squeue_gpu_alloc", Cmd: "squeue", Args: []string{"-t", "RUNNING", "-h",
				"--Format=JobID:20,username:20,tres-alloc:100,tres-per-node:30,NodeList:40"}},
			{Key: "sacct_gpu", Cmd: "sacct", Args: []string{"-a", "-X", "--format=AllocTRES",
				"--state=RUNNING", "--noheader", "--parsable2"}},
			{Key: "sinfo_gpu_total", Cmd: "sinfo", Args: []string{"-h", "-o", "%n %G"}},
		}...)
	}

	return cmds
}

func NewSlurmCache() *SlurmCache {
	return &SlurmCache{
		entries: make(map[string]*CacheEntry),
	}
}

// Get returns the cached output for a key. Returns nil if not yet cached.
func (c *SlurmCache) Get(key string) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if entry, ok := c.entries[key]; ok && entry.Output != nil {
		return entry.Output
	}
	return nil
}

// set stores command output under the given key.
// On failure, retains the old cached data if present.
func (c *SlurmCache) set(key string, output []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		if existing, ok := c.entries[key]; ok && existing.Output != nil {
			log.Warnf("Cache: command %s failed (%v), keeping previous cached data", key, err)
			existing.Err = err
			return
		}
	}
	c.entries[key] = &CacheEntry{
		Output:     output,
		LastUpdate: time.Now(),
		Err:        err,
	}
}

// executeCommand runs a single command and returns its output.
// Returns an error instead of calling log.Fatal so the cache can retain stale data.
func executeCommand(cmd string, args []string) ([]byte, error) {
	command := exec.Command(cmd, args...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := command.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// RefreshAll runs ALL commands in parallel and waits for completion.
func (c *SlurmCache) RefreshAll(gpuEnabled bool) {
	cmds := allCommands(gpuEnabled)
	var wg sync.WaitGroup

	start := time.Now()
	log.Infof("Cache refresh starting: %d commands", len(cmds))

	for _, spec := range cmds {
		wg.Add(1)
		go func(s CmdSpec) {
			defer wg.Done()
			cmdStart := time.Now()
			output, err := executeCommand(s.Cmd, s.Args)
			elapsed := time.Since(cmdStart)
			if err != nil {
				log.Warnf("Cache: %s failed in %v: %v", s.Key, elapsed, err)
				cacheRefreshErrors.WithLabelValues(s.Key).Inc()
			} else {
				log.Debugf("Cache: %s refreshed in %v (%d bytes)", s.Key, elapsed, len(output))
			}
			c.set(s.Key, output, err)
		}(spec)
	}

	wg.Wait()

	elapsed := time.Since(start)
	cacheRefreshDuration.Set(elapsed.Seconds())
	cacheRefreshTotal.Inc()
	cacheLastRefreshTimestamp.Set(float64(time.Now().Unix()))
	log.Infof("Cache refresh complete in %v", elapsed)
}

// StartBackgroundRefresh launches a goroutine that periodically refreshes the cache.
func (c *SlurmCache) StartBackgroundRefresh(interval time.Duration, gpuEnabled bool) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			c.RefreshAll(gpuEnabled)
		}
	}()
}

// GetCached is the main getter used by all collector data functions.
func GetCached(key string) []byte {
	if slurmCache == nil {
		log.Warnf("Cache not initialized, returning empty data for key %s", key)
		return []byte{}
	}
	data := slurmCache.Get(key)
	if data == nil {
		log.Warnf("Cache miss for key %s", key)
		return []byte{}
	}
	return data
}

// CachingCollector wraps multiple Prometheus collectors and caches their
// metric output. The heavy parsing work runs in the background via Refresh(),
// while Collect() instantly replays the pre-built metrics.
type CachingCollector struct {
	collectors []prometheus.Collector
	mu         sync.RWMutex
	metrics    []prometheus.Metric
}

func NewCachingCollector(collectors ...prometheus.Collector) *CachingCollector {
	return &CachingCollector{
		collectors: collectors,
	}
}

// Describe forwards descriptor declarations from all wrapped collectors.
func (cc *CachingCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range cc.collectors {
		c.Describe(ch)
	}
}

// Collect replays the pre-built cached metrics instantly.
func (cc *CachingCollector) Collect(ch chan<- prometheus.Metric) {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	for _, m := range cc.metrics {
		ch <- m
	}
}

// Refresh runs the real Collect() on all wrapped collectors in the background,
// capturing the metrics they produce and storing them for fast replay.
func (cc *CachingCollector) Refresh() {
	start := time.Now()

	ch := make(chan prometheus.Metric, 100000)
	done := make(chan struct{})

	var metrics []prometheus.Metric
	go func() {
		for m := range ch {
			metrics = append(metrics, m)
		}
		close(done)
	}()

	for _, c := range cc.collectors {
		c.Collect(ch)
	}
	close(ch)
	<-done

	cc.mu.Lock()
	cc.metrics = metrics
	cc.mu.Unlock()

	log.Infof("Metrics refresh complete in %v (%d metrics cached)", time.Since(start), len(metrics))
}
