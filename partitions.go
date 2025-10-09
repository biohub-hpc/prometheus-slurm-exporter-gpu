/* Copyright 2020 Victor Penso

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
	"fmt"
        "io/ioutil"
        "os/exec"
        "log"
        "sort"
        "strings"
        "strconv"
        "time"
        "github.com/prometheus/client_golang/prometheus"
)

// Single wait time metric for running jobs
var runningJobsWaitTimeBySize = prometheus.NewDesc(
    "slurm_running_jobs_wait_time_by_size_seconds",
    "Wait time statistics for currently running jobs by size",
    []string{"partition", "job_size", "resource_type", "stat"}, // stat: min, max, avg, median, p95
    nil,
)

// Helper functions for parsing resources (add these if you don't have them already)
func parseGPUCount(tres string) int {
    if tres == "" {
        return 0
    }

    parts := strings.Split(tres, ",")
    for _, part := range parts {
        if strings.HasPrefix(part, "gres/gpu") {
            if idx := strings.LastIndex(part, "="); idx != -1 {
                if count, err := strconv.Atoi(part[idx+1:]); err == nil {
                    return count
                }
            }
        }
    }
    return 0
}

func parseCPUCount(tres string) int {
    if tres == "" {
        return 0
    }

    parts := strings.Split(tres, ",")
    for _, part := range parts {
        if strings.HasPrefix(part, "cpu=") {
            if count, err := strconv.Atoi(part[4:]); err == nil {
                return count
            }
        }
    }
    return 0
}

// Job size categorization functions
func getGPUJobSize(gpuCount int) string {
    switch {
    case gpuCount == 0:
        return ""
    case gpuCount <= 2:
        return "small"
    case gpuCount <= 4:
        return "medium"
    case gpuCount <= 8:
        return "large"
    default:
        return "xlarge"
    }
}

func getCPUJobSize(cpuCount int) string {
    switch {
    case cpuCount == 0:
        return ""
    case cpuCount <= 16:
        return "small"
    case cpuCount <= 64:
        return "medium"
    case cpuCount <= 128:
        return "large"
    default:
        return "xlarge"
    }
}

// Parse squeue timestamp
func parseSqueueTimestamp(ts string) (time.Time, error) {
    // Try ISO format first (most common)
    if t, err := time.Parse("2006-01-02T15:04:05", ts); err == nil {
        return t, nil
    }

    // Try epoch timestamp
    if epoch, err := strconv.ParseInt(ts, 10, 64); err == nil {
        return time.Unix(epoch, 0), nil
    }

    // Try MM/DD-HH:MM:SS format (add current year)
    if strings.Contains(ts, "/") && strings.Contains(ts, "-") {
        currentYear := time.Now().Year()
        tsWithYear := fmt.Sprintf("%d/%s", currentYear, ts)
        if t, err := time.Parse("2006/01/02-15:04:05", tsWithYear); err == nil {
            return t, nil
        }
    }

    return time.Time{}, fmt.Errorf("cannot parse timestamp: %s", ts)
}

// Statistics tracker
type waitTimeStats struct {
    values []float64
}

func (w *waitTimeStats) add(value float64) {
    w.values = append(w.values, value)
}

func (w *waitTimeStats) calculate() (min, max, avg, median, p95 float64) {
    if len(w.values) == 0 {
        return 0, 0, 0, 0, 0
    }

    // Sort for percentile calculations
    sort.Float64s(w.values)

    min = w.values[0]
    max = w.values[len(w.values)-1]

    // Calculate average
    sum := 0.0
    for _, v := range w.values {
        sum += v
    }
    avg = sum / float64(len(w.values))

    // Calculate median (50th percentile)
    medianIdx := len(w.values) / 2
    if len(w.values)%2 == 0 && medianIdx > 0 {
        median = (w.values[medianIdx-1] + w.values[medianIdx]) / 2
    } else {
        median = w.values[medianIdx]
    }

    // Calculate 95th percentile
    p95Idx := int(float64(len(w.values)-1) * 0.95)
    p95 = w.values[p95Idx]

    return min, max, avg, median, p95
}

func (pc *PartitionsCollector) collectRunningJobsWaitTime(ch chan<- prometheus.Metric) {
    // Use --Format to get tres-alloc information
    cmd := exec.Command("squeue",
        "-h",                    // No header
        "-r",                    // Expand array jobs
        "-t", "RUNNING",         // Only running jobs
        "--Format", "jobid,partition,submittime,starttime,tres-alloc:100",
    )
    
    output, err := cmd.Output()
    if err != nil {
        log.Printf("ERROR: Failed to execute squeue: %v", err)
        return
    }
    
    // Map to track wait times
    statsMap := make(map[string]map[string]map[string]*waitTimeStats)
    
    lines := strings.Split(string(output), "\n")
    
    totalJobs := 0
    parsedJobs := 0
    
    for _, line := range lines {
        if line == "" {
            continue
        }
        
        totalJobs++
        
        // The output has fixed-width columns with spaces
        // We need to be more careful about parsing
        // Example line:
        // "23656800            cpu                 2025-10-02T13:58:38 2025-10-02T13:58:38 cpu=16,mem=250G,node=1,billing=16"
        
        // Use a regex or careful splitting to handle the fixed-width format
        // JobID is left-aligned, followed by spaces
        // Partition is left-aligned, followed by spaces
        // Times are in ISO format
        // TRES is everything after the second timestamp
        
        // Split into fields, handling multiple spaces
        fields := strings.Fields(line)
        if len(fields) < 5 {
            continue
        }
        
        //jobID := fields[0]
        partition := fields[1]
        submitTimeStr := fields[2]
        startTimeStr := fields[3]
        
        // Everything after the 4th field is TRES allocation
        // Join them in case TRES has spaces (though it typically doesn't)
        tresAlloc := strings.Join(fields[4:], " ")
        
        // Parse timestamps (ISO format based on your output)
        submitTime, err := time.Parse("2006-01-02T15:04:05", submitTimeStr)
        if err != nil {
            continue
        }
        
        startTime, err := time.Parse("2006-01-02T15:04:05", startTimeStr)
        if err != nil {
            continue
        }
        
        // Calculate wait time
        waitTime := startTime.Sub(submitTime).Seconds()
        if waitTime < 0 {
            continue
        }
        
        // Parse resources from TRES allocation string
        gpuCount := parseGPUFromTRESPart(tresAlloc)
        cpuCount := parseCPUFromTRES(tresAlloc)
        
        if cpuCount == 0 && gpuCount == 0 {
            continue
        }
        
        // Determine job size and type
        var jobSize, resourceType string
        
        if gpuCount > 0 {
            jobSize = getGPUJobSize(int(gpuCount))
            resourceType = "gpu"
        } else {
            jobSize = getCPUJobSize(cpuCount)
            resourceType = "cpu"
        }
        
        if jobSize == "" {
            continue
        }
        
        // Initialize nested maps
        if statsMap[partition] == nil {
            statsMap[partition] = make(map[string]map[string]*waitTimeStats)
        }
        if statsMap[partition][resourceType] == nil {
            statsMap[partition][resourceType] = make(map[string]*waitTimeStats)
        }
        if statsMap[partition][resourceType][jobSize] == nil {
            statsMap[partition][resourceType][jobSize] = &waitTimeStats{}
        }
        
        statsMap[partition][resourceType][jobSize].add(waitTime)
        parsedJobs++
    }
    
    // Calculate and emit statistics
    for partition, resourceMap := range statsMap {
        for resourceType, sizeMap := range resourceMap {
            for jobSize, stats := range sizeMap {
                min, max, avg, median, p95 := stats.calculate()
                
                ch <- prometheus.MustNewConstMetric(
                    pc.waitTimeBySize,
                    prometheus.GaugeValue,
                    min,
                    partition, jobSize, resourceType, "min",
                )
                ch <- prometheus.MustNewConstMetric(
                    pc.waitTimeBySize,
                    prometheus.GaugeValue,
                    max,
                    partition, jobSize, resourceType, "max",
                )
                ch <- prometheus.MustNewConstMetric(
                    pc.waitTimeBySize,
                    prometheus.GaugeValue,
                    avg,
                    partition, jobSize, resourceType, "avg",
                )
                ch <- prometheus.MustNewConstMetric(
                    pc.waitTimeBySize,
                    prometheus.GaugeValue,
                    median,
                    partition, jobSize, resourceType, "median",
                )
                ch <- prometheus.MustNewConstMetric(
                    pc.waitTimeBySize,
                    prometheus.GaugeValue,
                    p95,
                    partition, jobSize, resourceType, "p95",
                )
            }
        }
    }
}

func parseGPUFromTRESPart(tres string) int {
    if tres == "" {
        return 0
    }
    
    // TRES format: "cpu=16,mem=250G,node=1,billing=16,gres/gpu=2"
    parts := strings.Split(tres, ",")
    for _, part := range parts {
        part = strings.TrimSpace(part)
        // Look for gres/gpu=N
        if strings.HasPrefix(part, "gres/gpu=") {
            countStr := strings.TrimPrefix(part, "gres/gpu=")
            if count, err := strconv.Atoi(countStr); err == nil {
                return count
            }
        }
    }
    return 0
}

// Parse CPU count from TRES allocation string
func parseCPUFromTRES(tres string) int {
    if tres == "" {
        return 0
    }
    
    // TRES format: "cpu=16,mem=250G,node=1,billing=16"
    parts := strings.Split(tres, ",")
    for _, part := range parts {
        part = strings.TrimSpace(part)
        if strings.HasPrefix(part, "cpu=") {
            countStr := strings.TrimPrefix(part, "cpu=")
            if count, err := strconv.Atoi(countStr); err == nil {
                return count
            }
        }
    }
    return 0
}

func PartitionsData() []byte {
        cmd := exec.Command("sinfo", "-h", "-o%R,%C")
        stdout, err := cmd.StdoutPipe()
        if err != nil {
                log.Fatal(err)
        }
        if err := cmd.Start(); err != nil {
                log.Fatal(err)
        }
        out, _ := ioutil.ReadAll(stdout)
        if err := cmd.Wait(); err != nil {
                log.Fatal(err)
        }
        return out
}

func PartitionsPendingJobsData() []byte {
        cmd := exec.Command("squeue","-a","-r","-h","-o%P","--states=PENDING")
        stdout, err := cmd.StdoutPipe()
        if err != nil {
                log.Fatal(err)
        }
        if err := cmd.Start(); err != nil {
                log.Fatal(err)
        }
        out, _ := ioutil.ReadAll(stdout)
        if err := cmd.Wait(); err != nil {
                log.Fatal(err)
        }
        return out
}

type PartitionMetrics struct {
        allocated float64
        idle float64
        other float64
        pending float64
        total float64
}

func ParsePartitionsMetrics() map[string]*PartitionMetrics {
        partitions := make(map[string]*PartitionMetrics)
        lines := strings.Split(string(PartitionsData()), "\n")
        for _, line := range lines {
                if strings.Contains(line,",") {
                        // name of a partition
                        partition := strings.Split(line,",")[0]
                        _,key := partitions[partition]
                        if !key {
                                partitions[partition] = &PartitionMetrics{0,0,0,0,0}
                        }
                        states := strings.Split(line,",")[1]
                        allocated,_ := strconv.ParseFloat(strings.Split(states,"/")[0],64)
                        idle,_ := strconv.ParseFloat(strings.Split(states,"/")[1],64)
                        other,_ := strconv.ParseFloat(strings.Split(states,"/")[2],64)
                        total,_ := strconv.ParseFloat(strings.Split(states,"/")[3],64)
                        partitions[partition].allocated = allocated
                        partitions[partition].idle = idle
                        partitions[partition].other = other
                        partitions[partition].total = total
                }
        }
        // get list of pending jobs by partition name
        list := strings.Split(string(PartitionsPendingJobsData()),"\n")
        for _,partition := range list {
		// accumulate the number of pending jobs
		_,key := partitions[partition]
		if key {
			partitions[partition].pending += 1
                }
        }


        return partitions
}

type PartitionsCollector struct {
        allocated *prometheus.Desc
        idle *prometheus.Desc
        other *prometheus.Desc
        pending *prometheus.Desc
        total *prometheus.Desc
	waitTimeBySize *prometheus.Desc
}

func NewPartitionsCollector() *PartitionsCollector {
        labels := []string{"partition"}
        waitTimeLabels := []string{"partition", "job_size", "resource_type", "stat"}

        return &PartitionsCollector{
                allocated: prometheus.NewDesc("slurm_partition_cpus_allocated", "Allocated CPUs for partition", labels,nil),
		idle: prometheus.NewDesc("slurm_partition_cpus_idle", "Idle CPUs for partition", labels,nil),
		other: prometheus.NewDesc("slurm_partition_cpus_other", "Other CPUs for partition", labels,nil),
		pending: prometheus.NewDesc("slurm_partition_jobs_pending", "Pending jobs for partition", labels,nil),
		total: prometheus.NewDesc("slurm_partition_cpus_total", "Total CPUs for partition", labels,nil),
                waitTimeBySize: prometheus.NewDesc(
                  "slurm_running_jobs_wait_time_by_size_seconds",
                  "Wait time statistics for currently running jobs by size",
                  waitTimeLabels,  // <-- This should be the 4-label array, not 'labels'
                  nil,
                ),
        }
}

func (pc *PartitionsCollector) Describe(ch chan<- *prometheus.Desc) {
        ch <- pc.allocated
        ch <- pc.idle
        ch <- pc.other
        ch <- pc.pending
        ch <- pc.total
        ch <- pc.waitTimeBySize
}

func (pc *PartitionsCollector) Collect(ch chan<- prometheus.Metric) {
        pm := ParsePartitionsMetrics()
        for p := range pm {
                if pm[p].allocated > 0 {
                        ch <- prometheus.MustNewConstMetric(pc.allocated, prometheus.GaugeValue, pm[p].allocated, p)
                }
                if pm[p].idle > 0 {
                        ch <- prometheus.MustNewConstMetric(pc.idle, prometheus.GaugeValue, pm[p].idle, p)
                }
                if pm[p].other > 0 {
                        ch <- prometheus.MustNewConstMetric(pc.other, prometheus.GaugeValue, pm[p].other, p)
                }
                if pm[p].pending > 0 {
                        ch <- prometheus.MustNewConstMetric(pc.pending, prometheus.GaugeValue, pm[p].pending, p)
                }
                if pm[p].total > 0 {
                        ch <- prometheus.MustNewConstMetric(pc.total, prometheus.GaugeValue, pm[p].total, p)
                }
        }
	pc.collectRunningJobsWaitTime(ch)
}
