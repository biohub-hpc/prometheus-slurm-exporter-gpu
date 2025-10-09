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
        "io/ioutil"
        "os/exec"
        "log"
        "strings"
        "strconv"
        "time"
        "github.com/prometheus/client_golang/prometheus"
)

// Single wait time metric for running jobs
var runningJobsWaitTimeBySize = prometheus.NewDesc(
    "slurm_running_jobs_wait_time_by_size_seconds",
    "Wait time in seconds for each currently running job",
    []string{"jobid", "partition", "job_size", "resource_type", "user"},
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


func (pc *PartitionsCollector) collectRunningJobsWaitTime(ch chan<- prometheus.Metric) {
    // Use --Format to get tres-alloc information including user
    cmd := exec.Command("squeue",
        "-h",                    // No header
        "-r",                    // Expand array jobs
        "-t", "RUNNING",         // Only running jobs
        "--Format", "jobid,username,partition,submittime,starttime,tres-alloc:100",
    )

    output, err := cmd.Output()
    if err != nil {
        log.Printf("ERROR: Failed to execute squeue: %v", err)
        return
    }

    lines := strings.Split(string(output), "\n")

    totalLines := 0
    processedJobs := 0

    for _, line := range lines {
        if line == "" {
            continue
        }

        totalLines++

        // Split into fields, handling multiple spaces
        fields := strings.Fields(line)
        if len(fields) < 6 {
            log.Printf("DEBUG: Skipping line with insufficient fields (%d): %s", len(fields), line)
            continue
        }

        jobID := fields[0]
        user := fields[1]
        partition := fields[2]
        submitTimeStr := fields[3]
        startTimeStr := fields[4]

        // Everything after the 5th field is TRES allocation
        tresAlloc := strings.Join(fields[5:], " ")

        // Parse timestamps (ISO format)
        submitTime, err := time.Parse("2006-01-02T15:04:05", submitTimeStr)
        if err != nil {
            log.Printf("DEBUG: Failed to parse submit time '%s' for job %s: %v", submitTimeStr, jobID, err)
            continue
        }

        startTime, err := time.Parse("2006-01-02T15:04:05", startTimeStr)
        if err != nil {
            log.Printf("DEBUG: Failed to parse start time '%s' for job %s: %v", startTimeStr, jobID, err)
            continue
        }

        // Calculate wait time
        waitTime := startTime.Sub(submitTime).Seconds()
        if waitTime < 0 {
            log.Printf("DEBUG: Negative wait time for job %s: %v seconds", jobID, waitTime)
            continue
        }

        // Parse resources from TRES allocation string
        gpuCount := parseGPUFromTRESPart(tresAlloc)
        cpuCount := parseCPUFromTRES(tresAlloc)

        if cpuCount == 0 && gpuCount == 0 {
            log.Printf("DEBUG: Job %s has no CPU or GPU resources (tres: %s)", jobID, tresAlloc)
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
            log.Printf("DEBUG: Job %s resulted in empty job size", jobID)
            continue
        }

        // Emit a metric for this individual job
        ch <- prometheus.MustNewConstMetric(
            pc.waitTimeBySize,
            prometheus.GaugeValue,
            waitTime,
            jobID, partition, jobSize, resourceType, user,
        )

        processedJobs++
        if processedJobs <= 3 {
            log.Printf("DEBUG: Emitted metric for job %s: partition=%s, size=%s, type=%s, user=%s, wait_time=%v",
                jobID, partition, jobSize, resourceType, user, waitTime)
        }
    }

    log.Printf("INFO: collectRunningJobsWaitTime processed %d lines, emitted %d metrics", totalLines, processedJobs)
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
        waitTimeLabels := []string{"jobid", "partition", "job_size", "resource_type", "user"}

        return &PartitionsCollector{
                allocated: prometheus.NewDesc("slurm_partition_cpus_allocated", "Allocated CPUs for partition", labels,nil),
		idle: prometheus.NewDesc("slurm_partition_cpus_idle", "Idle CPUs for partition", labels,nil),
		other: prometheus.NewDesc("slurm_partition_cpus_other", "Other CPUs for partition", labels,nil),
		pending: prometheus.NewDesc("slurm_partition_jobs_pending", "Pending jobs for partition", labels,nil),
		total: prometheus.NewDesc("slurm_partition_cpus_total", "Total CPUs for partition", labels,nil),
                waitTimeBySize: prometheus.NewDesc(
                  "slurm_running_jobs_wait_time_by_size_seconds",
                  "Wait time in seconds for each currently running job",
                  waitTimeLabels,
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
