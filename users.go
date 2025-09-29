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
    "encoding/json"
    "io/ioutil"
    "os"
    "os/exec"
    "log"
    "strings"
    "strconv"
    "regexp"
    "sync"
    "time"
    "github.com/prometheus/client_golang/prometheus"
)

// Enhanced job tracking
type JobHistoryEntry struct {
    JobID        string    `json:"job_id"`
    User         string    `json:"user"`
    Partition    string    `json:"partition"`
    JobName      string    `json:"job_name"`
    FirstSeen    time.Time `json:"first_seen"`
    LastSeen     time.Time `json:"last_seen"`
    State        string    `json:"state"`
    CPUs         float64   `json:"cpus"`
    Memory       float64   `json:"memory"`  // in MB
    GPUs         float64   `json:"gpus"`
    Nodes        float64   `json:"nodes"`
    Priority     float64   `json:"priority"`
    QoS          string    `json:"qos"`
    SubmitTime   time.Time `json:"submit_time"`
    StartTime    time.Time `json:"start_time"`
    ArrayTaskID  string    `json:"array_task_id"`
    ExitCode     int       `json:"exit_code"`
    NodeList     string    `json:"node_list"`
}

var (
    userJobHistory     map[string]*JobHistoryEntry
    userJobHistoryMux  sync.RWMutex
    userHistoryFile    = "/home/mark.potts/slurm-exporter/job_history.json"

    // Counters that accumulate over time
    userJobCounts      map[string]map[string]float64
    countsMux          sync.RWMutex

    // Group mappings (could be loaded from config)
    userGroups         map[string]string  // user -> group mapping
)

func init() {
    userJobHistory = make(map[string]*JobHistoryEntry)
    userJobCounts = make(map[string]map[string]float64)
    userGroups = make(map[string]string)
    loadUserJobHistory()
    loadUserGroups()
}

func loadUserGroups() {
    // Load from config file or LDAP
    // For now, parse from user naming convention or config
    // Example: john.doe -> doe_lab
}

func loadUserJobHistory() {
    data, err := ioutil.ReadFile(userHistoryFile)
    if err != nil {
        log.Printf("No existing job history file: %v", err)
        return
    }

    var history map[string]*JobHistoryEntry
    if err := json.Unmarshal(data, &history); err != nil {
        log.Printf("Failed to parse job history: %v", err)
        return
    }
    userJobHistory = history
}

func saveUserJobHistory() {
    userJobHistoryMux.RLock()
    data, err := json.Marshal(userJobHistory)
    userJobHistoryMux.RUnlock()

    if err != nil {
        log.Printf("Failed to marshal job history: %v", err)
        return
    }

    os.MkdirAll("/home/mark.potts/slurm-exporter", 0755)
    if err := ioutil.WriteFile(userHistoryFile, data, 0644); err != nil {
        log.Printf("Failed to save job history: %v", err)
    }
}

// Parse TRES allocation string
func parseTRES(tresStr string) (cpus, memoryMB, nodes, gpus float64, reservation string) {
    if tresStr == "N/A" || tresStr == "" {
        return
    }
    
    parts := strings.Split(tresStr, ",")
    for _, part := range parts {
        part = strings.TrimSpace(part)
        
        // Handle regular key=value format
        kv := strings.Split(part, "=")
        if len(kv) != 2 {
            continue
        }
        key := strings.TrimSpace(kv[0])
        value := strings.TrimSpace(kv[1])
        
        switch key {
        case "cpu":
            cpus, _ = strconv.ParseFloat(value, 64)
        case "node":
            nodes, _ = strconv.ParseFloat(value, 64)
        case "mem":
            memoryMB = parseMemoryValue(value)
        case "gres/gpu":
            gpus, _ = strconv.ParseFloat(value, 64)
        case "reservation":
            reservation = value
        }
    }
    return
}

func parseMemoryValue(memStr string) float64 {
    memStr = strings.TrimSpace(memStr)
    if memStr == "" {
        return 0
    }

    re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT]?)B?$`)
    matches := re.FindStringSubmatch(strings.ToUpper(memStr))

    if len(matches) < 2 {
        return 0
    }

    value, _ := strconv.ParseFloat(matches[1], 64)

    if len(matches) >= 3 {
        switch matches[2] {
        case "K":
            return value / 1024
        case "M", "":
            return value
        case "G":
            return value * 1024
        case "T":
            return value * 1024 * 1024
        }
    }

    return value
}

// Parse time duration from SLURM format (D-HH:MM:SS or HH:MM:SS or MM:SS)
func parseTimeDuration(timeStr string) float64 {
    timeStr = strings.TrimSpace(timeStr)
    if timeStr == "" || timeStr == "N/A" || timeStr == "UNLIMITED" {
        return 0
    }

    var days, hours, minutes, seconds float64

    // Check for days
    if strings.Contains(timeStr, "-") {
        parts := strings.Split(timeStr, "-")
        days, _ = strconv.ParseFloat(parts[0], 64)
        timeStr = parts[1]
    }

    // Parse time portion
    timeParts := strings.Split(timeStr, ":")
    switch len(timeParts) {
    case 3:  // HH:MM:SS
        hours, _ = strconv.ParseFloat(timeParts[0], 64)
        minutes, _ = strconv.ParseFloat(timeParts[1], 64)
        seconds, _ = strconv.ParseFloat(timeParts[2], 64)
    case 2:  // MM:SS
        minutes, _ = strconv.ParseFloat(timeParts[0], 64)
        seconds, _ = strconv.ParseFloat(timeParts[1], 64)
    case 1:  // SS
        seconds, _ = strconv.ParseFloat(timeParts[0], 64)
    }

    return days*86400 + hours*3600 + minutes*60 + seconds
}

// Parse submit time from various formats
func parseSubmitTime(timeStr string) time.Time {
    // Try various formats SLURM might use
    formats := []string{
        "2006-01-02T15:04:05",
        "2006-01-02 15:04:05",
        "01/02/06 15:04:05",
        "01/02/2006 15:04:05",
    }

    for _, format := range formats {
        if t, err := time.Parse(format, timeStr); err == nil {
            return t
        }
    }

    return time.Time{}
}

// Get extended data from squeue with all fields
// Get extended data from squeue with all fields
func UsersDataComplete() ([]byte, map[string]map[string]string) {
    // First get basic job info with standard format that we know works
    cmd1 := exec.Command("squeue", "-a", "-r", "-h",
        "-o", "%i|%u|%P|%j|%T|%M|%L|%S|%p|%q|%N")
    
    output1, err := cmd1.Output()
    if err != nil {
        log.Printf("Error getting squeue data: %v", err)
        return []byte{}, make(map[string]map[string]string)
    }
    
    // Then get TRES data separately with --Format
    cmd2 := exec.Command("squeue", "-a", "-r", "-h",
        "--Format=JobID:20,tres-alloc:100")
    
    output2, err := cmd2.Output()
    if err != nil {
        log.Printf("Error getting TRES data: %v", err)
    }
    
    // Parse TRES data into a map
    tresMap := make(map[string]string)
    if err == nil {
        tresLines := strings.Split(string(output2), "\n")
        for _, line := range tresLines {
            line = strings.TrimSpace(line)
            if line == "" || strings.Contains(line, "JOBID") {
                continue
            }
            // The format is: "JobID    tres-alloc-string"
            // Split at first whitespace group
            fields := strings.Fields(line)
            if len(fields) >= 2 {
                jobID := fields[0]
                // Everything after the first field is TRES
                tres := strings.Join(fields[1:], " ")
                tresMap[jobID] = tres
            }
        }
    }
    
    // Parse main squeue output
    jobData := make(map[string]map[string]string)
    lines := strings.Split(string(output1), "\n")
    
    for _, line := range lines {
        line = strings.TrimSpace(line)
        if line == "" {
            continue
        }
        
        fields := strings.Split(line, "|")
        if len(fields) >= 11 {
            jobID := strings.TrimSpace(fields[0])
            
            // Get TRES from our map, or use empty string if not found
            tres := ""
            if t, ok := tresMap[jobID]; ok {
                tres = t
            }
            
            jobData[jobID] = map[string]string{
                "user":        strings.TrimSpace(fields[1]),
                "partition":   strings.TrimSpace(fields[2]),
                "name":        strings.TrimSpace(fields[3]),
                "state":       strings.TrimSpace(fields[4]),
                "time_used":   strings.TrimSpace(fields[5]),
                "time_left":   strings.TrimSpace(fields[6]),
                "submit_time": strings.TrimSpace(fields[7]),
                "priority":    strings.TrimSpace(fields[8]),
                "qos":         strings.TrimSpace(fields[9]),
                "tres":        tres,  // From TRES map
                "nodes":       strings.TrimSpace(fields[10]),
                "array_id":    "",
                "exit_code":   "0",
            }
        }
    }
    
    // Debug logging to verify TRES data
    debugCount := 0
    for jobID, job := range jobData {
        if job["state"] == "R" && job["tres"] != "" && debugCount < 3 {
            log.Printf("Job %s: state=%s, TRES='%s'", jobID, job["state"], job["tres"])
            debugCount++
        }
    }
    
    return output1, jobData
}

// Keep existing simple function for backward compatibility
func UsersData() []byte {
    cmd := exec.Command("squeue", "-a", "-r", "-h", "-o", "%A|%u|%T|%C")
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

type UserJobMetrics struct {
    // Existing metrics
    pending       float64
    running       float64
    running_cpus  float64
    suspended     float64

    // Resource metrics
    pending_cpus  float64
    memory_mb     float64
    gpus          float64
    nodes         float64

    // Wait time metrics
    total_wait_seconds float64
    max_wait_seconds   float64
    avg_wait_seconds   float64
    pending_job_count  float64  // For calculating average

    // Job duration metrics
    total_runtime_seconds    float64
    max_runtime_seconds      float64
    total_timeleft_seconds   float64
    walltime_usage_percent   float64
    jobs_near_timeout        float64  // <10% time remaining

    // Priority metrics
    avg_priority        float64
    priority_sum        float64
    high_priority_pending float64  // Priority > 1000 still pending

    // Resource efficiency
    cpu_memory_ratio    float64
    gpu_cpu_ratio       float64
    small_jobs          float64  // <4 CPUs
    large_jobs          float64  // >64 CPUs
    memory_intensive    float64  // >10GB/CPU

    // Array job metrics
    array_job_count     float64
    array_job_elements  float64

    // QoS metrics
    qos_jobs           map[string]float64
    qos_cpus           map[string]float64
    qos_wait_times     map[string]float64

    // Node spread metrics
    unique_nodes       map[string]bool
    exclusive_nodes    float64

    // Failure metrics
    failed_jobs        float64
    oom_failures       float64
    timeout_failures   float64
    node_failures      float64
    exit_codes         map[int]float64

    // Reservation metrics
    reserved_cpus      float64
    reserved_nodes     float64
    reservation_jobs   float64

    // Per-partition metrics
    partitions         map[string]float64
    part_cpus          map[string]float64
    part_memory        map[string]float64
    part_gpus          map[string]float64
    part_wait_time     map[string]float64

    // Add partition-specific tracking
    part_runtime      map[string]float64  // partition -> runtime seconds
    part_cpus_pending map[string]float64  // partition -> pending CPUs
    part_mem_pending  map[string]float64  // partition -> pending memory MB
    part_near_timeout map[string]float64  // partition -> jobs near timeout
    part_small_jobs   map[string]float64  // partition -> small jobs count
    part_large_jobs   map[string]float64  // partition -> large jobs count
    part_nodes        map[string]float64  // partition -> nodes running

    // Group metrics (if user belongs to a group)
    group              string
}

// Enhanced parser
func ParseUsersMetricsComplete(jobData map[string]map[string]string) map[string]*UserJobMetrics {
    users := make(map[string]*UserJobMetrics)
    groups := make(map[string]*UserJobMetrics)  // Track by group too

    currentTime := time.Now()
    seenJobs := make(map[string]bool)

    for jobID, job := range jobData {
        user := job["user"]
        if user == "" {
            continue
        }

        seenJobs[jobID] = true

        // Initialize user metrics if needed
        if _, exists := users[user]; !exists {
            users[user] = &UserJobMetrics{
                qos_jobs:       make(map[string]float64),
                qos_cpus:       make(map[string]float64),
                qos_wait_times: make(map[string]float64),
                unique_nodes:   make(map[string]bool),
                exit_codes:     make(map[int]float64),
                partitions:     make(map[string]float64),
                part_cpus:      make(map[string]float64),
                part_memory:    make(map[string]float64),
                part_gpus:      make(map[string]float64),
                part_wait_time: make(map[string]float64),
                part_runtime:      make(map[string]float64),
                part_cpus_pending: make(map[string]float64),
                part_mem_pending:  make(map[string]float64),
                part_near_timeout: make(map[string]float64),
                part_small_jobs:   make(map[string]float64),
                part_large_jobs:   make(map[string]float64),
                part_nodes:        make(map[string]float64),
            }
        }

        // Parse all job attributes
        partition := job["partition"]
        state := strings.ToLower(job["state"])
        qos := job["qos"]
        priority, _ := strconv.ParseFloat(job["priority"], 64)

        // Parse TRES
        cpus, memoryMB, nodes, gpus, reservation := parseTRES(job["tres"])

        // Parse times
        timeUsed := parseTimeDuration(job["time_used"])
        timeLeft := parseTimeDuration(job["time_left"])
        submitTime := parseSubmitTime(job["submit_time"])

        // Parse array info
        arrayID := job["array_id"]
	isArrayJob := strings.Contains(jobID, "_")

        // Then for array job tracking:
        if isArrayJob {
            users[user].array_job_count++

            // Parse array element from JobID like "123456_1" or "123456_[1-100]"
            if parts := strings.Split(jobID, "_"); len(parts) > 1 {
                elementStr := parts[1]

                // Check if it's a range like "[1-100]"
                if strings.HasPrefix(elementStr, "[") && strings.HasSuffix(elementStr, "]") {
                    // It's a range specification
                    rangeStr := strings.Trim(elementStr, "[]")
                    if strings.Contains(rangeStr, "-") {
                        rangeParts := strings.Split(rangeStr, "-")
                        if len(rangeParts) == 2 {
                            start, _ := strconv.ParseFloat(rangeParts[0], 64)
                            end, _ := strconv.ParseFloat(rangeParts[1], 64)
                            users[user].array_job_elements += (end - start + 1)
                        }
                    }
                } else {
                    // It's a single element, count it as 1
                    users[user].array_job_elements++
                }
            }
        }

        // Parse exit code
        exitCode, _ := strconv.Atoi(job["exit_code"])

        // Parse node list
        nodeList := job["nodes"]

        // Track job history
        userJobHistoryMux.Lock()
        if entry, exists := userJobHistory[jobID]; exists {
            entry.LastSeen = currentTime
            entry.State = state
            if exitCode != 0 {
                entry.ExitCode = exitCode
            }
        } else {
            // New job detected
            userJobHistory[jobID] = &JobHistoryEntry{
                JobID:       jobID,
                User:        user,
                Partition:   partition,
                JobName:     job["name"],
                FirstSeen:   currentTime,
                LastSeen:    currentTime,
                State:       state,
                CPUs:        cpus,
                Memory:      memoryMB,
                GPUs:        gpus,
                Nodes:       nodes,
                Priority:    priority,
                QoS:         qos,
                SubmitTime:  submitTime,
                ArrayTaskID: arrayID,
                ExitCode:    exitCode,
                NodeList:    nodeList,
            }

            // Increment submission counter
            countsMux.Lock()
            if userJobCounts[user] == nil {
                userJobCounts[user] = make(map[string]float64)
            }
            userJobCounts[user]["submitted"]++
            userJobCounts[user]["submitted_"+partition]++
            if isArrayJob {
                userJobCounts[user]["array_submitted"]++
            }
            countsMux.Unlock()
        }
        userJobHistoryMux.Unlock()

        // Update metrics based on state
        switch state {
        case "pd", "pending":
            users[user].pending++
            users[user].pending_cpus += cpus
            users[user].pending_job_count++

            // Calculate wait time
            if !submitTime.IsZero() {
                waitTime := currentTime.Sub(submitTime).Seconds()
                users[user].total_wait_seconds += waitTime
                if waitTime > users[user].max_wait_seconds {
                    users[user].max_wait_seconds = waitTime
                }
                users[user].part_wait_time[partition] += waitTime
                users[user].qos_wait_times[qos] += waitTime
            }

            // Track high priority pending
            if priority > 1000 {
                users[user].high_priority_pending++
            }

            users[user].partitions[partition+"_pending"]++
            users[user].priority_sum += priority

            // Add partition-specific pending resource tracking
            users[user].part_cpus_pending[partition] += cpus
            users[user].part_mem_pending[partition] += memoryMB



        case "r", "running":
            users[user].running++
            users[user].running_cpus += cpus
            users[user].memory_mb += memoryMB
            users[user].gpus += gpus
            users[user].nodes += nodes

            // Runtime metrics
            users[user].total_runtime_seconds += timeUsed
            if timeUsed > users[user].max_runtime_seconds {
                users[user].max_runtime_seconds = timeUsed
            }
            users[user].total_timeleft_seconds += timeLeft

            // Check if near timeout
            if timeLeft > 0 && timeUsed > 0 {
                percentUsed := (timeUsed / (timeUsed + timeLeft)) * 100
                users[user].walltime_usage_percent += percentUsed
                if percentUsed > 90 {
                    users[user].jobs_near_timeout++
                }
            }

            // Node tracking
            if nodeList != "" && nodeList != "N/A" {
                // Parse node list (handles formats like "node[1-3,5]" or "node1,node2")
                nodeNames := parseNodeList(nodeList)
                for _, node := range nodeNames {
                    users[user].unique_nodes[node] = true
                }
                if nodes > 0 {
                    users[user].exclusive_nodes += nodes
                }
            }

            // Resource efficiency metrics
            if cpus > 0 {
                if memoryMB > 0 {
                    ratio := cpus / (memoryMB / 1024.0)  // CPUs per GB
                    users[user].cpu_memory_ratio += ratio
                }
                if gpus > 0 {
                    users[user].gpu_cpu_ratio += gpus / cpus
                }
            }

            // Job size classification
            if cpus < 4 {
                users[user].small_jobs++
            } else if cpus > 64 {
                users[user].large_jobs++
            }

            // Memory intensive check
            if cpus > 0 && memoryMB/1024/cpus > 10 {
                users[user].memory_intensive++
            }

            // Partition metrics
            users[user].partitions[partition+"_running"]++
            users[user].part_cpus[partition] += cpus
            users[user].part_memory[partition] += memoryMB
            users[user].part_gpus[partition] += gpus

            // QoS metrics
            users[user].qos_jobs[qos]++
            users[user].qos_cpus[qos] += cpus

            // Reservation tracking
            if reservation != "" {
                users[user].reservation_jobs++
                users[user].reserved_cpus += cpus
                users[user].reserved_nodes += nodes
            }
            // Add partition runtime tracking
            users[user].part_runtime[partition] += timeUsed

            // Add partition nodes tracking
            users[user].part_nodes[partition] += nodes

            // Check if near timeout BY PARTITION
            if timeLeft > 0 && timeUsed > 0 {
                percentUsed := (timeUsed / (timeUsed + timeLeft)) * 100
                if percentUsed > 90 {
                    users[user].part_near_timeout[partition]++
                }
            }

            // Job size classification BY PARTITION
            if cpus < 4 {
                users[user].part_small_jobs[partition]++
            } else if cpus > 64 {
                users[user].part_large_jobs[partition]++
            }

        case "s", "suspended":
            users[user].suspended++

        case "f", "failed":
            users[user].failed_jobs++
            users[user].exit_codes[exitCode]++

            // Classify failure type
            if exitCode == 137 || exitCode == 9 {
                users[user].oom_failures++  // Likely OOM kill
            } else if exitCode == 140 {
                users[user].timeout_failures++  // SIGTERM from timeout
            } else if exitCode > 128 {
                users[user].node_failures++  // Other signal-based termination
            }
        }

        // Array job tracking
        if isArrayJob {
            users[user].array_job_count++
            // Count array elements (parse array ID like "123_[1-100]")
            if elements := parseArrayElements(arrayID); elements > 0 {
                users[user].array_job_elements += elements
            }
        }

        // Track priority average
        if priority > 0 {
            users[user].avg_priority = users[user].priority_sum / (users[user].pending + users[user].running)
        }

        // Group metrics
        if group := userGroups[user]; group != "" {
            users[user].group = group
            if _, exists := groups[group]; !exists {
                groups[group] = &UserJobMetrics{
                    partitions:  make(map[string]float64),
                    part_cpus:   make(map[string]float64),
                    part_memory: make(map[string]float64),
                    part_gpus:   make(map[string]float64),
                }
            }
            groups[group].running_cpus += cpus
            groups[group].gpus += gpus
            groups[group].memory_mb += memoryMB
        }
    }

    // Calculate averages
    for user := range users {
        if users[user].pending_job_count > 0 {
            users[user].avg_wait_seconds = users[user].total_wait_seconds / users[user].pending_job_count
        }
        if users[user].running > 0 {
            users[user].walltime_usage_percent = users[user].walltime_usage_percent / users[user].running
        }
    }

    // Check for completed jobs
    userJobHistoryMux.Lock()
    for jobID, entry := range userJobHistory {
        if !seenJobs[jobID] && (entry.State == "r" || entry.State == "running") {
            entry.State = "completed"
            entry.LastSeen = currentTime

            countsMux.Lock()
            if userJobCounts[entry.User] == nil {
                userJobCounts[entry.User] = make(map[string]float64)
            }
            userJobCounts[entry.User]["completed"]++
            userJobCounts[entry.User]["completed_"+entry.Partition]++
            countsMux.Unlock()
        }
    }

    // Clean up old entries
    for jobID, entry := range userJobHistory {
        if currentTime.Sub(entry.LastSeen) > 7*24*time.Hour {
            delete(userJobHistory, jobID)
        }
    }
    userJobHistoryMux.Unlock()

    go saveUserJobHistory()

    return users
}

// Parse node list from SLURM format
func parseNodeList(nodeStr string) []string {
    // Handle formats like "node[1-3,5]" or "node1,node2"
    var nodes []string

    // Simple comma-separated list
    if !strings.Contains(nodeStr, "[") {
        return strings.Split(nodeStr, ",")
    }

    // Complex format with brackets
    // This is simplified - full implementation would need more parsing
    nodes = append(nodes, nodeStr)

    return nodes
}

// Parse array element count
func parseArrayElements(arrayID string) float64 {
    // Parse formats like "123_[1-100]" or "123_1"
    if strings.Contains(arrayID, "[") && strings.Contains(arrayID, "]") {
        // Extract range
        start := strings.Index(arrayID, "[")
        end := strings.Index(arrayID, "]")
        if start < end {
            rangeStr := arrayID[start+1 : end]
            // Parse range like "1-100"
            if strings.Contains(rangeStr, "-") {
                parts := strings.Split(rangeStr, "-")
                if len(parts) == 2 {
                    first, _ := strconv.ParseFloat(parts[0], 64)
                    last, _ := strconv.ParseFloat(parts[1], 64)
                    return last - first + 1
                }
            }
        }
    }
    return 1  // Single array element
}

// Keep backward compatibility
func ParseUsersMetrics(input []byte) map[string]*UserJobMetrics {
    users := make(map[string]*UserJobMetrics)
    lines := strings.Split(string(input), "\n")
    for _, line := range lines {
        if strings.Contains(line, "|") {
            fields := strings.Split(line, "|")
            if len(fields) >= 4 {
                user := fields[1]
                if _, exists := users[user]; !exists {
                    users[user] = &UserJobMetrics{}
                }
                state := strings.ToLower(fields[2])
                cpus, _ := strconv.ParseFloat(fields[3], 64)

                switch {
                case strings.HasPrefix(state, "pending"):
                    users[user].pending++
                case strings.HasPrefix(state, "running"):
                    users[user].running++
                    users[user].running_cpus += cpus
                case strings.HasPrefix(state, "suspended"):
                    users[user].suspended++
                }
            }
        }
    }
    return users
}

type UsersCollector struct {
    // Existing metrics
    pending      *prometheus.Desc
    running      *prometheus.Desc
    running_cpus *prometheus.Desc
    suspended    *prometheus.Desc

    // Resource metrics
    pending_cpus *prometheus.Desc
    memory_mb    *prometheus.Desc
    gpus         *prometheus.Desc
    nodes        *prometheus.Desc

    // Wait time metrics
    wait_time_total    *prometheus.Desc
    wait_time_max      *prometheus.Desc
    wait_time_avg      *prometheus.Desc

    // Job duration metrics
    runtime_total      *prometheus.Desc
    runtime_max        *prometheus.Desc
    timeleft_total     *prometheus.Desc
    walltime_usage_pct *prometheus.Desc
    jobs_near_timeout  *prometheus.Desc

    // Priority metrics
    priority_avg       *prometheus.Desc
    high_priority_pending *prometheus.Desc

    // Resource efficiency
    cpu_memory_ratio   *prometheus.Desc
    gpu_cpu_ratio      *prometheus.Desc
    small_jobs         *prometheus.Desc
    large_jobs         *prometheus.Desc
    memory_intensive   *prometheus.Desc

    // Array job metrics
    array_jobs         *prometheus.Desc
    array_elements     *prometheus.Desc

    // QoS metrics
    qos_jobs           *prometheus.Desc
    qos_cpus           *prometheus.Desc
    qos_wait_time      *prometheus.Desc

    // Node metrics
    unique_nodes       *prometheus.Desc
    exclusive_nodes    *prometheus.Desc

    // Failure metrics
    failed_jobs        *prometheus.Desc
    oom_failures       *prometheus.Desc
    timeout_failures   *prometheus.Desc
    node_failures      *prometheus.Desc
    exit_codes         *prometheus.Desc

    // Reservation metrics
    reserved_cpus      *prometheus.Desc
    reserved_nodes     *prometheus.Desc
    reservation_jobs   *prometheus.Desc

    // Per-partition metrics
    partition_jobs     *prometheus.Desc
    partition_cpus     *prometheus.Desc
    partition_memory   *prometheus.Desc
    partition_gpus     *prometheus.Desc
    partition_wait     *prometheus.Desc
    partition_runtime      *prometheus.Desc
    partition_cpus_pending *prometheus.Desc
    partition_mem_pending  *prometheus.Desc
    partition_near_timeout *prometheus.Desc
    partition_small_jobs   *prometheus.Desc
    partition_large_jobs   *prometheus.Desc
    partition_nodes        *prometheus.Desc


    // Group metrics
    group_cpus         *prometheus.Desc
    group_gpus         *prometheus.Desc
    group_memory       *prometheus.Desc

    // Historical counters
    jobs_submitted     *prometheus.Desc
    jobs_completed     *prometheus.Desc
    array_submitted    *prometheus.Desc
}

func NewUsersCollector() *UsersCollector {
    labels := []string{"user"}
    partLabels := []string{"user", "partition"}
    qosLabels := []string{"user", "qos"}
    groupLabels := []string{"group"}
    exitLabels := []string{"user", "exit_code"}

    return &UsersCollector{
        // Existing
        pending: prometheus.NewDesc(
            "slurm_user_jobs_pending",
            "Pending jobs for user", labels, nil),
        running: prometheus.NewDesc(
            "slurm_user_jobs_running",
            "Running jobs for user", labels, nil),
        running_cpus: prometheus.NewDesc(
            "slurm_user_cpus_running",
            "Running CPUs for user", labels, nil),
        suspended: prometheus.NewDesc(
            "slurm_user_jobs_suspended",
            "Suspended jobs for user", labels, nil),

        // Resource metrics
        pending_cpus: prometheus.NewDesc(
            "slurm_user_cpus_pending",
            "Pending CPUs for user", labels, nil),
        memory_mb: prometheus.NewDesc(
            "slurm_user_memory_running_mb",
            "Running memory in MB for user", labels, nil),
        gpus: prometheus.NewDesc(
            "slurm_user_gpus_running",
            "Running GPUs for user", labels, nil),
        nodes: prometheus.NewDesc(
            "slurm_user_nodes_running",
            "Running nodes for user", labels, nil),

        // Wait time
        wait_time_total: prometheus.NewDesc(
            "slurm_user_wait_time_seconds_total",
            "Total wait time for pending jobs", labels, nil),
        wait_time_max: prometheus.NewDesc(
            "slurm_user_wait_time_seconds_max",
            "Maximum wait time for pending jobs", labels, nil),
        wait_time_avg: prometheus.NewDesc(
            "slurm_user_wait_time_seconds_avg",
            "Average wait time for pending jobs", labels, nil),

        // Job duration
        runtime_total: prometheus.NewDesc(
            "slurm_user_runtime_seconds_total",
            "Total runtime for running jobs", labels, nil),
        runtime_max: prometheus.NewDesc(
            "slurm_user_runtime_seconds_max",
            "Maximum runtime for running jobs", labels, nil),
        timeleft_total: prometheus.NewDesc(
            "slurm_user_timeleft_seconds_total",
            "Total time left for running jobs", labels, nil),
        walltime_usage_pct: prometheus.NewDesc(
            "slurm_user_walltime_usage_percent",
            "Average walltime usage percentage", labels, nil),
        jobs_near_timeout: prometheus.NewDesc(
            "slurm_user_jobs_near_timeout",
            "Jobs with <10% time remaining", labels, nil),

        // Priority
        priority_avg: prometheus.NewDesc(
            "slurm_user_priority_avg",
            "Average job priority", labels, nil),
        high_priority_pending: prometheus.NewDesc(
            "slurm_user_high_priority_pending",
            "High priority jobs still pending", labels, nil),

        // Efficiency
        cpu_memory_ratio: prometheus.NewDesc(
            "slurm_user_cpu_memory_ratio",
            "CPUs per GB of memory", labels, nil),
        gpu_cpu_ratio: prometheus.NewDesc(
            "slurm_user_gpu_cpu_ratio",
            "GPUs per CPU", labels, nil),
        small_jobs: prometheus.NewDesc(
            "slurm_user_small_jobs",
            "Jobs using <4 CPUs", labels, nil),
        large_jobs: prometheus.NewDesc(
            "slurm_user_large_jobs",
            "Jobs using >64 CPUs", labels, nil),
        memory_intensive: prometheus.NewDesc(
            "slurm_user_memory_intensive_jobs",
            "Jobs using >10GB per CPU", labels, nil),

        // Array jobs
        array_jobs: prometheus.NewDesc(
            "slurm_user_array_jobs",
            "Number of array jobs", labels, nil),
        array_elements: prometheus.NewDesc(
            "slurm_user_array_elements",
            "Total array job elements", labels, nil),

        // QoS
        qos_jobs: prometheus.NewDesc(
            "slurm_user_qos_jobs",
            "Jobs per QoS", qosLabels, nil),
        qos_cpus: prometheus.NewDesc(
            "slurm_user_qos_cpus",
            "CPUs per QoS", qosLabels, nil),
        qos_wait_time: prometheus.NewDesc(
            "slurm_user_qos_wait_seconds",
            "Wait time per QoS", qosLabels, nil),

        // Nodes
        unique_nodes: prometheus.NewDesc(
            "slurm_user_unique_nodes",
            "Number of unique nodes used", labels, nil),
        exclusive_nodes: prometheus.NewDesc(
            "slurm_user_exclusive_nodes",
            "Exclusively allocated nodes", labels, nil),

        // Failures
        failed_jobs: prometheus.NewDesc(
            "slurm_user_failed_jobs",
            "Failed jobs", labels, nil),
        oom_failures: prometheus.NewDesc(
            "slurm_user_oom_failures",
            "Out of memory failures", labels, nil),
        timeout_failures: prometheus.NewDesc(
            "slurm_user_timeout_failures",
            "Timeout failures", labels, nil),
        node_failures: prometheus.NewDesc(
            "slurm_user_node_failures",
            "Node-related failures", labels, nil),
        exit_codes: prometheus.NewDesc(
            "slurm_user_exit_codes",
            "Job exit code distribution", exitLabels, nil),

        // Reservations
        reserved_cpus: prometheus.NewDesc(
            "slurm_user_reserved_cpus",
            "CPUs in reservations", labels, nil),
        reserved_nodes: prometheus.NewDesc(
            "slurm_user_reserved_nodes",
            "Nodes in reservations", labels, nil),
        reservation_jobs: prometheus.NewDesc(
            "slurm_user_reservation_jobs",
            "Jobs using reservations", labels, nil),

        // Partitions
        partition_jobs: prometheus.NewDesc(
            "slurm_user_partition_jobs",
            "Jobs per partition",
            []string{"user", "partition", "state"}, nil),
        partition_cpus: prometheus.NewDesc(
            "slurm_user_partition_cpus_running",
            "Running CPUs per partition", partLabels, nil),
        partition_memory: prometheus.NewDesc(
            "slurm_user_partition_memory_running_mb",
            "Running memory per partition", partLabels, nil),
        partition_gpus: prometheus.NewDesc(
            "slurm_user_partition_gpus_running",
            "Running GPUs per partition", partLabels, nil),
        partition_wait: prometheus.NewDesc(
            "slurm_user_partition_wait_seconds",
            "Wait time per partition", partLabels, nil),
        partition_runtime: prometheus.NewDesc(
            "slurm_user_partition_runtime_seconds",
            "Total runtime seconds per partition", partLabels, nil),
        partition_cpus_pending: prometheus.NewDesc(
            "slurm_user_partition_cpus_pending",
            "Pending CPUs per partition", partLabels, nil),
        partition_mem_pending: prometheus.NewDesc(
            "slurm_user_partition_memory_pending_mb",
            "Pending memory MB per partition", partLabels, nil),
        partition_near_timeout: prometheus.NewDesc(
            "slurm_user_partition_jobs_near_timeout",
            "Jobs near timeout per partition", partLabels, nil),
        partition_small_jobs: prometheus.NewDesc(
            "slurm_user_partition_small_jobs",
            "Small jobs (<4 CPUs) per partition", partLabels, nil),
        partition_large_jobs: prometheus.NewDesc(
            "slurm_user_partition_large_jobs",
            "Large jobs (>64 CPUs) per partition", partLabels, nil),
        partition_nodes: prometheus.NewDesc(
            "slurm_user_partition_nodes_running",
            "Running nodes per partition", partLabels, nil),

        // Groups
        group_cpus: prometheus.NewDesc(
            "slurm_group_cpus_running",
            "Running CPUs per group", groupLabels, nil),
        group_gpus: prometheus.NewDesc(
            "slurm_group_gpus_running",
            "Running GPUs per group", groupLabels, nil),
        group_memory: prometheus.NewDesc(
            "slurm_group_memory_running_mb",
            "Running memory per group", groupLabels, nil),

        // Historical
        jobs_submitted: prometheus.NewDesc(
            "slurm_user_jobs_submitted_total",
            "Total jobs submitted", partLabels, nil),
        jobs_completed: prometheus.NewDesc(
            "slurm_user_jobs_completed_total",
            "Total jobs completed", partLabels, nil),
        array_submitted: prometheus.NewDesc(
            "slurm_user_array_jobs_submitted_total",
            "Total array jobs submitted", labels, nil),
    }
}

func (uc *UsersCollector) Describe(ch chan<- *prometheus.Desc) {
    // Existing
    ch <- uc.pending
    ch <- uc.running
    ch <- uc.running_cpus
    ch <- uc.suspended

    // All new metrics
    ch <- uc.pending_cpus
    ch <- uc.memory_mb
    ch <- uc.gpus
    ch <- uc.nodes
    ch <- uc.wait_time_total
    ch <- uc.wait_time_max
    ch <- uc.wait_time_avg
    ch <- uc.runtime_total
    ch <- uc.runtime_max
    ch <- uc.timeleft_total
    ch <- uc.walltime_usage_pct
    ch <- uc.jobs_near_timeout
    ch <- uc.priority_avg
    ch <- uc.high_priority_pending
    ch <- uc.cpu_memory_ratio
    ch <- uc.gpu_cpu_ratio
    ch <- uc.small_jobs
    ch <- uc.large_jobs
    ch <- uc.memory_intensive
    ch <- uc.array_jobs
    ch <- uc.array_elements
    ch <- uc.qos_jobs
    ch <- uc.qos_cpus
    ch <- uc.qos_wait_time
    ch <- uc.unique_nodes
    ch <- uc.exclusive_nodes
    ch <- uc.failed_jobs
    ch <- uc.oom_failures
    ch <- uc.timeout_failures
    ch <- uc.node_failures
    ch <- uc.exit_codes
    ch <- uc.reserved_cpus
    ch <- uc.reserved_nodes
    ch <- uc.reservation_jobs
    ch <- uc.partition_jobs
    ch <- uc.partition_cpus
    ch <- uc.partition_memory
    ch <- uc.partition_gpus
    ch <- uc.partition_wait
    ch <- uc.partition_runtime
    ch <- uc.partition_cpus_pending
    ch <- uc.partition_mem_pending
    ch <- uc.partition_near_timeout
    ch <- uc.partition_small_jobs
    ch <- uc.partition_large_jobs
    ch <- uc.partition_nodes
    ch <- uc.group_cpus
    ch <- uc.group_gpus
    ch <- uc.group_memory
    ch <- uc.jobs_submitted
    ch <- uc.jobs_completed
    ch <- uc.array_submitted
}

func (uc *UsersCollector) Collect(ch chan<- prometheus.Metric) {
    // Get complete job data
    _, jobData := UsersDataComplete()

    // If no data, fall back to simple collection
    if len(jobData) == 0 {
        um := ParseUsersMetrics(UsersData())
        // Emit basic metrics only
        for u := range um {
            if um[u].pending > 0 {
                ch <- prometheus.MustNewConstMetric(uc.pending,
                    prometheus.GaugeValue, um[u].pending, u)
            }
            if um[u].running > 0 {
                ch <- prometheus.MustNewConstMetric(uc.running,
                    prometheus.GaugeValue, um[u].running, u)
            }
            if um[u].running_cpus > 0 {
                ch <- prometheus.MustNewConstMetric(uc.running_cpus,
                    prometheus.GaugeValue, um[u].running_cpus, u)
            }
        }
        return
    }

    // Parse complete metrics
    um := ParseUsersMetricsComplete(jobData)
    groupMetrics := make(map[string]*UserJobMetrics)

    // Emit all metrics
    for u, m := range um {
        // Basic metrics
        if m.pending > 0 {
            ch <- prometheus.MustNewConstMetric(uc.pending,
                prometheus.GaugeValue, m.pending, u)
        }
        if m.running > 0 {
            ch <- prometheus.MustNewConstMetric(uc.running,
                prometheus.GaugeValue, m.running, u)
        }
        if m.running_cpus > 0 {
            ch <- prometheus.MustNewConstMetric(uc.running_cpus,
                prometheus.GaugeValue, m.running_cpus, u)
        }
        if m.suspended > 0 {
            ch <- prometheus.MustNewConstMetric(uc.suspended,
                prometheus.GaugeValue, m.suspended, u)
        }

        // Resource metrics
        if m.pending_cpus > 0 {
            ch <- prometheus.MustNewConstMetric(uc.pending_cpus,
                prometheus.GaugeValue, m.pending_cpus, u)
        }
        if m.memory_mb > 0 {
            ch <- prometheus.MustNewConstMetric(uc.memory_mb,
                prometheus.GaugeValue, m.memory_mb, u)
        }
        if m.gpus > 0 {
            ch <- prometheus.MustNewConstMetric(uc.gpus,
                prometheus.GaugeValue, m.gpus, u)
        }
        if m.nodes > 0 {
            ch <- prometheus.MustNewConstMetric(uc.nodes,
                prometheus.GaugeValue, m.nodes, u)
        }

        // Wait time metrics
        if m.total_wait_seconds > 0 {
            ch <- prometheus.MustNewConstMetric(uc.wait_time_total,
                prometheus.GaugeValue, m.total_wait_seconds, u)
        }
        if m.max_wait_seconds > 0 {
            ch <- prometheus.MustNewConstMetric(uc.wait_time_max,
                prometheus.GaugeValue, m.max_wait_seconds, u)
        }
        if m.avg_wait_seconds > 0 {
            ch <- prometheus.MustNewConstMetric(uc.wait_time_avg,
                prometheus.GaugeValue, m.avg_wait_seconds, u)
        }

        // Runtime metrics
        if m.total_runtime_seconds > 0 {
            ch <- prometheus.MustNewConstMetric(uc.runtime_total,
                prometheus.GaugeValue, m.total_runtime_seconds, u)
        }
        if m.max_runtime_seconds > 0 {
            ch <- prometheus.MustNewConstMetric(uc.runtime_max,
                prometheus.GaugeValue, m.max_runtime_seconds, u)
        }
        if m.total_timeleft_seconds > 0 {
            ch <- prometheus.MustNewConstMetric(uc.timeleft_total,
                prometheus.GaugeValue, m.total_timeleft_seconds, u)
        }
        if m.walltime_usage_percent > 0 {
            ch <- prometheus.MustNewConstMetric(uc.walltime_usage_pct,
                prometheus.GaugeValue, m.walltime_usage_percent, u)
        }
        if m.jobs_near_timeout > 0 {
            ch <- prometheus.MustNewConstMetric(uc.jobs_near_timeout,
                prometheus.GaugeValue, m.jobs_near_timeout, u)
        }

        // Priority metrics
        if m.avg_priority > 0 {
            ch <- prometheus.MustNewConstMetric(uc.priority_avg,
                prometheus.GaugeValue, m.avg_priority, u)
        }
        if m.high_priority_pending > 0 {
            ch <- prometheus.MustNewConstMetric(uc.high_priority_pending,
                prometheus.GaugeValue, m.high_priority_pending, u)
        }

        // Efficiency metrics
        if m.cpu_memory_ratio > 0 {
            ch <- prometheus.MustNewConstMetric(uc.cpu_memory_ratio,
                prometheus.GaugeValue, m.cpu_memory_ratio, u)
        }
        if m.gpu_cpu_ratio > 0 {
            ch <- prometheus.MustNewConstMetric(uc.gpu_cpu_ratio,
                prometheus.GaugeValue, m.gpu_cpu_ratio, u)
        }
        if m.small_jobs > 0 {
            ch <- prometheus.MustNewConstMetric(uc.small_jobs,
                prometheus.GaugeValue, m.small_jobs, u)
        }
        if m.large_jobs > 0 {
            ch <- prometheus.MustNewConstMetric(uc.large_jobs,
                prometheus.GaugeValue, m.large_jobs, u)
        }
        if m.memory_intensive > 0 {
            ch <- prometheus.MustNewConstMetric(uc.memory_intensive,
                prometheus.GaugeValue, m.memory_intensive, u)
        }

        // Array job metrics
        if m.array_job_count > 0 {
            ch <- prometheus.MustNewConstMetric(uc.array_jobs,
                prometheus.GaugeValue, m.array_job_count, u)
        }
        if m.array_job_elements > 0 {
            ch <- prometheus.MustNewConstMetric(uc.array_elements,
                prometheus.GaugeValue, m.array_job_elements, u)
        }

        // QoS metrics
        for qos, jobs := range m.qos_jobs {
            if jobs > 0 {
                ch <- prometheus.MustNewConstMetric(uc.qos_jobs,
                    prometheus.GaugeValue, jobs, u, qos)
            }
        }
        for qos, cpus := range m.qos_cpus {
            if cpus > 0 {
                ch <- prometheus.MustNewConstMetric(uc.qos_cpus,
                    prometheus.GaugeValue, cpus, u, qos)
            }
        }
        for qos, wait := range m.qos_wait_times {
            if wait > 0 {
                ch <- prometheus.MustNewConstMetric(uc.qos_wait_time,
                    prometheus.GaugeValue, wait, u, qos)
            }
        }

        // Node metrics
        if len(m.unique_nodes) > 0 {
            ch <- prometheus.MustNewConstMetric(uc.unique_nodes,
                prometheus.GaugeValue, float64(len(m.unique_nodes)), u)
        }
        if m.exclusive_nodes > 0 {
            ch <- prometheus.MustNewConstMetric(uc.exclusive_nodes,
                prometheus.GaugeValue, m.exclusive_nodes, u)
        }

        // Failure metrics
        if m.failed_jobs > 0 {
            ch <- prometheus.MustNewConstMetric(uc.failed_jobs,
                prometheus.GaugeValue, m.failed_jobs, u)
        }
        if m.oom_failures > 0 {
            ch <- prometheus.MustNewConstMetric(uc.oom_failures,
                prometheus.GaugeValue, m.oom_failures, u)
        }
        if m.timeout_failures > 0 {
            ch <- prometheus.MustNewConstMetric(uc.timeout_failures,
                prometheus.GaugeValue, m.timeout_failures, u)
        }
        if m.node_failures > 0 {
            ch <- prometheus.MustNewConstMetric(uc.node_failures,
                prometheus.GaugeValue, m.node_failures, u)
        }
        for code, count := range m.exit_codes {
            if count > 0 {
                ch <- prometheus.MustNewConstMetric(uc.exit_codes,
                    prometheus.GaugeValue, count, u, strconv.Itoa(code))
            }
        }

        // Reservation metrics
        if m.reserved_cpus > 0 {
            ch <- prometheus.MustNewConstMetric(uc.reserved_cpus,
                prometheus.GaugeValue, m.reserved_cpus, u)
        }
        if m.reserved_nodes > 0 {
            ch <- prometheus.MustNewConstMetric(uc.reserved_nodes,
                prometheus.GaugeValue, m.reserved_nodes, u)
        }
        if m.reservation_jobs > 0 {
            ch <- prometheus.MustNewConstMetric(uc.reservation_jobs,
                prometheus.GaugeValue, m.reservation_jobs, u)
        }

        // Partition metrics
        for partState, count := range m.partitions {
            parts := strings.Split(partState, "_")
            if len(parts) == 2 {
                ch <- prometheus.MustNewConstMetric(uc.partition_jobs,
                    prometheus.GaugeValue, count, u, parts[0], parts[1])
            }
        }
        for part, cpus := range m.part_cpus {
            if cpus > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_cpus,
                    prometheus.GaugeValue, cpus, u, part)
            }
        }
        for part, mem := range m.part_memory {
            if mem > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_memory,
                    prometheus.GaugeValue, mem, u, part)
            }
        }
        for part, gpus := range m.part_gpus {
            if gpus > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_gpus,
                    prometheus.GaugeValue, gpus, u, part)
            }
        }
        for part, wait := range m.part_wait_time {
            if wait > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_wait,
                    prometheus.GaugeValue, wait, u, part)
            }
        }
	// Add after existing partition metrics
        for part, runtime := range m.part_runtime {
            if runtime > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_runtime,
                    prometheus.GaugeValue, runtime, u, part)
            }
        }
        for part, cpus := range m.part_cpus_pending {
            if cpus > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_cpus_pending,
                    prometheus.GaugeValue, cpus, u, part)
            }
        }
        for part, mem := range m.part_mem_pending {
            if mem > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_mem_pending,
                    prometheus.GaugeValue, mem, u, part)
            }
        }
        for part, count := range m.part_near_timeout {
            if count > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_near_timeout,
                    prometheus.GaugeValue, count, u, part)
            }
        }
        for part, count := range m.part_small_jobs {
            if count > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_small_jobs,
                    prometheus.GaugeValue, count, u, part)
            }
        }
        for part, count := range m.part_large_jobs {
            if count > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_large_jobs,
                    prometheus.GaugeValue, count, u, part)
            }
        }
        for part, nodes := range m.part_nodes {
            if nodes > 0 {
                ch <- prometheus.MustNewConstMetric(uc.partition_nodes,
                    prometheus.GaugeValue, nodes, u, part)
            }
        }

        // Aggregate group metrics
        if m.group != "" {
            if _, exists := groupMetrics[m.group]; !exists {
                groupMetrics[m.group] = &UserJobMetrics{}
            }
            groupMetrics[m.group].running_cpus += m.running_cpus
            groupMetrics[m.group].gpus += m.gpus
            groupMetrics[m.group].memory_mb += m.memory_mb
        }
    }

    // Emit group metrics
    for group, metrics := range groupMetrics {
        if metrics.running_cpus > 0 {
            ch <- prometheus.MustNewConstMetric(uc.group_cpus,
                prometheus.GaugeValue, metrics.running_cpus, group)
        }
        if metrics.gpus > 0 {
            ch <- prometheus.MustNewConstMetric(uc.group_gpus,
                prometheus.GaugeValue, metrics.gpus, group)
        }
        if metrics.memory_mb > 0 {
            ch <- prometheus.MustNewConstMetric(uc.group_memory,
                prometheus.GaugeValue, metrics.memory_mb, group)
        }
    }

    // Emit historical counters
    countsMux.RLock()
    for user, counts := range userJobCounts {
        for metric, value := range counts {
            if strings.HasPrefix(metric, "submitted_") {
                partition := strings.TrimPrefix(metric, "submitted_")
                ch <- prometheus.MustNewConstMetric(uc.jobs_submitted,
                    prometheus.CounterValue, value, user, partition)
            } else if strings.HasPrefix(metric, "completed_") {
                partition := strings.TrimPrefix(metric, "completed_")
                ch <- prometheus.MustNewConstMetric(uc.jobs_completed,
                    prometheus.CounterValue, value, user, partition)
            } else if metric == "submitted" {
                ch <- prometheus.MustNewConstMetric(uc.jobs_submitted,
                    prometheus.CounterValue, value, user, "all")
            } else if metric == "completed" {
                ch <- prometheus.MustNewConstMetric(uc.jobs_completed,
                    prometheus.CounterValue, value, user, "all")
            } else if metric == "array_submitted" {
                ch <- prometheus.MustNewConstMetric(uc.array_submitted,
                    prometheus.CounterValue, value, user)
            }
        }
    }
    countsMux.RUnlock()
}
