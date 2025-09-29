/* Copyright 2020 Joeri Hermans, Victor Penso, Matteo Dessalvi

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

// Also need to import fmt at the top of the file
import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"os/exec"
	"strconv"
	"strings"
)

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	utilization float64
	// Metrics by GPU type
	allocByType map[string]float64
	totalByType map[string]float64
	idleByType  map[string]float64
	// New: Metrics by user and GPU type
	allocByUserAndType map[string]map[string]float64 // map[user]map[gpu_type]count
}

// Struct to hold node to GPU type mapping
type NodeGPUInfo struct {
	gpuType  string
	gpuCount float64
}

func GPUsGetMetrics() *GPUsMetrics {
	return ParseGPUsMetrics()
}

// Enhanced GetNodeGPUMapping with debug output
func GetNodeGPUMapping() map[string]NodeGPUInfo {
	nodeGPUs := make(map[string]NodeGPUInfo)
	
	// Use sinfo to get node GPU information
	args := []string{"-h", "-o", "%N %G"}
	output := string(Execute("sinfo", args))
	
	log.Infof("Raw sinfo output:\n%s", output)
	
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					nodeList := fields[0]
					gres := fields[1]
					
					log.Debugf("Processing line: nodeList=%s, gres=%s", nodeList, gres)
					
					// Skip nodes without GPUs
					if gres == "(null)" || !strings.Contains(gres, "gpu:") {
						log.Debugf("  Skipping non-GPU node group: %s", nodeList)
						continue
					}
					
					// Expand node list (handles ranges like gpu-f-[1-2,5])
					expandedNodes := expandNodeList(nodeList)
					log.Debugf("  Expanded %s to %d nodes: %v", nodeList, len(expandedNodes), expandedNodes)
					
					// Parse GPU information from GRES
					var gpuType string
					var gpuCount float64
					
					// Remove the (S:x) part first
					gresClean := strings.Split(gres, "(")[0]
					
					parts := strings.Split(gresClean, ":")
					if len(parts) == 3 {
						// Format: gpu:type:count
						gpuType = parts[1]
						gpuCount, _ = strconv.ParseFloat(parts[2], 64)
					} else if len(parts) == 2 {
						// Format: gpu:count
						gpuType = "generic"
						gpuCount, _ = strconv.ParseFloat(parts[1], 64)
					}
					
					log.Debugf("  Parsed GPU info: type=%s, count=%v", gpuType, gpuCount)
					
					// Assign GPU info to all expanded nodes
					for _, node := range expandedNodes {
						nodeGPUs[node] = NodeGPUInfo{
							gpuType:  gpuType,
							gpuCount: gpuCount,
						}
					}
				}
			}
		}
	}
	
	// Print summary of mapping
	log.Infof("=== Node to GPU Mapping Summary ===")
	log.Infof("Total nodes with GPUs: %d", len(nodeGPUs))
	
	// Group by GPU type for summary
	typeCount := make(map[string]int)
	for node, info := range nodeGPUs {
		typeCount[info.gpuType]++
		// Print first few nodes of each type as examples
		if typeCount[info.gpuType] <= 3 {
			log.Infof("  %s -> %s (count: %v)", node, info.gpuType, info.gpuCount)
		}
	}
	
	// Print type summary
	log.Infof("=== GPU Type Distribution ===")
	for gpuType, count := range typeCount {
		log.Infof("  %s: %d nodes", gpuType, count)
	}
	
	return nodeGPUs
}

// Helper function to expand SLURM node range format
func expandNodeList(nodeList string) []string {
	var expandedNodes []string
	
	// Handle multiple node patterns separated by commas outside brackets
	// First, we need to handle the case where commas are inside brackets differently
	// from commas outside brackets
	
	// Check if this contains brackets
	if !strings.Contains(nodeList, "[") {
		// No brackets, just split by comma
		return strings.Split(nodeList, ",")
	}
	
	// Parse more carefully when brackets are involved
	currentPos := 0
	nodePatterns := []string{}
	
	for currentPos < len(nodeList) {
		// Find the next bracket or comma
		bracketStart := strings.Index(nodeList[currentPos:], "[")
		commaPos := strings.Index(nodeList[currentPos:], ",")
		
		if bracketStart == -1 && commaPos == -1 {
			// No more special characters, take the rest
			if currentPos < len(nodeList) {
				nodePatterns = append(nodePatterns, nodeList[currentPos:])
			}
			break
		}
		
		if bracketStart != -1 && (commaPos == -1 || bracketStart < commaPos) {
			// Found a bracket before any comma
			bracketStart += currentPos
			bracketEnd := strings.Index(nodeList[bracketStart:], "]")
			if bracketEnd == -1 {
				break // Malformed
			}
			bracketEnd += bracketStart
			
			// Get the complete pattern including brackets
			pattern := nodeList[currentPos : bracketEnd+1]
			nodePatterns = append(nodePatterns, pattern)
			
			// Move position past this pattern
			currentPos = bracketEnd + 1
			// Skip comma if present
			if currentPos < len(nodeList) && nodeList[currentPos] == ',' {
				currentPos++
			}
		} else if commaPos != -1 {
			// Found comma before any bracket
			commaPos += currentPos
			pattern := nodeList[currentPos:commaPos]
			if pattern != "" {
				nodePatterns = append(nodePatterns, pattern)
			}
			currentPos = commaPos + 1
		}
	}
	
	// Now expand each pattern
	for _, pattern := range nodePatterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		
		// Check if this is a range pattern with brackets
		if strings.Contains(pattern, "[") && strings.Contains(pattern, "]") {
			// Extract prefix and range
			bracketStart := strings.Index(pattern, "[")
			bracketEnd := strings.Index(pattern, "]")
			
			if bracketStart != -1 && bracketEnd != -1 && bracketEnd > bracketStart {
				prefix := pattern[:bracketStart]
				rangeStr := pattern[bracketStart+1 : bracketEnd]
				suffix := pattern[bracketEnd+1:]
				
				// Expand the range
				nodes := expandRange(prefix, rangeStr, suffix)
				expandedNodes = append(expandedNodes, nodes...)
			} else {
				// Malformed, add as-is
				expandedNodes = append(expandedNodes, pattern)
			}
		} else {
			// Single node, no range
			expandedNodes = append(expandedNodes, pattern)
		}
	}
	
	return expandedNodes
}

// Helper function to expand a range like "1-2,5" or "01-20"
func expandRange(prefix, rangeStr, suffix string) []string {
	var nodes []string
	
	// Split by comma for multiple ranges/values
	ranges := strings.Split(rangeStr, ",")
	
	for _, r := range ranges {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		
		if strings.Contains(r, "-") {
			// It's a range like "1-2" or "01-20"
			parts := strings.Split(r, "-")
			if len(parts) == 2 {
				// Determine if we need zero-padding
				padWidth := 0
				if strings.HasPrefix(parts[0], "0") && len(parts[0]) > 1 {
					padWidth = len(parts[0])
				}
				
				start, err1 := strconv.Atoi(parts[0])
				end, err2 := strconv.Atoi(parts[1])
				
				if err1 == nil && err2 == nil {
					for i := start; i <= end; i++ {
						var nodeNum string
						if padWidth > 0 {
							// Zero-pad the number
							nodeNum = fmt.Sprintf("%0*d", padWidth, i)
						} else {
							nodeNum = strconv.Itoa(i)
						}
						nodes = append(nodes, prefix+nodeNum+suffix)
					}
				}
			}
		} else {
			// Single number, not a range
			nodes = append(nodes, prefix+r+suffix)
		}
	}
	
	return nodes
}

func ParseAllocatedGPUsByTypeAndUser() (float64, map[string]float64, map[string]map[string]float64) {
	var totalGPUs = 0.0
	gpusByType := make(map[string]float64)
	gpusByUserAndType := make(map[string]map[string]float64)
	
	// First, get the node to GPU mapping
	nodeGPUMap := GetNodeGPUMapping()
	
	// Get running jobs
	args := []string{"-h", "-o", "%i|%u|%P|%N|%t", "-t", "RUNNING"}
	output := string(Execute("squeue", args))
	// Log first few lines of squeue output
	lines := strings.Split(output, "\n")
	log.Infof("=== Raw squeue output (first 10 lines) ===")
	for i, line := range lines {
		if i < 10 && line != "" {
			log.Infof("  %s", line)
		}
	}
	log.Infof("Total squeue lines: %d", len(lines))
	
	log.Infof("=== Processing Running Jobs ===")
	jobCount := 0
	gpuJobCount := 0
	
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				fields := strings.Split(line, "|")
				if len(fields) >= 5 {
					jobid := fields[0]
					user := fields[1]
					partition := fields[2]
					nodeList := fields[3]
					state := fields[4]
					
					// Skip if not running
					if state != "R" {
						continue
					}
					
					jobCount++
					
					// Expand the node list to individual nodes
					nodes := expandNodeList(nodeList)
					
					// Log ALL GPU jobs for debugging (not just first 10)
					debugLog := false
					foundGPU := false
					
					// For each node this job is using, count the GPUs
					for _, node := range nodes {
						if nodeInfo, exists := nodeGPUMap[node]; exists {
							foundGPU = true
							// Check if this is a GPU partition
							if strings.Contains(partition, "gpu") || strings.Contains(partition, "interact") || strings.Contains(partition, "preempt") {
								gpusUsed := 1.0
								gpuJobCount++
								
								// Log GPU jobs to see what's being counted
								if !debugLog {
									log.Debugf("GPU Job %s: user=%s, partition=%s, node=%s, gpu_type=%s", 
										jobid, user, partition, node, nodeInfo.gpuType)
									debugLog = true // Only log once per job
								}
								
								totalGPUs += gpusUsed
								gpusByType[nodeInfo.gpuType] += gpusUsed
								
								// Initialize user map if needed
								if _, exists := gpusByUserAndType[user]; !exists {
									gpusByUserAndType[user] = make(map[string]float64)
								}
								gpusByUserAndType[user][nodeInfo.gpuType] += gpusUsed
							}
						}
					}
					
					// Log if we found a job on GPU nodes but didn't count it
					if foundGPU && !strings.Contains(partition, "gpu") && !strings.Contains(partition, "interact") && !strings.Contains(partition, "preempt") {
						log.Warnf("Job %s on GPU node but partition '%s' not recognized as GPU partition", jobid, partition)
					}
				}
			}
		}
	}
	
	log.Infof("=== Allocation Summary ===")
	log.Infof("Total jobs processed: %d", jobCount)
	log.Infof("GPU jobs counted: %d", gpuJobCount)
	log.Infof("Total GPUs allocated: %v", totalGPUs)
	log.Infof("GPUs by type:")
	for gpuType, count := range gpusByType {
		log.Infof("  %s: %v", gpuType, count)
	}
	
	// Show sample of users by GPU type
	log.Infof("Sample users by GPU type:")
	for gpuType, _ := range gpusByType {
		log.Infof("  Users on %s:", gpuType)
		count := 0
		for user, types := range gpusByUserAndType {
			if types[gpuType] > 0 {
				log.Infof("    %s: %v GPUs", user, types[gpuType])
				count++
				if count >= 3 {
					break // Just show first 3 users per type
				}
			}
		}
	}
	
	return totalGPUs, gpusByType, gpusByUserAndType
}

func ParseAllocatedGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-a", "-X", "--format=AllocTRES", "--state=RUNNING", "--noheader", "--parsable2"}
	output := string(Execute("sacct", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				for _, resource := range strings.Split(line, ",") {
					if strings.HasPrefix(resource, "gres/gpu=") {
						descriptor := strings.TrimPrefix(resource, "gres/gpu=")
						job_gpus, _ := strconv.ParseFloat(descriptor, 64)
						num_gpus += job_gpus
					}
				}
			}
		}
	}

	return num_gpus
}

// Parse total GPUs by type
func ParseTotalGPUsByType() (float64, map[string]float64) {
	var totalGPUs = 0.0
	gpusByType := make(map[string]float64)
	
	args := []string{"-h", "-o", "%n %G"}
	output := string(Execute("sinfo", args))
	
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					gres := fields[1]
					
					for _, resource := range strings.Split(gres, ",") {
						if strings.HasPrefix(resource, "gpu:") {
							parts := strings.Split(resource, ":")
							
							if len(parts) >= 3 {
								// Format: gpu:type:count
								gpuType := parts[1]
								countStr := strings.Split(parts[2], "(")[0]
								count, _ := strconv.ParseFloat(countStr, 64)
								totalGPUs += count
								gpusByType[gpuType] += count
							} else if len(parts) == 2 {
								// Format: gpu:count (generic)
								countStr := strings.Split(parts[1], "(")[0]
								count, _ := strconv.ParseFloat(countStr, 64)
								totalGPUs += count
								gpusByType["generic"] += count
							}
						}
					}
				}
			}
		}
	}
	
	return totalGPUs, gpusByType
}

func ParseTotalGPUs() float64 {
	total, _ := ParseTotalGPUsByType()
	return total
}

func ParseGPUsMetrics() *GPUsMetrics {
	var gm GPUsMetrics
	
	// Get totals and by-type metrics
	total_gpus, totalByType := ParseTotalGPUsByType()
	allocated_gpus, allocByType, allocByUserAndType := ParseAllocatedGPUsByTypeAndUser()
	
	gm.alloc = allocated_gpus
	gm.idle = total_gpus - allocated_gpus
	gm.total = total_gpus
	if total_gpus > 0 {
		gm.utilization = allocated_gpus / total_gpus
	} else {
		gm.utilization = 0
	}
	
	// Calculate idle by type
	gm.allocByType = allocByType
	gm.totalByType = totalByType
	gm.idleByType = make(map[string]float64)
	gm.allocByUserAndType = allocByUserAndType
	
	for gpuType, total := range totalByType {
		allocated := allocByType[gpuType] // Returns 0 if not present
		gm.idleByType[gpuType] = total - allocated
	}
	
	return &gm
}

// Execute the sinfo command and return its output
func Execute(command string, arguments []string) []byte {
	cmd := exec.Command(command, arguments...)
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

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	return &GPUsCollector{
		alloc:       prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		idle:        prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		total:       prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
		utilization: prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", nil, nil),
		// Metrics with GPU type labels
		allocByType: prometheus.NewDesc(
			"slurm_gpus_alloc_by_type",
			"Allocated GPUs by type",
			[]string{"gpu_type"}, nil),
		idleByType: prometheus.NewDesc(
			"slurm_gpus_idle_by_type",
			"Idle GPUs by type",
			[]string{"gpu_type"}, nil),
		totalByType: prometheus.NewDesc(
			"slurm_gpus_total_by_type",
			"Total GPUs by type",
			[]string{"gpu_type"}, nil),
		utilizationByType: prometheus.NewDesc(
			"slurm_gpus_utilization_by_type",
			"GPU utilization by type",
			[]string{"gpu_type"}, nil),
		// New: Metrics with both user and GPU type labels
		allocByUser: prometheus.NewDesc(
			"slurm_gpus_alloc_by_user",
			"Allocated GPUs by user and type",
			[]string{"user", "gpu_type"}, nil),
		utilizationByUser: prometheus.NewDesc(
			"slurm_gpus_utilization_by_user",
			"GPU utilization percentage by user and type (user's allocation / total of that type)",
			[]string{"user", "gpu_type"}, nil),
	}
}

type GPUsCollector struct {
	alloc             *prometheus.Desc
	idle              *prometheus.Desc
	total             *prometheus.Desc
	utilization       *prometheus.Desc
	allocByType       *prometheus.Desc
	idleByType        *prometheus.Desc
	totalByType       *prometheus.Desc
	utilizationByType *prometheus.Desc
	allocByUser       *prometheus.Desc
	utilizationByUser *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
	ch <- cc.allocByType
	ch <- cc.idleByType
	ch <- cc.totalByType
	ch <- cc.utilizationByType
	ch <- cc.allocByUser
	ch <- cc.utilizationByUser
}

func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm := GPUsGetMetrics()

	// Original metrics
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm.utilization)

	// Metrics by GPU type
	for gpuType, count := range cm.allocByType {
		ch <- prometheus.MustNewConstMetric(cc.allocByType, prometheus.GaugeValue, count, gpuType)
	}

	for gpuType, count := range cm.idleByType {
		ch <- prometheus.MustNewConstMetric(cc.idleByType, prometheus.GaugeValue, count, gpuType)
	}

	for gpuType, count := range cm.totalByType {
		ch <- prometheus.MustNewConstMetric(cc.totalByType, prometheus.GaugeValue, count, gpuType)

		// Calculate and export utilization by type
		if count > 0 {
			allocated := cm.allocByType[gpuType]
			utilization := allocated / count
			ch <- prometheus.MustNewConstMetric(cc.utilizationByType, prometheus.GaugeValue, utilization, gpuType)
		}
	}

	// Metrics by user and GPU type
	for user, gpuTypes := range cm.allocByUserAndType {
		for gpuType, count := range gpuTypes {
			ch <- prometheus.MustNewConstMetric(cc.allocByUser, prometheus.GaugeValue, count, user, gpuType)

			// New: Calculate and export utilization by user and type
			// This represents what percentage of that GPU type this user is consuming
			if totalOfType, exists := cm.totalByType[gpuType]; exists && totalOfType > 0 {
				userUtilization := count / totalOfType
				ch <- prometheus.MustNewConstMetric(cc.utilizationByUser, prometheus.GaugeValue, userUtilization, user, gpuType)
			}
		}
	}
}

// Test the expandNodeList function
func TestExpandNodeList() {
	testCases := []string{
		"gpu-f-[1-2,5]",
		"gpu-h-[1-2,4,7-8]",
		"gpu-sm01-[01-20]",
		"gpu-e-[1-8],gpu-sm01-[01-20],gpu-sm02-[01-20]",
		"gpu-a-2,gpu-d-[1-2]",
	}

	log.Infof("=== Testing Node Expansion ===")
	for _, test := range testCases {
		expanded := expandNodeList(test)
		log.Infof("Input: %s", test)
		log.Infof("Output (%d nodes): %v", len(expanded), expanded)
	}
}
