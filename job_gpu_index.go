/* Copyright 2026 Chan Zuckerberg Biohub

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
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// JobGPUAssignment is one (Hostname, gpu_index) -> (user, gpu_type) mapping.
// Field names Hostname / GPU match DCGM's labels exactly so the PromQL join is
// `on(Hostname, gpu)`.
type JobGPUAssignment struct {
	User     string
	Hostname string
	GPU      string
	GPUType  string
}

// extractNodesAndGres pulls "Nodes=..." and "GRES=..." out of a per-node
// allocation line in `scontrol -d show job` output.
func extractNodesAndGres(line string) (string, string) {
	var nodes, gres string
	for _, f := range strings.Fields(line) {
		if strings.HasPrefix(f, "Nodes=") {
			nodes = strings.TrimPrefix(f, "Nodes=")
		}
		if strings.HasPrefix(f, "GRES=") {
			gres = strings.TrimPrefix(f, "GRES=")
		}
	}
	return nodes, gres
}

// parseGresIdx pulls the GPU type and physical indices out of a GRES spec like
// "gpu:a40:1(IDX:0)", "gpu:h100:2(IDX:0,3)", or "gpu:a100:4(IDX:0-2,5)".
func parseGresIdx(gres string) (string, []string) {
	for _, piece := range splitGresEntries(gres) {
		if !strings.HasPrefix(piece, "gpu:") {
			continue
		}
		idxStart := strings.Index(piece, "(IDX:")
		if idxStart < 0 {
			continue
		}
		rest := piece[idxStart+len("(IDX:"):]
		idxEnd := strings.Index(rest, ")")
		if idxEnd < 0 {
			continue
		}

		gpuType := "generic"
		if parts := strings.Split(piece[:idxStart], ":"); len(parts) >= 3 {
			gpuType = parts[1]
		}

		return gpuType, expandIdxRange(rest[:idxEnd])
	}
	return "", nil
}

// splitGresEntries splits a GRES spec on commas at paren-depth 0, so that
// "gpu:a100:2(IDX:0,3),mem=4G" yields ["gpu:a100:2(IDX:0,3)", "mem=4G"].
func splitGresEntries(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// expandIdxRange turns "0", "0,3", or "0-2,5" into ["0"], ["0","3"], ["0","1","2","5"].
func expandIdxRange(s string) []string {
	var result []string
	for _, part := range strings.Split(s, ",") {
		if strings.Contains(part, "-") {
			r := strings.SplitN(part, "-", 2)
			start, err1 := strconv.Atoi(r[0])
			end, err2 := strconv.Atoi(r[1])
			if err1 != nil || err2 != nil {
				continue
			}
			for i := start; i <= end; i++ {
				result = append(result, strconv.Itoa(i))
			}
		} else if _, err := strconv.Atoi(part); err == nil {
			result = append(result, part)
		}
	}
	return result
}

type JobGPUIndexCollector struct {
	jobGPUIndex *prometheus.Desc
}

func NewJobGPUIndexCollector() *JobGPUIndexCollector {
	return &JobGPUIndexCollector{
		jobGPUIndex: prometheus.NewDesc(
			"slurm_job_gpu_index",
			"Indicator (=1) per allocated GPU. Join with DCGM via on(Hostname, gpu).",
			[]string{"user", "Hostname", "gpu", "gpu_type"}, nil),
	}
}

func (c *JobGPUIndexCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobGPUIndex
}

func (c *JobGPUIndexCollector) Collect(ch chan<- prometheus.Metric) {
	if jobStaticCache == nil {
		return
	}
	seen := make(map[string]bool)
	for _, info := range jobStaticCache.Snapshot() {
		for _, a := range info.GPUs {
			key := a.User + "|" + a.Hostname + "|" + a.GPU + "|" + a.GPUType
			if seen[key] {
				continue
			}
			seen[key] = true
			ch <- prometheus.MustNewConstMetric(
				c.jobGPUIndex, prometheus.GaugeValue, 1,
				a.User, a.Hostname, a.GPU, a.GPUType)
		}
	}
}
