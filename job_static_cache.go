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
	"sync"
	"time"

	"github.com/prometheus/common/log"
)

// JobStaticInfo holds the per-job fields from `scontrol show job` that don't
// change for the lifetime of the job. Cached once per job, reused until the
// job leaves squeue.
type JobStaticInfo struct {
	JobID   int
	User    string
	WorkDir string
	NumCPUs int
	GPUs    []JobGPUAssignment
}

// JobStaticCache tracks per-job static info. On each Refresh() it diffs the
// set of currently running job IDs (cheap, from the squeue cache) against
// its own entries, evicts ended jobs, and batch-fetches scontrol details only
// for jobs it hasn't seen before.
type JobStaticCache struct {
	mu           sync.RWMutex
	entries      map[int]*JobStaticInfo
	diffInterval time.Duration
	lastDiff     time.Time
}

var jobStaticCache *JobStaticCache

func NewJobStaticCache(diffInterval time.Duration) *JobStaticCache {
	return &JobStaticCache{
		entries:      make(map[int]*JobStaticInfo),
		diffInterval: diffInterval,
	}
}

// Snapshot returns a copy of every cached job for safe iteration by collectors.
func (c *JobStaticCache) Snapshot() []JobStaticInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]JobStaticInfo, 0, len(c.entries))
	for _, info := range c.entries {
		out = append(out, *info)
	}
	return out
}

// Refresh evicts ended jobs and fetches scontrol details for any new ones.
// Skipped if less than diffInterval has elapsed since the last successful run
// (unless the cache is empty, e.g. exporter cold start).
func (c *JobStaticCache) Refresh() {
	c.mu.RLock()
	skipUntil := c.lastDiff.Add(c.diffInterval)
	empty := len(c.entries) == 0
	c.mu.RUnlock()
	if !empty && time.Now().Before(skipUntil) {
		return
	}

	running := runningJobIDsFromSqueueCache()
	if len(running) == 0 {
		return
	}

	c.mu.Lock()
	var newIDs []int
	for id := range running {
		if _, ok := c.entries[id]; !ok {
			newIDs = append(newIDs, id)
		}
	}
	for id := range c.entries {
		if !running[id] {
			delete(c.entries, id)
		}
	}
	c.mu.Unlock()

	if len(newIDs) == 0 {
		c.mu.Lock()
		c.lastDiff = time.Now()
		c.mu.Unlock()
		return
	}

	output := fetchScontrolForJobs(newIDs)
	if len(output) == 0 {
		return
	}

	// The full-scan branch of fetchScontrolForJobs returns every job
	// slurmctld knows about (including pending, completing, etc.). Filter to
	// the running set so we don't cache non-running jobs.
	wanted := make(map[int]bool, len(newIDs))
	for _, id := range newIDs {
		wanted[id] = true
	}

	infos := parseScontrolJobs(output)
	c.mu.Lock()
	for _, info := range infos {
		if !wanted[info.JobID] {
			continue
		}
		c.entries[info.JobID] = info
	}
	c.lastDiff = time.Now()
	c.mu.Unlock()
}

// fetchScontrolForJobs returns concatenated `scontrol -d show job` output for
// the given job ids. scontrol does not accept multiple ids in one call, so we
// either call once per id (cheap for a handful) or do a single full scan when
// the batch is large enough to amortize the overhead.
//
// Per-call cost on this cluster: ~18 ms each. Full scan: ~90-140 ms.
// Break-even is around 6 jobs.
func fetchScontrolForJobs(ids []int) string {
	const fullScanThreshold = 6
	start := time.Now()

	if len(ids) >= fullScanThreshold {
		out, err := executeCommand("scontrol", []string{"-d", "show", "job"})
		if err != nil {
			log.Warnf("JobStaticCache: scontrol full scan failed in %v: %v",
				time.Since(start), err)
			return ""
		}
		log.Debugf("JobStaticCache: full scan for %d new jobs in %v (%d bytes)",
			len(ids), time.Since(start), len(out))
		return string(out)
	}

	var combined strings.Builder
	for _, id := range ids {
		out, err := executeCommand("scontrol", []string{"-d", "show", "job", strconv.Itoa(id)})
		if err != nil {
			log.Warnf("JobStaticCache: scontrol show job %d failed: %v", id, err)
			continue
		}
		combined.Write(out)
		combined.WriteByte('\n')
	}
	log.Debugf("JobStaticCache: per-job fetch of %d jobs in %v (%d bytes)",
		len(ids), time.Since(start), combined.Len())
	return combined.String()
}

// runningJobIDsFromSqueueCache reads the cached `squeue_users_basic` output
// (format "%A|%u|%T|%C") and returns the set of jobs in state RUNNING.
func runningJobIDsFromSqueueCache() map[int]bool {
	output := string(GetCached("squeue_users_basic"))
	running := make(map[int]bool)
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Split(line, "|")
		if len(fields) < 3 || fields[2] != "RUNNING" {
			continue
		}
		// JobID may carry an array suffix like "12345_3" — strip it; we use
		// the underlying numeric ID since scontrol show job accepts that form.
		idStr := fields[0]
		if u := strings.IndexAny(idStr, "_+"); u >= 0 {
			idStr = idStr[:u]
		}
		if id, err := strconv.Atoi(idStr); err == nil {
			running[id] = true
		}
	}
	return running
}

// parseScontrolJobs walks `scontrol -d show job` output and returns one
// JobStaticInfo per JobId block.
func parseScontrolJobs(output string) []*JobStaticInfo {
	var infos []*JobStaticInfo
	var current *JobStaticInfo

	flush := func() {
		if current != nil {
			infos = append(infos, current)
		}
	}

	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "JobId=") {
			flush()
			current = &JobStaticInfo{}
			for _, f := range strings.Fields(trimmed) {
				if !strings.HasPrefix(f, "JobId=") {
					continue
				}
				idStr := strings.TrimPrefix(f, "JobId=")
				if id, err := strconv.Atoi(idStr); err == nil {
					current.JobID = id
				}
				break
			}
			continue
		}
		if current == nil {
			continue
		}

		switch {
		case strings.HasPrefix(trimmed, "UserId="):
			for _, f := range strings.Fields(trimmed) {
				if !strings.HasPrefix(f, "UserId=") {
					continue
				}
				u := strings.TrimPrefix(f, "UserId=")
				if p := strings.Index(u, "("); p > 0 {
					u = u[:p]
				}
				current.User = u
				break
			}
		case strings.HasPrefix(trimmed, "NumNodes="):
			for _, f := range strings.Fields(trimmed) {
				if strings.HasPrefix(f, "NumCPUs=") {
					if n, err := strconv.Atoi(strings.TrimPrefix(f, "NumCPUs=")); err == nil {
						current.NumCPUs = n
					}
				}
			}
		case strings.HasPrefix(trimmed, "WorkDir="):
			current.WorkDir = strings.TrimPrefix(trimmed, "WorkDir=")
		case strings.HasPrefix(trimmed, "Nodes="):
			nodeName, gresStr := extractNodesAndGres(trimmed)
			if gresStr == "" || gresStr == "(null)" {
				continue
			}
			gpuType, indices := parseGresIdx(gresStr)
			if gpuType == "" || len(indices) == 0 {
				continue
			}
			for _, host := range expandNodeList(nodeName) {
				for _, idx := range indices {
					current.GPUs = append(current.GPUs, JobGPUAssignment{
						User:     current.User,
						Hostname: host,
						GPU:      idx,
						GPUType:  gpuType,
					})
				}
			}
		}
	}
	flush()

	// Backfill User onto any GPUs that were parsed before the UserId line —
	// scontrol always emits UserId before Nodes= in practice, but be defensive.
	for _, info := range infos {
		for i := range info.GPUs {
			if info.GPUs[i].User == "" {
				info.GPUs[i].User = info.User
			}
		}
	}

	return infos
}
