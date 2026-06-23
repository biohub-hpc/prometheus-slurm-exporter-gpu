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
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// classifyWorkDir maps a job's WorkDir to (project, project_root) labels.
//
//	/hpc/projects/<name>/...  -> ("<name>", "projects")
//	/hpc/scratch/<name>/...   -> ("<name>", "scratch")
//	/hpc/mydata/... | /home/. -> ("personal", "personal")
//	anything else             -> ("unknown", "unknown")
func classifyWorkDir(workdir string) (project, root string) {
	switch {
	case strings.HasPrefix(workdir, "/hpc/projects/"):
		return projectFromPathSuffix(workdir, "/hpc/projects/"), "projects"
	case strings.HasPrefix(workdir, "/hpc/scratch/"):
		return projectFromPathSuffix(workdir, "/hpc/scratch/"), "scratch"
	case strings.HasPrefix(workdir, "/hpc/mydata/"),
		strings.HasPrefix(workdir, "/home/"):
		return "personal", "personal"
	default:
		return "unknown", "unknown"
	}
}

func projectFromPathSuffix(workdir, prefix string) string {
	suffix := strings.TrimPrefix(workdir, prefix)
	if i := strings.Index(suffix, "/"); i >= 0 {
		suffix = suffix[:i]
	}
	if suffix == "" {
		return "unknown"
	}
	return suffix
}

type projectKey struct {
	project, root, user string
}

type projectGPUKey struct {
	project, root, user, gpuType string
}

type JobProjectCollector struct {
	jobsRunning *prometheus.Desc
	cpusRunning *prometheus.Desc
	gpusRunning *prometheus.Desc
}

func NewJobProjectCollector() *JobProjectCollector {
	return &JobProjectCollector{
		jobsRunning: prometheus.NewDesc(
			"slurm_jobs_running_by_project",
			"Number of running jobs grouped by project (derived from WorkDir).",
			[]string{"project", "project_root", "user"}, nil),
		cpusRunning: prometheus.NewDesc(
			"slurm_cpus_running_by_project",
			"Total CPUs allocated to running jobs grouped by project.",
			[]string{"project", "project_root", "user"}, nil),
		gpusRunning: prometheus.NewDesc(
			"slurm_gpus_running_by_project",
			"Total GPUs allocated to running jobs grouped by project and GPU type.",
			[]string{"project", "project_root", "user", "gpu_type"}, nil),
	}
}

func (c *JobProjectCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobsRunning
	ch <- c.cpusRunning
	ch <- c.gpusRunning
}

func (c *JobProjectCollector) Collect(ch chan<- prometheus.Metric) {
	if jobStaticCache == nil {
		return
	}
	jobs := make(map[projectKey]int)
	cpus := make(map[projectKey]int)
	gpus := make(map[projectGPUKey]int)

	for _, info := range jobStaticCache.Snapshot() {
		project, root := classifyWorkDir(info.WorkDir)
		k := projectKey{project, root, info.User}
		jobs[k]++
		cpus[k] += info.NumCPUs
		for _, a := range info.GPUs {
			gpus[projectGPUKey{project, root, info.User, a.GPUType}]++
		}
	}

	for k, n := range jobs {
		ch <- prometheus.MustNewConstMetric(c.jobsRunning, prometheus.GaugeValue,
			float64(n), k.project, k.root, k.user)
	}
	for k, n := range cpus {
		ch <- prometheus.MustNewConstMetric(c.cpusRunning, prometheus.GaugeValue,
			float64(n), k.project, k.root, k.user)
	}
	for k, n := range gpus {
		ch <- prometheus.MustNewConstMetric(c.gpusRunning, prometheus.GaugeValue,
			float64(n), k.project, k.root, k.user, k.gpuType)
	}
}
