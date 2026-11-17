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
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// SacctJobID is the job id in the form sacct and the pipeline call logs use:
// "<ArrayJobId>_<ArrayTaskId>" for an array task, the plain JobId otherwise.
func (info JobStaticInfo) SacctJobID() string {
	if info.ArrayJobID > 0 && info.ArrayTaskID != "" {
		return strconv.Itoa(info.ArrayJobID) + "_" + info.ArrayTaskID
	}
	return strconv.Itoa(info.JobID)
}

// formatGPUAlloc renders a job's GPUs as "host:idx,idx;host:idx", hosts and
// indices sorted, e.g. "gpu-i-2:0,1" or "gpu-b-1:0,3;gpu-b-2:0,3". Consumers
// split on ";" then ":" then "," to get the (Hostname, gpu) pairs DCGM uses.
func formatGPUAlloc(gpus []JobGPUAssignment) string {
	byHost := map[string][]string{}
	seen := map[string]bool{}
	for _, g := range gpus {
		key := g.Hostname + ":" + g.GPU
		if seen[key] {
			continue
		}
		seen[key] = true
		byHost[g.Hostname] = append(byHost[g.Hostname], g.GPU)
	}
	hosts := make([]string, 0, len(byHost))
	for h := range byHost {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	parts := make([]string, 0, len(hosts))
	for _, h := range hosts {
		idx := byHost[h]
		sort.Slice(idx, func(i, j int) bool {
			a, errA := strconv.Atoi(idx[i])
			b, errB := strconv.Atoi(idx[j])
			if errA != nil || errB != nil {
				return idx[i] < idx[j]
			}
			return a < b
		})
		parts = append(parts, h+":"+strings.Join(idx, ","))
	}
	return strings.Join(parts, ";")
}

// JobGPUAllocCollector exports one series per running job that holds GPUs,
// labelled with the job's id and its GPU allocation. Unlike slurm_job_gpu_index
// (one series per GPU, no job id) this ties DCGM's (Hostname, gpu) to a job
// while adding only one series per GPU job; CPU-only jobs emit nothing.
type JobGPUAllocCollector struct {
	jobGPUAlloc *prometheus.Desc
}

func NewJobGPUAllocCollector() *JobGPUAllocCollector {
	return &JobGPUAllocCollector{
		jobGPUAlloc: prometheus.NewDesc(
			"slurm_job_gpu_alloc_info",
			"Indicator (=1) per running job holding GPUs. jobid is in sacct form (array_task for array tasks); "+
				"gpus is \"host:idx,idx;host:idx\" in DCGM's Hostname/gpu terms.",
			[]string{"jobid", "user", "gpus"}, nil),
	}
}

func (c *JobGPUAllocCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobGPUAlloc
}

func (c *JobGPUAllocCollector) Collect(ch chan<- prometheus.Metric) {
	if jobStaticCache == nil {
		return
	}
	seen := make(map[string]bool)
	for _, info := range jobStaticCache.Snapshot() {
		if len(info.GPUs) == 0 {
			continue
		}
		id := info.SacctJobID()
		if seen[id] {
			continue
		}
		seen[id] = true
		ch <- prometheus.MustNewConstMetric(
			c.jobGPUAlloc, prometheus.GaugeValue, 1,
			id, info.User, formatGPUAlloc(info.GPUs))
	}
}
