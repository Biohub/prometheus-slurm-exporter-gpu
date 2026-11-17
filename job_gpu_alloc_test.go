package main

import "testing"

// Shape of `scontrol -d show job` output on Bruno (Slurm), anonymised.
const scontrolGPUJobs = `JobId=38150822 ArrayJobId=38143012 ArrayTaskId=10 ArrayTaskThrottle=1 JobName=NAME
   UserId=alice(1001) GroupId=grp(2001) MCS_label=N/A
   NumNodes=1 NumCPUs=8 NumTasks=1 CPUs/Task=8 ReqB:S:C:T=0:0:*:*
   JOB_GRES=gpu:a100:1
     Nodes=gpu-d-1 CPU_IDs=8-15 Mem=65536 GRES=gpu:a100:1(IDX:1)
   WorkDir=/tmp

JobId=38150900 JobName=NAME
   UserId=bob(1002) GroupId=grp(2001) MCS_label=N/A
   NumNodes=2 NumCPUs=64 NumTasks=2 CPUs/Task=32 ReqB:S:C:T=0:0:*:*
   JOB_GRES=gpu:h100:4
     Nodes=gpu-b-[1-2] CPU_IDs=0-31 Mem=131072 GRES=gpu:h100:2(IDX:3,0)
   WorkDir=/tmp

JobId=38150950 JobName=NAME
   UserId=carol(1003) GroupId=grp(2001) MCS_label=N/A
   NumNodes=1 NumCPUs=8 NumTasks=1 CPUs/Task=8 ReqB:S:C:T=0:0:*:*
     Nodes=cpu-b-2 CPU_IDs=0-7 Mem=16384 GRES=
   WorkDir=/tmp
`

func TestParseScontrolArrayFields(t *testing.T) {
	infos := parseScontrolJobs(scontrolGPUJobs)
	if len(infos) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(infos))
	}
	if infos[0].ArrayJobID != 38143012 || infos[0].ArrayTaskID != "10" {
		t.Errorf("array task fields not parsed: %+v", *infos[0])
	}
	if got := infos[0].SacctJobID(); got != "38143012_10" {
		t.Errorf("array task SacctJobID = %q, want 38143012_10", got)
	}
	if got := infos[1].SacctJobID(); got != "38150900" {
		t.Errorf("plain job SacctJobID = %q, want 38150900", got)
	}
}

func TestFormatGPUAlloc(t *testing.T) {
	infos := parseScontrolJobs(scontrolGPUJobs)
	cases := []struct {
		job  int
		want string
	}{
		{0, "gpu-d-1:1"},
		{1, "gpu-b-1:0,3;gpu-b-2:0,3"}, // hosts and indices sorted numerically
		{2, ""},                        // CPU-only job: no GPUs
	}
	for _, c := range cases {
		if got := formatGPUAlloc(infos[c.job].GPUs); got != c.want {
			t.Errorf("job %d: formatGPUAlloc = %q, want %q", infos[c.job].JobID, got, c.want)
		}
	}
	if got := formatGPUAlloc([]JobGPUAssignment{{Hostname: "h", GPU: "10"}, {Hostname: "h", GPU: "2"}, {Hostname: "h", GPU: "2"}}); got != "h:2,10" {
		t.Errorf("numeric sort / dedupe: got %q, want h:2,10", got)
	}
}
