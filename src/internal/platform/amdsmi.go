package platform

import (
	"context"
	"encoding/json"
	"os/exec"

	"opentela/internal/common"
)

// amd-smi is rocm-smi's replacement; newer ROCm images ship only amd-smi.
// Two calls: `static --asic` for the product name, `metric --mem-usage` for
// total/used VRAM. Both print {"gpu_data": [...]} (older amd-smi versions
// print the bare list), keyed by "gpu" index.

type amdSMIValue struct {
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type amdSMIGPU struct {
	GPU  int `json:"gpu"`
	ASIC struct {
		MarketName string `json:"market_name"`
	} `json:"asic"`
	MemUsage struct {
		TotalVRAM amdSMIValue `json:"total_vram"`
		UsedVRAM  amdSMIValue `json:"used_vram"`
	} `json:"mem_usage"`
}

func getAMDSMIGPUs() []common.GPUSpec {
	static, err := runAMDSMI("static", "--asic", "--json")
	if err != nil {
		common.Logger.Debug("amd-smi unavailable: ", err)
		return []common.GPUSpec{}
	}
	// Memory is best effort: the names alone are enough to advertise the GPUs.
	usage, err := runAMDSMI("metric", "--mem-usage", "--json")
	if err != nil {
		common.Logger.Debug("amd-smi metric --mem-usage failed: ", err)
	}
	return parseAMDSMI(static, usage)
}

func runAMDSMI(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), smiTimeout)
	defer cancel()
	return exec.CommandContext(ctx, "amd-smi", args...).Output()
}

func parseAMDSMI(static, usage []byte) []common.GPUSpec {
	gpus := []common.GPUSpec{}
	names, err := decodeAMDSMI(static)
	if err != nil {
		common.Logger.Debug("could not parse amd-smi static output: ", err)
		return gpus
	}
	mem := map[int]amdSMIGPU{}
	if usages, err := decodeAMDSMI(usage); err == nil {
		for _, u := range usages {
			mem[u.GPU] = u
		}
	}
	for _, g := range names {
		if g.ASIC.MarketName == "" || g.ASIC.MarketName == "N/A" {
			continue
		}
		spec := common.GPUSpec{Name: g.ASIC.MarketName}
		if u, ok := mem[g.GPU]; ok {
			spec.TotalMemory = toMiB(u.MemUsage.TotalVRAM)
			spec.UsedMemory = toMiB(u.MemUsage.UsedVRAM)
		}
		gpus = append(gpus, spec)
	}
	return gpus
}

func decodeAMDSMI(out []byte) ([]amdSMIGPU, error) {
	var wrapped struct {
		GPUData []amdSMIGPU `json:"gpu_data"`
	}
	if err := json.Unmarshal(out, &wrapped); err == nil && wrapped.GPUData != nil {
		return wrapped.GPUData, nil
	}
	var bare []amdSMIGPU
	err := json.Unmarshal(out, &bare)
	return bare, err
}

// toMiB matches nvidia-smi's units (MiB). amd-smi reports VRAM in "MB",
// which is MiB in practice (MI300A: 131072 MB = 128 GiB).
func toMiB(v amdSMIValue) int64 {
	switch v.Unit {
	case "B":
		return int64(v.Value) / (1024 * 1024)
	case "KB":
		return int64(v.Value) / 1024
	case "GB":
		return int64(v.Value * 1024)
	default: // "MB"
		return int64(v.Value)
	}
}
