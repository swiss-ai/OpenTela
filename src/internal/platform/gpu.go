package platform

import (
	"opentela/internal/common"
	"os/exec"
	"strconv"
	"strings"
)

func GetGPUInfo() []common.GPUSpec {
	gpus := getNvidiaGPUs()
	if len(gpus) > 0 {
		return gpus
	}
	return getAMDGPUs()
}

func getNvidiaGPUs() []common.GPUSpec {
	cmd := exec.Command("nvidia-smi", "--query-gpu=name,memory.total,memory.used", "--format=csv,noheader,nounits")
	out, err := cmd.Output()
	if err != nil {
		common.Logger.Debug("Error running nvidia-smi: ", err, " - (expected if no NVIDIA GPU is present)")
		return nil
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var gpus []common.GPUSpec
	for _, line := range lines {
		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			continue
		}
		name := strings.TrimSpace(fields[0])
		nameLower := strings.ToLower(name)
		if name == "" || strings.Contains(nameLower, "unknown") || strings.Contains(nameLower, "n/a") {
			continue
		}
		total, _ := strconv.ParseInt(strings.TrimSpace(fields[1]), 10, 64)
		used, _ := strconv.ParseInt(strings.TrimSpace(fields[2]), 10, 64)
		gpus = append(gpus, common.GPUSpec{
			Name:        name,
			TotalMemory: total,
			UsedMemory:  used,
		})
	}
	return gpus
}

func getAMDGPUs() []common.GPUSpec {
	cmd := exec.Command("rocm-smi", "--showproductname", "--showmeminfo", "vram", "--csv")
	out, err := cmd.Output()
	if err != nil {
		common.Logger.Debug("Error running rocm-smi: ", err, " - (expected if no AMD GPU is present)")
		return []common.GPUSpec{}
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 {
		return []common.GPUSpec{}
	}

	// Parse header to find column indices
	header := strings.Split(lines[0], ",")
	colIdx := map[string]int{}
	for i, h := range header {
		colIdx[strings.TrimSpace(h)] = i
	}

	var dies []common.GPUSpec
	for _, line := range lines[1:] {
		fields := strings.Split(line, ",")

		name := csvField(fields, colIdx, "Card series")
		if name == "" {
			name = csvField(fields, colIdx, "Card Series")
		}
		if name == "" {
			name = csvField(fields, colIdx, "Card model")
		}
		if name == "" {
			continue
		}

		var total, used int64
		if v := csvField(fields, colIdx, "VRAM Total Memory (B)"); v != "" {
			total, _ = strconv.ParseInt(v, 10, 64)
			total = total / (1024 * 1024) // bytes to MiB (matches nvidia-smi units)
		}
		if v := csvField(fields, colIdx, "VRAM Total Used Memory (B)"); v != "" {
			used, _ = strconv.ParseInt(v, 10, 64)
			used = used / (1024 * 1024) // bytes to MiB
		}

		dies = append(dies, common.GPUSpec{
			Name:        name,
			TotalMemory: total,
			UsedMemory:  used,
		})
	}

	// MCM GPUs (e.g. MI250X) report 2 dies per physical GPU — merge consecutive pairs
	if len(dies) > 0 && strings.Contains(dies[0].Name, "MCM") {
		var gpus []common.GPUSpec
		for i := 0; i < len(dies); i += 2 {
			gpu := dies[i]
			if i+1 < len(dies) && dies[i].Name == dies[i+1].Name {
				gpu.TotalMemory += dies[i+1].TotalMemory
				gpu.UsedMemory += dies[i+1].UsedMemory
			}
			gpus = append(gpus, gpu)
		}
		return gpus
	}

	return dies
}

func csvField(fields []string, colIdx map[string]int, col string) string {
	idx, ok := colIdx[col]
	if !ok || idx >= len(fields) {
		return ""
	}
	return strings.TrimSpace(fields[idx])
}
