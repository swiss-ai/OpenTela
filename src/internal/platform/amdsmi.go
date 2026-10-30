package platform

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"

	"opentela/internal/common"
)

// amd-smi is rocm-smi's replacement; newer ROCm images ship only amd-smi.
// Two calls: `static --asic --board` for the product name and board serial,
// `metric --mem-usage` for total/used VRAM. Both print {"gpu_data": [...]}
// (older amd-smi versions print the bare list), keyed by "gpu" index.
//
// amd-smi lists each die of a multi-die package (MI250X: 2 GCDs per OAM) as
// its own GPU; the dies share the board's product serial, so entries with the
// same serial are merged into one GPU, as getAMDGPUs does for rocm-smi's MCM.

type amdSMIValue struct {
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
}

type amdSMIGPU struct {
	GPU  int `json:"gpu"`
	ASIC struct {
		MarketName string `json:"market_name"`
		DeviceID   string `json:"device_id"`
	} `json:"asic"`
	Board struct {
		ProductSerial string `json:"product_serial"`
	} `json:"board"`
	MemUsage struct {
		TotalVRAM amdSMIValue `json:"total_vram"`
		UsedVRAM  amdSMIValue `json:"used_vram"`
	} `json:"mem_usage"`
}

func getAMDSMIGPUs() []common.GPUSpec {
	static, err := runAMDSMI("static", "--asic", "--board", "--json")
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
	bySerial := map[string]int{} // board serial -> index in gpus
	for _, g := range names {
		name := knownValue(g.ASIC.MarketName)
		if name == "" {
			name = amdNameFromID(knownValue(g.ASIC.DeviceID))
		}
		if name == "" {
			continue
		}
		spec := common.GPUSpec{Name: name}
		if u, ok := mem[g.GPU]; ok {
			spec.TotalMemory = toMiB(u.MemUsage.TotalVRAM)
			spec.UsedMemory = toMiB(u.MemUsage.UsedVRAM)
		}
		serial := knownValue(g.Board.ProductSerial)
		if serial == "" {
			gpus = append(gpus, spec)
			continue
		}
		if i, ok := bySerial[serial]; ok {
			gpus[i].TotalMemory += spec.TotalMemory
			gpus[i].UsedMemory += spec.UsedMemory
			continue
		}
		bySerial[serial] = len(gpus)
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

// amdDeviceNames maps PCI device IDs to product names for images whose
// rocm-smi / amd-smi lack a PCI-ID database and report the name as "N/A".
var amdDeviceNames = map[string]string{
	"0x738c": "AMD Instinct MI100",
	"0x7408": "AMD Instinct MI250X",
	"0x740c": "AMD Instinct MI250X/MI250",
	"0x740f": "AMD Instinct MI210",
	"0x74a0": "AMD Instinct MI300A",
	"0x74a1": "AMD Instinct MI300X",
	"0x74a5": "AMD Instinct MI325X",
}

// amdNameFromID names a GPU from its device ID ("0x74a0"); an ID missing from
// the table still advertises the GPU, and a value that isn't an ID is kept.
func amdNameFromID(id string) string {
	if id == "" {
		return ""
	}
	if name, ok := amdDeviceNames[strings.ToLower(id)]; ok {
		return name
	}
	if strings.HasPrefix(strings.ToLower(id), "0x") {
		return "AMD GPU " + id
	}
	return id
}

// knownValue returns the first value that is set, skipping the tools'
// "N/A" placeholder.
func knownValue(vs ...string) string {
	for _, v := range vs {
		if v = strings.TrimSpace(v); v != "" && !strings.EqualFold(v, "N/A") {
			return v
		}
	}
	return ""
}
