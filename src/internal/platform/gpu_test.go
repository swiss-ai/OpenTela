package platform

import (
	"opentela/internal/common"
	"os/exec"
	"testing"
)

func TestGetGPUInfo(t *testing.T) {
	// Test that GetGPUInfo returns valid structure regardless of environment
	t.Run("returns valid structure", func(t *testing.T) {
		result := GetGPUInfo()
		if result == nil {
			t.Error("Expected non-nil slice")
		}
		for _, gpu := range result {
			if gpu.Name == "" {
				t.Error("GPU name should not be empty")
			}
			if gpu.TotalMemory < 0 {
				t.Error("Total memory should not be negative")
			}
			if gpu.UsedMemory < 0 {
				t.Error("Used memory should not be negative")
			}
		}
	})
}

func TestGetNvidiaGPUs(t *testing.T) {
	cmd := exec.Command("which", "nvidia-smi")
	if err := cmd.Run(); err != nil {
		t.Skip("nvidia-smi not available, skipping")
	}

	result := getNvidiaGPUs()
	for _, gpu := range result {
		if gpu.Name == "" {
			t.Error("GPU name should not be empty")
		}
	}
}

func TestGetAMDGPUs(t *testing.T) {
	cmd := exec.Command("which", "rocm-smi")
	if err := cmd.Run(); err != nil {
		t.Skip("rocm-smi not available, skipping")
	}

	result := getAMDGPUs()
	for _, gpu := range result {
		if gpu.Name == "" {
			t.Error("GPU name should not be empty")
		}
	}
}

func TestCsvField(t *testing.T) {
	colIdx := map[string]int{"Name": 0, "Memory": 1, "Used": 2}

	tests := []struct {
		name     string
		fields   []string
		col      string
		expected string
	}{
		{"valid field", []string{"Tesla T4", "15360", "1024"}, "Name", "Tesla T4"},
		{"trims whitespace", []string{"  Tesla T4  ", " 15360 "}, "Name", "Tesla T4"},
		{"missing column", []string{"Tesla T4"}, "Missing", ""},
		{"index out of range", []string{"Tesla T4"}, "Memory", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := csvField(tt.fields, colIdx, tt.col)
			if got != tt.expected {
				t.Errorf("csvField() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNvidiaUnknownFiltering(t *testing.T) {
	// Verify that getNvidiaGPUs would skip "unknown" entries
	// We test the filtering logic indirectly through GetGPUInfo behavior:
	// if nvidia-smi returns "unknown", it should fallback to rocm-smi
	cmd := exec.Command("which", "nvidia-smi")
	hasNvidia := cmd.Run() == nil

	cmd = exec.Command("which", "rocm-smi")
	hasROCm := cmd.Run() == nil

	if !hasNvidia && !hasROCm {
		t.Skip("neither nvidia-smi nor rocm-smi available")
	}

	result := GetGPUInfo()
	for _, gpu := range result {
		if gpu.Name == "unknown" || gpu.Name == "[Unknown Error]" || gpu.Name == "N/A" {
			t.Errorf("GPU name should not be %q after filtering", gpu.Name)
		}
	}
}

// A failed probe (empty result) must not wipe GPUs already known; a successful
// probe still refreshes them (used memory changes while a model runs).
func TestGetGPUInfoKeepsLastKnownOnEmptyProbe(t *testing.T) {
	orig := probeGPUs
	defer func() { probeGPUs = orig; lastGPUs = nil }()
	lastGPUs = nil

	results := [][]common.GPUSpec{
		{},
		{{Name: "MI300A", TotalMemory: 100, UsedMemory: 1}},
		{},
		{{Name: "MI300A", TotalMemory: 100, UsedMemory: 7}},
		{},
	}
	call := 0
	probeGPUs = func() []common.GPUSpec { r := results[call]; call++; return r }

	want := []int64{-1, 1, 1, 7, 7} // -1: no GPUs known yet
	for i, w := range want {
		got := GetGPUInfo()
		if got == nil {
			t.Fatalf("call %d: nil slice", i)
		}
		if w == -1 {
			if len(got) != 0 {
				t.Fatalf("call %d: want no GPUs, got %+v", i, got)
			}
			continue
		}
		if len(got) != 1 || got[0].UsedMemory != w {
			t.Fatalf("call %d: want 1 GPU with used=%d, got %+v", i, w, got)
		}
	}
}
