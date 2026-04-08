package platform

import (
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
