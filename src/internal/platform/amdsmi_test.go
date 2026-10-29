package platform

import (
	"os"
	"testing"
)

// Fixtures are amd-smi 27.1.0 output from the rocm/vllm image model-launch uses
// on Beverin (MI300A), which ships amd-smi but no rocm-smi.
func TestParseAMDSMI(t *testing.T) {
	static, err := os.ReadFile("testdata/amdsmi_static_mi300a.json")
	if err != nil {
		t.Fatal(err)
	}
	usage, err := os.ReadFile("testdata/amdsmi_memusage_mi300a.json")
	if err != nil {
		t.Fatal(err)
	}

	gpus := parseAMDSMI(static, usage)
	if len(gpus) != 2 {
		t.Fatalf("want 2 GPUs, got %+v", gpus)
	}
	for _, g := range gpus {
		if g.Name != "AMD Instinct MI300A" || g.TotalMemory != 131072 {
			t.Fatalf("unexpected GPU %+v", g)
		}
	}
	if gpus[0].UsedMemory != 31 || gpus[1].UsedMemory != 98304 {
		t.Fatalf("used memory not matched by gpu index: %+v", gpus)
	}

	// Names alone still advertise the GPUs when the memory call fails.
	if g := parseAMDSMI(static, nil); len(g) != 2 || g[0].TotalMemory != 0 {
		t.Fatalf("want 2 GPUs without memory, got %+v", g)
	}
	// Older amd-smi prints the bare list instead of {"gpu_data": [...]}.
	bare := []byte(`[{"gpu": 0, "asic": {"market_name": "AMD Instinct MI250X"}}]`)
	if g := parseAMDSMI(bare, nil); len(g) != 1 || g[0].Name != "AMD Instinct MI250X" {
		t.Fatalf("bare list not parsed: %+v", g)
	}
	// Garbage yields an empty (non-nil) slice, never a panic.
	if g := parseAMDSMI([]byte("amd-smi: error"), nil); g == nil || len(g) != 0 {
		t.Fatalf("want empty slice, got %+v", g)
	}
}

func TestToMiB(t *testing.T) {
	cases := map[amdSMIValue]int64{
		{Value: 131072, Unit: "MB"}:      131072,
		{Value: 128, Unit: "GB"}:         131072,
		{Value: 137438953472, Unit: "B"}: 131072,
		{Value: 134217728, Unit: "KB"}:   131072,
	}
	for in, want := range cases {
		if got := toMiB(in); got != want {
			t.Errorf("toMiB(%+v) = %d, want %d", in, got, want)
		}
	}
}
