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

// MI250X (Beverin mi200): amd-smi lists each of the 2 dies per OAM package as
// a GPU; the dies share board.product_serial (serials anonymised in the fixture).
func TestParseAMDSMIMergesMultiDiePackages(t *testing.T) {
	static, err := os.ReadFile("testdata/amdsmi_static_mi250x.json")
	if err != nil {
		t.Fatal(err)
	}
	usage, err := os.ReadFile("testdata/amdsmi_memusage_mi250x.json")
	if err != nil {
		t.Fatal(err)
	}

	gpus := parseAMDSMI(static, usage)
	if len(gpus) != 4 {
		t.Fatalf("want 4 GPUs (8 dies merged in pairs), got %d: %+v", len(gpus), gpus)
	}
	for _, g := range gpus {
		if g.Name != "AMD Instinct MI250X" || g.TotalMemory != 2*65520 || g.UsedMemory != 2*10 {
			t.Fatalf("unexpected GPU %+v", g)
		}
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

// Images without a PCI-ID database (Mel's vllm_apertus2_rocm on Beverin mi300)
// report every name and the board serial as "N/A", leaving only the device ID.
func TestNamesFromDeviceIDWhenToolsReportNA(t *testing.T) {
	static, err := os.ReadFile("testdata/amdsmi_static_mi300a_no_pciids.json")
	if err != nil {
		t.Fatal(err)
	}
	gpus := parseAMDSMI(static, nil)
	if len(gpus) != 4 {
		t.Fatalf("amd-smi: want 4 GPUs (N/A serials must not merge), got %+v", gpus)
	}
	for _, g := range gpus {
		if g.Name != "AMD Instinct MI300A" {
			t.Fatalf("amd-smi: unexpected GPU %+v", g)
		}
	}

	csv, err := os.ReadFile("testdata/rocmsmi_mi300a_no_pciids.csv")
	if err != nil {
		t.Fatal(err)
	}
	gpus = parseROCmSMI(csv)
	if len(gpus) != 4 {
		t.Fatalf("rocm-smi: want 4 GPUs, got %+v", gpus)
	}
	for _, g := range gpus {
		if g.Name != "AMD Instinct MI300A" || g.TotalMemory != 131072 || g.UsedMemory != 31 {
			t.Fatalf("rocm-smi: unexpected GPU %+v", g)
		}
	}
}

func TestAMDNameFromID(t *testing.T) {
	cases := map[string]string{
		"0x74a0":              "AMD Instinct MI300A",
		"0x74A0":              "AMD Instinct MI300A",
		"0x1234":              "AMD GPU 0x1234",
		"AMD Instinct MI250X": "AMD Instinct MI250X",
		"":                    "",
	}
	for in, want := range cases {
		if got := amdNameFromID(in); got != want {
			t.Errorf("amdNameFromID(%q) = %q, want %q", in, got, want)
		}
	}
}
