package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

// profiling runs one workload under a CPU and allocation profile, so the
// bottleneck is identified by measurement rather than by reasoning about which
// layer "should" dominate.
//
// The client shares this process, so a profile contains both sides. That is
// handled by reading the profile and attributing frames to the server's call
// path explicitly: the report states the split rather than quoting a flat
// "top functions" list that mixes them.
func profiling(w workload, conns int, dur time.Duration, outDir string) error {
	host, stop, err := serve(newPhoton(w, 0))
	if err != nil {
		return err
	}
	defer stop()

	if _, _, _, err := drive(host, w, conns, 2*time.Second); err != nil {
		return fmt.Errorf("warmup: %w", err)
	}

	cpuPath := outDir + "/cpu.prof"
	f, err := os.Create(cpuPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		return err
	}

	rps, lat, count, err := drive(host, w, conns, dur)

	pprof.StopCPUProfile()
	if err != nil {
		return err
	}
	fmt.Printf("workload %s: %.0f req/s, p50=%.0fus p99=%.0fus, %d requests\n",
		w.name, rps, lat.p50, lat.p99, count)
	fmt.Printf("cpu profile: %s\n\n", cpuPath)

	return writeHeapProfile(outDir + "/heap.prof")
}

func writeHeapProfile(path string) error {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	runtime.GC()
	runtime.ReadMemStats(&after)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
		return err
	}
	fmt.Printf("heap profile: %s (live %d KB, total allocated %d MB)\n\n",
		path, after.HeapAlloc/1024, after.TotalAlloc/(1<<20))
	return nil
}

// writeBlockProfile records where goroutines block, which is how a contended
// mutex or a saturated channel would show up rather than being inferred.
func writeBlockProfile(path string) error {
	runtime.SetBlockProfileRate(10000) // sample 1 in 10,000 blocking events
	defer runtime.SetBlockProfileRate(0)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pprof.Lookup("block").WriteTo(f, 0)
}
