//go:build !linux

package photon

// readCgroupLimit returns 0: cgroups are a Linux mechanism. A container on
// another OS is a Linux VM underneath, and the binary inside it is the Linux
// build.
func readCgroupLimit() int64 { return 0 }

// readMemTotal returns 0 on platforms where total RAM is not read. Detection
// then falls back to the 256 MB budget, which is conservative for a developer
// machine and is overridden by GOMEMLIMIT or photon.MemoryBudget in production.
func readMemTotal() int64 { return 0 }
