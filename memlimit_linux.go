//go:build linux

package photon

import (
	"os"
	"path"
	"strconv"
	"strings"
)

// readCgroupLimit returns the tightest cgroup v2 memory limit that applies to
// this process, 0 if there is none.
//
// The process's own cgroup is found through /proc/self/cgroup and every
// ancestor up to the root is checked, because a limit can be set at any level:
// a container runtime usually sets it on the container's own cgroup (mounted
// at the root of the namespace), while systemd's MemoryMax= sets it on the
// service's cgroup, several levels below the root of the host hierarchy.
// Reading only /sys/fs/cgroup/memory.max finds the first and misses the second.
//
// cgroup v1 is deliberately not read. It is deprecated, and reading both
// invites a precedence bug where the v1 path silently wins on a host that has
// both. A host still on v1 falls through to MemTotal.
func readCgroupLimit() int64 {
	dir := "/"
	if b, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			// The unified hierarchy is the line "0::<path>".
			if p, ok := strings.CutPrefix(line, "0::"); ok {
				dir = path.Clean("/" + strings.TrimSpace(p))
				break
			}
		}
	}
	var limit int64
	for {
		if n := readCgroupMemoryMax(path.Join("/sys/fs/cgroup", dir, "memory.max")); n > 0 &&
			(limit == 0 || n < limit) {
			limit = n
		}
		if dir == "/" || dir == "." {
			return limit
		}
		dir = path.Dir(dir)
	}
}

// readCgroupMemoryMax parses one memory.max file, 0 if it is absent or "max".
func readCgroupMemoryMax(file string) int64 {
	b, err := os.ReadFile(file)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(b))
	if s == "max" || s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// readMemTotal returns the machine's total RAM, 0 if it cannot be read.
func readMemTotal() int64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}
