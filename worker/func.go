package worker

import (
	"encoding/json"
	"runtime"
)

type systemInfo struct {
	GOOS, GOARCH, GOROOT, Version string
	NumCPU, NumGoroutine          int
	NumCgoCall                    int64
}

func SysInfo(job *Job) ([]byte, error) {
	return json.Marshal(&systemInfo{
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		GOROOT:       runtime.GOROOT(),
		Version:      runtime.Version(),
		NumCPU:       runtime.NumCPU(),
		NumGoroutine: runtime.NumGoroutine(),
		NumCgoCall:   runtime.NumCgoCall(),
	})
}

var memState runtime.MemStats

func MemInfo(job *Job) ([]byte, error) {
	runtime.ReadMemStats(&memState)
	return json.Marshal(&memState)
}
